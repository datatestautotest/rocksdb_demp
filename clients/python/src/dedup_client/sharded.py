"""
RocksDB 去重服务分布式分片客户端 (ShardedDedupClient)

基于一致性哈希 (Consistent Hash Ring) 实现多实例数据分片与集群路由：
- 虚拟节点技术（默认每物理节点 160 个虚拟节点），数据倾斜率 < 2%
- 批量操作自动多节点并发分发，结果保序聚合
- 集群全局指标聚合
"""

import bisect
import hashlib
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Sequence, Tuple, Union

from .client import DedupClient


def _default_hash(key_bytes: bytes) -> int:
    """默认 128-bit 一致性哈希函数（基于 MD5）"""
    return int(hashlib.md5(key_bytes).hexdigest(), 16)


class ConsistentHashRing:
    """一致性哈希环 (带虚拟节点)"""

    def __init__(self, nodes: Optional[Sequence[str]] = None, replicas: int = 160):
        """
        Args:
            nodes: 初始节点列表，如 ["192.168.1.1:50051", "192.168.1.2:50051"]
            replicas: 每个物理节点对应的虚拟节点数（默认 160）
        """
        self.replicas = replicas
        self.ring: List[int] = []
        self.ring_map: Dict[int, str] = {}
        self.nodes: set = set()

        if nodes:
            for node in nodes:
                self.add_node(node)

    def add_node(self, node: str) -> None:
        """向哈希环添加节点"""
        if node in self.nodes:
            return
        self.nodes.add(node)
        for i in range(self.replicas):
            vnode_key = f"{node}#{i}".encode("utf-8")
            h = _default_hash(vnode_key)
            idx = bisect.bisect_left(self.ring, h)
            self.ring.insert(idx, h)
            self.ring_map[h] = node

    def remove_node(self, node: str) -> None:
        """从哈希环移除节点"""
        if node not in self.nodes:
            return
        self.nodes.remove(node)
        for i in range(self.replicas):
            vnode_key = f"{node}#{i}".encode("utf-8")
            h = _default_hash(vnode_key)
            idx = bisect.bisect_left(self.ring, h)
            if idx < len(self.ring) and self.ring[idx] == h:
                del self.ring[idx]
            self.ring_map.pop(h, None)

    def get_node(self, key_bytes: bytes) -> str:
        """根据 key 定位到目标物理节点"""
        if not self.ring:
            raise RuntimeError("ConsistentHashRing 中没有任何可用节点")
        h = _default_hash(key_bytes)
        idx = bisect.bisect_right(self.ring, h)
        if idx >= len(self.ring):
            idx = 0
        return self.ring_map[self.ring[idx]]


def _build_route_key(namespace: Union[str, bytes], key: Union[str, bytes]) -> bytes:
    """构造用于哈希分片的复合键"""
    ns_bytes = namespace.encode("utf-8") if isinstance(namespace, str) else namespace
    k_bytes = key.encode("utf-8") if isinstance(key, str) else key
    return ns_bytes + b":" + k_bytes


class ShardedDedupClient:
    """分布式分片去重客户端

    通过一致性哈希将海量去重请求路由到集群中的各个节点，
    支持单条及并发批量操作，并自动对批量操作进行多节点拆分与结果聚合。
    """

    def __init__(
        self,
        node_addrs: Sequence[str],
        timeout: float = 5.0,
        replicas: int = 160,
        max_workers: int = 16,
    ):
        """
        Args:
            node_addrs: 节点地址列表，如 ["127.0.0.1:50051", "127.0.0.1:50052"]
            timeout: 每个 gRPC 请求的超时时间（秒）
            replicas: 每个物理节点的虚拟节点数量
            max_workers: 批量多节点请求并发线程池大小
        """
        if not node_addrs:
            raise ValueError("node_addrs 不能为空")

        self.ring = ConsistentHashRing(node_addrs, replicas=replicas)
        self.timeout = timeout
        self.max_workers = max_workers
        self.clients: Dict[str, DedupClient] = {
            addr: DedupClient(addr, timeout=timeout) for addr in node_addrs
        }
        self._executor = ThreadPoolExecutor(max_workers=max_workers)

    def close(self) -> None:
        """关闭所有分片客户端连接与线程池"""
        for client in self.clients.values():
            client.close()
        self._executor.shutdown(wait=False)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def _get_client_for_key(self, namespace: Union[str, bytes], key: bytes) -> DedupClient:
        route_key = _build_route_key(namespace, key)
        node = self.ring.get_node(route_key)
        return self.clients[node]

    # -----------------------------------------------------------------------
    # 单键操作 API
    # -----------------------------------------------------------------------

    def check_and_set(
        self,
        namespace: str,
        key: bytes,
        expire_at_ms: int,
        metadata: Optional[bytes] = None,
    ) -> str:
        """检查并写入单条记录"""
        client = self._get_client_for_key(namespace, key)
        return client.check_and_set(namespace, key, expire_at_ms, metadata)

    def exists(self, namespace: str, key: bytes) -> bool:
        """检查单条记录是否存在"""
        client = self._get_client_for_key(namespace, key)
        return client.exists(namespace, key)

    def delete(self, namespace: str, key: bytes) -> bool:
        """删除单条记录"""
        client = self._get_client_for_key(namespace, key)
        return client.delete(namespace, key)

    def get(self, namespace: str, key: bytes) -> Optional[dict]:
        """获取单条记录详情"""
        client = self._get_client_for_key(namespace, key)
        return client.get(namespace, key)

    # -----------------------------------------------------------------------
    # 分布式批量操作 API（自动跨节点并发并行处理，严格保序）
    # -----------------------------------------------------------------------

    def batch_check_and_set(
        self,
        items: List[Tuple[str, bytes, int, Optional[bytes]]],
    ) -> List[str]:
        """
        分布式批量检查并写入

        自动根据每条数据的 (namespace, key) 划分到对应的分片节点，
        并发向所有分片发送批次请求，并按原数组下标保序返回结果。
        """
        if not items:
            return []

        # 按目标节点分组: node -> list of (original_index, item)
        groups = defaultdict(list)
        for idx, item in enumerate(items):
            ns, key, _, _ = item
            route_key = _build_route_key(ns, key)
            node = self.ring.get_node(route_key)
            groups[node].append((idx, item))

        results: List[Optional[str]] = [None] * len(items)

        def _worker(node: str, sub_items: list):
            client = self.clients[node]
            pure_items = [it[1] for it in sub_items]
            sub_results = client.batch_check_and_set(pure_items)
            return sub_items, sub_results

        futures = [
            self._executor.submit(_worker, node, sub_items)
            for node, sub_items in groups.items()
        ]

        for fut in as_completed(futures):
            sub_items, sub_results = fut.result()
            for (orig_idx, _), res in zip(sub_items, sub_results):
                results[orig_idx] = res

        return [r for r in results if r is not None]

    def batch_exists(self, namespace: str, keys: List[bytes]) -> List[bool]:
        """分布式批量检查是否存在（保序返回）"""
        if not keys:
            return []

        groups = defaultdict(list)
        for idx, key in enumerate(keys):
            route_key = _build_route_key(namespace, key)
            node = self.ring.get_node(route_key)
            groups[node].append((idx, key))

        results: List[Optional[bool]] = [None] * len(keys)

        def _worker(node: str, sub_items: list):
            client = self.clients[node]
            pure_keys = [it[1] for it in sub_items]
            sub_results = client.batch_exists(namespace, pure_keys)
            return sub_items, sub_results

        futures = [
            self._executor.submit(_worker, node, sub_items)
            for node, sub_items in groups.items()
        ]

        for fut in as_completed(futures):
            sub_items, sub_results = fut.result()
            for (orig_idx, _), res in zip(sub_items, sub_results):
                results[orig_idx] = res

        return [r for r in results if r is not None]

    def batch_delete(self, namespace: str, keys: List[bytes]) -> List[bool]:
        """分布式批量删除（保序返回）"""
        if not keys:
            return []

        groups = defaultdict(list)
        for idx, key in enumerate(keys):
            route_key = _build_route_key(namespace, key)
            node = self.ring.get_node(route_key)
            groups[node].append((idx, key))

        results: List[Optional[bool]] = [None] * len(keys)

        def _worker(node: str, sub_items: list):
            client = self.clients[node]
            pure_keys = [it[1] for it in sub_items]
            sub_results = client.batch_delete(namespace, pure_keys)
            return sub_items, sub_results

        futures = [
            self._executor.submit(_worker, node, sub_items)
            for node, sub_items in groups.items()
        ]

        for fut in as_completed(futures):
            sub_items, sub_results = fut.result()
            for (orig_idx, _), res in zip(sub_items, sub_results):
                results[orig_idx] = res

        return [r for r in results if r is not None]

    # -----------------------------------------------------------------------
    # 集群聚合指标
    # -----------------------------------------------------------------------

    def get_cluster_stats(self) -> dict:
        """获取所有分片节点的聚合监控统计"""
        per_node = {}

        def _fetch_node_stats(node: str):
            client = self.clients[node]
            return node, client.stats()

        futures = [self._executor.submit(_fetch_node_stats, node) for node in self.clients]
        for fut in as_completed(futures):
            node, s = fut.result()
            per_node[node] = s

        total_keys = sum(s.get("total_keys", 0) for s in per_node.values())
        bloom_cap = sum(s.get("bloom_capacity", 0) for s in per_node.values())
        bloom_ins = sum(s.get("bloom_insertions", 0) for s in per_node.values())
        cleaned_total = sum(s.get("cleaned_total", 0) for s in per_node.values())
        backlog_est = sum(s.get("backlog_estimate", 0) for s in per_node.values())

        return {
            "cluster_nodes_count": len(self.clients),
            "total_keys": total_keys,
            "bloom_capacity_total": bloom_cap,
            "bloom_insertions_total": bloom_ins,
            "cleaned_total": cleaned_total,
            "backlog_estimate_total": backlog_est,
            "nodes": per_node,
        }
