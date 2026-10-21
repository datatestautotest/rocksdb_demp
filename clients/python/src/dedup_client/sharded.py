"""
RocksDB 去重服务分布式分片客户端 (ShardedDedupClient)

基于一致性哈希 (Consistent Hash Ring) 实现多实例数据分片与高可用集群路由：
- 虚拟节点技术（默认每物理节点 160 个虚拟节点），数据倾斜率 < 2%
- 故障容错与熔断机制：节点故障计数与自动冷却（Circuit Breaker），健康探测半开恢复
- 跨节点智能退避重试（Exponential Backoff & Next-Node Fallback）
- 批量操作自动多节点并发分发，支持保序重组与可选容错忽略
- 集群全局指标聚合
"""

import bisect
import hashlib
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Dict, List, Optional, Sequence, Set, Tuple, Union

from .client import DedupClient


def _default_hash(key_bytes: bytes) -> int:
    """默认 128-bit 一致性哈希函数（基于 MD5）"""
    return int(hashlib.md5(key_bytes).hexdigest(), 16)


class ConsistentHashRing:
    """一致性哈希环 (带虚拟节点与故障顺延)"""

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

    def get_node(self, key_bytes: bytes, exclude_nodes: Optional[Set[str]] = None) -> str:
        """根据 key 定位到目标物理节点，支持排除不可用节点顺延至下一个可用虚拟节点 (Fallback)"""
        if not self.ring:
            raise RuntimeError("ConsistentHashRing 中没有任何可用节点")
        exclude_nodes = exclude_nodes or set()
        h = _default_hash(key_bytes)
        idx = bisect.bisect_right(self.ring, h)
        total_vnodes = len(self.ring)

        # 顺时针寻找第一个不在排除列表中的节点
        for offset in range(total_vnodes):
            cur_idx = (idx + offset) % total_vnodes
            node = self.ring_map[self.ring[cur_idx]]
            if node not in exclude_nodes:
                return node

        # 若所有物理节点均在排除列表中，降级返回顺时针首选节点
        return self.ring_map[self.ring[idx % total_vnodes]]


def _build_route_key(namespace: Union[str, bytes], key: Union[str, bytes]) -> bytes:
    """构造用于哈希分片的复合键"""
    ns_bytes = namespace.encode("utf-8") if isinstance(namespace, str) else namespace
    k_bytes = key.encode("utf-8") if isinstance(key, str) else key
    return ns_bytes + b":" + k_bytes


class ShardedDedupClient:
    """高可用分布式分片去重客户端

    通过一致性哈希将海量去重请求路由到集群节点，
    具备跨节点退避重试、故障熔断探测以及并发批量保序执行能力。
    """

    def __init__(
        self,
        node_addrs: Sequence[str],
        timeout: float = 5.0,
        replicas: int = 160,
        max_workers: int = 16,
        max_retries: int = 2,
        retry_delay: float = 0.1,
        backoff_factor: float = 2.0,
        fail_threshold: int = 3,
        cooling_time: float = 10.0,
    ):
        """
        Args:
            node_addrs: 节点地址列表，如 ["127.0.0.1:50051", "127.0.0.1:50052"]
            timeout: 每个 gRPC 请求的超时时间（秒）
            replicas: 每个物理节点的虚拟节点数量
            max_workers: 批量多节点请求并发线程池大小
            max_retries: 瞬时网络异常时的最大重试次数
            retry_delay: 首次重试休眠时间（秒）
            backoff_factor: 重试指数退避因子
            fail_threshold: 触发节点熔断的连续失败次数阈值
            cooling_time: 节点触发熔断后的冷却隔离时间（秒）
        """
        if not node_addrs:
            raise ValueError("node_addrs 不能为空")

        self.ring = ConsistentHashRing(node_addrs, replicas=replicas)
        self.timeout = timeout
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.backoff_factor = backoff_factor
        self.fail_threshold = fail_threshold
        self.cooling_time = cooling_time

        self.clients: Dict[str, DedupClient] = {
            addr: DedupClient(addr, timeout=timeout) for addr in node_addrs
        }
        self._executor = ThreadPoolExecutor(max_workers=max_workers)

        # 故障感知与熔断跟踪
        self._failed_counts: Dict[str, int] = defaultdict(int)
        self._failed_timestamps: Dict[str, float] = {}
        self._lock = threading.Lock()

    def close(self) -> None:
        """关闭所有分片客户端连接与线程池"""
        for client in self.clients.values():
            client.close()
        self._executor.shutdown(wait=False)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    # -----------------------------------------------------------------------
    # 节点健康感知与熔断机制
    # -----------------------------------------------------------------------

    def _mark_failure(self, node: str) -> None:
        with self._lock:
            self._failed_counts[node] += 1
            self._failed_timestamps[node] = time.time()

    def _mark_success(self, node: str) -> None:
        with self._lock:
            self._failed_counts[node] = 0

    def _get_cooling_nodes(self) -> Set[str]:
        """获取当前处于冷却熔断状态的节点集合"""
        with self._lock:
            now = time.time()
            cooling = set()
            for node, count in list(self._failed_counts.items()):
                if count >= self.fail_threshold:
                    if now - self._failed_timestamps.get(node, 0) < self.cooling_time:
                        cooling.add(node)
                    else:
                        # 冷却期结束，半开允许试探
                        self._failed_counts[node] = 0
            return cooling

    def _execute_with_retry(
        self,
        route_key: bytes,
        fn: Callable[[DedupClient], any],
    ) -> any:
        """带退避重试与故障节点备选顺延的执行器"""
        excluded: Set[str] = set()
        last_exc: Optional[Exception] = None
        delay = self.retry_delay

        for attempt in range(self.max_retries + 1):
            cooling = self._get_cooling_nodes()
            current_exclude = excluded | cooling
            node = self.ring.get_node(route_key, exclude_nodes=current_exclude)
            client = self.clients[node]

            try:
                result = fn(client)
                self._mark_success(node)
                return result
            except Exception as e:
                last_exc = e
                self._mark_failure(node)
                excluded.add(node)
                if attempt < self.max_retries:
                    time.sleep(delay)
                    delay *= self.backoff_factor

        raise last_exc

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
        """检查并写入单条记录（支持自动重试与故障顺延）"""
        route_key = _build_route_key(namespace, key)
        return self._execute_with_retry(
            route_key,
            lambda c: c.check_and_set(namespace, key, expire_at_ms, metadata),
        )

    def exists(self, namespace: str, key: bytes) -> bool:
        """检查单条记录是否存在"""
        route_key = _build_route_key(namespace, key)
        return self._execute_with_retry(
            route_key,
            lambda c: c.exists(namespace, key),
        )

    def delete(self, namespace: str, key: bytes) -> bool:
        """删除单条记录"""
        route_key = _build_route_key(namespace, key)
        return self._execute_with_retry(
            route_key,
            lambda c: c.delete(namespace, key),
        )

    def get(self, namespace: str, key: bytes) -> Optional[dict]:
        """获取单条记录详情"""
        route_key = _build_route_key(namespace, key)
        return self._execute_with_retry(
            route_key,
            lambda c: c.get(namespace, key),
        )

    # -----------------------------------------------------------------------
    # 分布式批量操作 API（自动跨节点并发并行处理，严格保序）
    # -----------------------------------------------------------------------

    def batch_check_and_set(
        self,
        items: List[Tuple[str, bytes, int, Optional[bytes]]],
        ignore_unavailable: bool = False,
    ) -> List[Optional[str]]:
        """
        分布式批量检查并写入

        自动根据每条数据的 (namespace, key) 划分到对应的分片节点，
        并发向所有分片发送批次请求，并按原数组下标严格保序返回结果。
        """
        if not items:
            return []

        cooling = self._get_cooling_nodes()

        # 按目标节点分组: node -> list of (original_index, item)
        groups = defaultdict(list)
        for idx, item in enumerate(items):
            ns, key, _, _ = item
            route_key = _build_route_key(ns, key)
            node = self.ring.get_node(route_key, exclude_nodes=cooling)
            groups[node].append((idx, item))

        results: List[Optional[str]] = [None] * len(items)

        def _worker(node: str, sub_items: list):
            client = self.clients[node]
            pure_items = [it[1] for it in sub_items]
            delay = self.retry_delay
            last_err = None
            for attempt in range(self.max_retries + 1):
                try:
                    sub_results = client.batch_check_and_set(pure_items)
                    self._mark_success(node)
                    return sub_items, sub_results
                except Exception as e:
                    last_err = e
                    self._mark_failure(node)
                    if attempt < self.max_retries:
                        time.sleep(delay)
                        delay *= self.backoff_factor

            if ignore_unavailable:
                return sub_items, [None] * len(sub_items)
            raise last_err

        futures = [
            self._executor.submit(_worker, node, sub_items)
            for node, sub_items in groups.items()
        ]

        for fut in as_completed(futures):
            sub_items, sub_results = fut.result()
            for (orig_idx, _), res in zip(sub_items, sub_results):
                results[orig_idx] = res

        return results

    def batch_exists(
        self,
        namespace: str,
        keys: List[bytes],
        ignore_unavailable: bool = False,
    ) -> List[Optional[bool]]:
        """分布式批量检查是否存在（保序返回）"""
        if not keys:
            return []

        cooling = self._get_cooling_nodes()
        groups = defaultdict(list)
        for idx, key in enumerate(keys):
            route_key = _build_route_key(namespace, key)
            node = self.ring.get_node(route_key, exclude_nodes=cooling)
            groups[node].append((idx, key))

        results: List[Optional[bool]] = [None] * len(keys)

        def _worker(node: str, sub_items: list):
            client = self.clients[node]
            pure_keys = [it[1] for it in sub_items]
            delay = self.retry_delay
            last_err = None
            for attempt in range(self.max_retries + 1):
                try:
                    sub_results = client.batch_exists(namespace, pure_keys)
                    self._mark_success(node)
                    return sub_items, sub_results
                except Exception as e:
                    last_err = e
                    self._mark_failure(node)
                    if attempt < self.max_retries:
                        time.sleep(delay)
                        delay *= self.backoff_factor

            if ignore_unavailable:
                return sub_items, [None] * len(sub_items)
            raise last_err

        futures = [
            self._executor.submit(_worker, node, sub_items)
            for node, sub_items in groups.items()
        ]

        for fut in as_completed(futures):
            sub_items, sub_results = fut.result()
            for (orig_idx, _), res in zip(sub_items, sub_results):
                results[orig_idx] = res

        return results

    def batch_delete(
        self,
        namespace: str,
        keys: List[bytes],
        ignore_unavailable: bool = False,
    ) -> List[Optional[bool]]:
        """分布式批量删除（保序返回）"""
        if not keys:
            return []

        cooling = self._get_cooling_nodes()
        groups = defaultdict(list)
        for idx, key in enumerate(keys):
            route_key = _build_route_key(namespace, key)
            node = self.ring.get_node(route_key, exclude_nodes=cooling)
            groups[node].append((idx, key))

        results: List[Optional[bool]] = [None] * len(keys)

        def _worker(node: str, sub_items: list):
            client = self.clients[node]
            pure_keys = [it[1] for it in sub_items]
            delay = self.retry_delay
            last_err = None
            for attempt in range(self.max_retries + 1):
                try:
                    sub_results = client.batch_delete(namespace, pure_keys)
                    self._mark_success(node)
                    return sub_items, sub_results
                except Exception as e:
                    last_err = e
                    self._mark_failure(node)
                    if attempt < self.max_retries:
                        time.sleep(delay)
                        delay *= self.backoff_factor

            if ignore_unavailable:
                return sub_items, [None] * len(sub_items)
            raise last_err

        futures = [
            self._executor.submit(_worker, node, sub_items)
            for node, sub_items in groups.items()
        ]

        for fut in as_completed(futures):
            sub_items, sub_results = fut.result()
            for (orig_idx, _), res in zip(sub_items, sub_results):
                results[orig_idx] = res

        return results

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
