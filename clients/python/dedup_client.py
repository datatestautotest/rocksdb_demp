"""
RocksDB 去重服务 Python 客户端

依赖安装:
    pip install grpcio grpcio-tools

生成 gRPC 代码:
    python -m grpc_tools.protoc -I../../proto --python_out=. --grpc_python_out=. ../../proto/dedup.proto

使用示例:
    from dedup_client import DedupClient

    client = DedupClient("localhost:50051")

    # 检查并写入
    result = client.check_and_set(
        namespace="spider_news",
        key=b"https://example.com/page/1",
        expire_at_ms=1735689600000,
        metadata=b"some metadata",
    )
    print(result)  # "inserted" / "duplicate" / "expired_and_replaced"

    # 仅检查是否存在
    exists = client.exists("spider_news", b"https://example.com/page/1")
"""

import time
from typing import Optional, List, Tuple

import grpc

# 生成的 gRPC 代码
try:
    import dedup_pb2
    import dedup_pb2_grpc
except ImportError:
    raise ImportError(
        "请先生成 gRPC 代码: "
        "python -m grpc_tools.protoc -I../../proto --python_out=. --grpc_python_out=. ../../proto/dedup.proto"
    )


class DedupClient:
    """去重服务 gRPC 客户端"""

    def __init__(self, addr: str, timeout: float = 5.0):
        """
        Args:
            addr: 服务地址，如 "localhost:50051"
            timeout: 默认超时时间（秒）
        """
        self.addr = addr
        self.timeout = timeout
        self.channel = grpc.insecure_channel(addr)
        self.stub = dedup_pb2_grpc.DedupServiceStub(self.channel)

    def close(self):
        """关闭连接"""
        self.channel.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    # -----------------------------------------------------------------------
    # 核心 API
    # -----------------------------------------------------------------------

    def check_and_set(
        self,
        namespace: str,
        key: bytes,
        expire_at_ms: int,
        metadata: Optional[bytes] = None,
    ) -> str:
        """
        检查 key 是否存在，不存在则写入（原子操作）

        Returns:
            "inserted" - 新写入
            "duplicate" - 已存在（未过期）
            "expired_and_replaced" - 已过期，已替换
        """
        req = dedup_pb2.CheckAndSetRequest(
            namespace=namespace,
            key=key,
            expire_at_ms=expire_at_ms,
            metadata=metadata or b"",
        )
        resp = self.stub.CheckAndSet(req, timeout=self.timeout)
        return self._result_to_str(resp.result)

    def batch_check_and_set(
        self,
        items: List[Tuple[str, bytes, int, Optional[bytes]]],
    ) -> List[str]:
        """
        批量检查并写入

        Args:
            items: [(namespace, key, expire_at_ms, metadata), ...]

        Returns:
            结果列表，顺序与输入一致
        """
        req_items = []
        for ns, key, expire_at, meta in items:
            req_items.append(dedup_pb2.CheckAndSetRequest(
                namespace=ns,
                key=key,
                expire_at_ms=expire_at,
                metadata=meta or b"",
            ))
        req = dedup_pb2.BatchCheckAndSetRequest(items=req_items)
        resp = self.stub.BatchCheckAndSet(req, timeout=self.timeout)
        return [self._result_to_str(r.result) for r in resp.results]

    def exists(self, namespace: str, key: bytes) -> bool:
        """仅检查 key 是否存在且未过期"""
        req = dedup_pb2.ExistsRequest(namespace=namespace, key=key)
        resp = self.stub.Exists(req, timeout=self.timeout)
        return resp.exists

    def batch_exists(self, namespace: str, keys: List[bytes]) -> List[bool]:
        """批量检查是否存在"""
        req = dedup_pb2.BatchExistsRequest(namespace=namespace, keys=keys)
        resp = self.stub.BatchExists(req, timeout=self.timeout)
        return list(resp.exists)

    def get(self, namespace: str, key: bytes) -> Optional[dict]:
        """
        获取 key 的详细信息

        Returns:
            {
                "expire_at_ms": int,
                "created_at_ms": int,
                "metadata": bytes,
            }
            不存在返回 None
        """
        req = dedup_pb2.GetRequest(namespace=namespace, key=key)
        resp = self.stub.Get(req, timeout=self.timeout)
        if not resp.found:
            return None
        return {
            "expire_at_ms": resp.value.expire_at_ms,
            "created_at_ms": resp.value.created_at_ms,
            "metadata": resp.value.metadata,
        }

    def delete(self, namespace: str, key: bytes) -> bool:
        """删除 key，返回是否成功删除"""
        req = dedup_pb2.DeleteRequest(namespace=namespace, key=key)
        resp = self.stub.Delete(req, timeout=self.timeout)
        return resp.deleted

    def batch_delete(self, namespace: str, keys: List[bytes]) -> List[bool]:
        """批量删除"""
        req = dedup_pb2.BatchDeleteRequest(namespace=namespace, keys=keys)
        resp = self.stub.BatchDelete(req, timeout=self.timeout)
        return list(resp.deleted)

    def stats(self, namespace: str = "") -> dict:
        """
        获取服务统计信息

        Returns:
            {
                "total_keys": int,
                "bloom_capacity": int,
                "bloom_insertions": int,
                "bloom_fpp": float,
                "cleaned_total": int,
                "backlog_estimate": int,
                "current_speed": int,
                "qps_estimate": int,
                "error_count": int,
            }
        """
        req = dedup_pb2.StatsRequest(namespace=namespace)
        resp = self.stub.Stats(req, timeout=self.timeout)
        return {
            "total_keys": resp.total_keys,
            "bloom_capacity": resp.bloom_capacity,
            "bloom_insertions": resp.bloom_insertions,
            "bloom_fpp": resp.bloom_fpp,
            "cleaned_total": resp.cleaned_total,
            "backlog_estimate": resp.backlog_estimate,
            "current_speed": resp.current_speed,
            "qps_estimate": resp.qps_estimate,
            "error_count": resp.error_count,
        }

    # -----------------------------------------------------------------------
    # 工具方法
    # -----------------------------------------------------------------------

    @staticmethod
    def _result_to_str(result: int) -> str:
        """将 proto enum 转换为字符串"""
        if result == dedup_pb2.CheckResult.INSERTED:
            return "inserted"
        elif result == dedup_pb2.CheckResult.DUPLICATE:
            return "duplicate"
        elif result == dedup_pb2.CheckResult.EXPIRED_AND_REPLACED:
            return "expired_and_replaced"
        else:
            return f"unknown({result})"


# ---------------------------------------------------------------------------
#  使用示例
# ---------------------------------------------------------------------------

def main():
    """简单的使用示例"""
    print("连接去重服务...")
    with DedupClient("localhost:50051") as client:
        ns = "spider_news"
        expire_at = int(time.time() * 1000) + 30 * 86400 * 1000  # 30天后

        # 测试 check_and_set
        print("\n--- 测试 check_and_set ---")
        result = client.check_and_set(ns, b"https://example.com/1", expire_at)
        print(f"第一次写入: {result}")  # inserted

        result = client.check_and_set(ns, b"https://example.com/1", expire_at)
        print(f"第二次写入: {result}")  # duplicate

        # 测试 exists
        print("\n--- 测试 exists ---")
        print(f"存在: {client.exists(ns, b'https://example.com/1')}")  # True
        print(f"不存在: {client.exists(ns, b'https://example.com/999')}")  # False

        # 测试 get
        print("\n--- 测试 get ---")
        value = client.get(ns, b"https://example.com/1")
        print(f"详情: {value}")

        # 测试批量
        print("\n--- 测试 batch_check_and_set ---")
        items = [
            (ns, f"https://example.com/batch_{i}".encode(), expire_at, None)
            for i in range(10)
        ]
        results = client.batch_check_and_set(items)
        print(f"批量结果: {results}")

        # 测试 stats
        print("\n--- 服务状态 ---")
        stats = client.stats()
        for k, v in stats.items():
            print(f"  {k}: {v}")

        # 测试 delete
        print("\n--- 测试 delete ---")
        deleted = client.delete(ns, b"https://example.com/1")
        print(f"删除: {deleted}")  # True
        print(f"删除后存在: {client.exists(ns, b'https://example.com/1')}")  # False


if __name__ == "__main__":
    main()
