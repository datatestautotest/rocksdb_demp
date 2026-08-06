"""
RocksDB 去重服务 Python 客户端库

基于 RocksDB + 布隆过滤器的高并发持久化去重引擎的 Python 客户端。

快速开始:
    from dedup_client import DedupClient

    with DedupClient("localhost:50051") as client:
        result = client.check_and_set(
            namespace="spider_news",
            key=b"https://example.com/page/1",
            expire_at_ms=1735689600000,
        )
        print(result)  # "inserted" / "duplicate" / "expired_and_replaced"
"""

from .client import DedupClient

__all__ = ["DedupClient"]
__version__ = "0.1.0"
