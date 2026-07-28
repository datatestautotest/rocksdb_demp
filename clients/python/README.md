# Python 客户端使用说明

## 安装依赖

```bash
pip install -r requirements.txt
```

## 生成 gRPC 代码

```bash
cd clients/python
python -m grpc_tools.protoc -I../../proto --python_out=. --grpc_python_out=. ../../proto/dedup.proto
```

生成后会得到两个文件：
- `dedup_pb2.py` - 消息定义
- `dedup_pb2_grpc.py` - gRPC 服务/客户端存根

## 快速开始

```python
from dedup_client import DedupClient
import time

# 连接服务
with DedupClient("localhost:50051") as client:
    ns = "spider_news"
    expire_at = int(time.time() * 1000) + 30 * 86400 * 1000  # 30天后过期

    # 核心接口：检查并写入
    result = client.check_and_set(
        namespace=ns,
        key=b"https://example.com/page/1",
        expire_at_ms=expire_at,
        metadata=b'{"depth": 3, "source": "seed"}',
    )

    if result == "inserted":
        print("新 URL，开始抓取")
    elif result == "duplicate":
        print("重复 URL，跳过")
    elif result == "expired_and_replaced":
        print("已过期，重新抓取")
```

## 运行示例

```bash
# 先启动服务端
cargo run -- serve --addr 0.0.0.0:50051 --db-path ./data/dedup

# 再运行 Python 示例
python dedup_client.py
```

## API 列表

| 方法 | 说明 |
|---|---|
| `check_and_set(namespace, key, expire_at_ms, metadata)` | 原子检查并写入 |
| `batch_check_and_set(items)` | 批量检查并写入 |
| `exists(namespace, key)` | 仅检查是否存在 |
| `batch_exists(namespace, keys)` | 批量检查存在性 |
| `get(namespace, key)` | 获取详细信息 |
| `delete(namespace, key)` | 删除 key |
| `batch_delete(namespace, keys)` | 批量删除 |
| `stats(namespace)` | 获取服务统计 |
