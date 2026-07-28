"""
测试重启恢复：验证重启后数据仍然存在，布隆过滤器正确重建
"""
import grpc
import dedup_pb2
import dedup_pb2_grpc

channel = grpc.insecure_channel("127.0.0.1:50051")
stub = dedup_pb2_grpc.DedupServiceStub(channel)

print("=== 重启恢复测试 ===")

# 1. 检查服务状态
stats = stub.Stats(dedup_pb2.StatsRequest())
print(f"\n服务状态:")
print(f"  total_keys: {stats.total_keys:,}")
print(f"  bloom_capacity: {stats.bloom_capacity:,}")
print(f"  bloom_insertions: {stats.bloom_insertions:,}")
print(f"  cleaned_total: {stats.cleaned_total:,}")

# 2. 验证之前写入的数据仍然存在（来自 basic 测试）
print("\n--- 验证 test_basic 数据 ---")
ns_basic = "test_basic"
basic_ok = 0
basic_missing = 0
for i in range(100):
    if i == 42:  # 之前删掉的
        continue
    key = f"https://example.com/page/{i}".encode()
    resp = stub.Exists(dedup_pb2.ExistsRequest(namespace=ns_basic, key=key))
    if resp.exists:
        basic_ok += 1
    else:
        basic_missing += 1

print(f"  存在: {basic_ok}/99, 丢失: {basic_missing}")

# 验证被删除的 key 42 不存在
resp = stub.Exists(dedup_pb2.ExistsRequest(
    namespace=ns_basic,
    key=b"https://example.com/page/42",
))
print(f"  已删除的 key 42 存在: {resp.exists} (期望 False)")

# 3. 验证扩容测试的数据
print("\n--- 验证 test_expansion 数据 ---")
ns_expand = "test_expansion"
expand_ok = 0
for i in range(0, 15000, 1000):  # 抽样检查
    key = f"https://expand-test.com/{i}".encode()
    resp = stub.Exists(dedup_pb2.ExistsRequest(namespace=ns_expand, key=key))
    if resp.exists:
        expand_ok += 1

print(f"  抽样 15 个，存在: {expand_ok}/15")

# 4. 验证过期清理后的数据
print("\n--- 验证 test_expiry 数据 ---")
ns_expiry = "test_expiry"

# 短期数据应该已过期
short_missing = 0
for i in range(0, 500, 50):
    key = f"https://expiry-test.com/short/{i}".encode()
    resp = stub.Exists(dedup_pb2.ExistsRequest(namespace=ns_expiry, key=key))
    if not resp.exists:
        short_missing += 1
print(f"  短期数据(抽样10个)已过期: {short_missing}/10 (期望 10)")

# 长期数据应该还在
long_ok = 0
for i in range(0, 100, 10):
    key = f"https://expiry-test.com/long/{i}".encode()
    resp = stub.Exists(dedup_pb2.ExistsRequest(namespace=ns_expiry, key=key))
    if resp.exists:
        long_ok += 1
print(f"  长期数据(抽样10个)存在: {long_ok}/10 (期望 10)")

# 5. 验证布隆过滤器重建后仍然能正确去重
print("\n--- 验证布隆过滤器重建后去重正确 ---")
ns = "test_basic"
dup_count = 0
for i in range(10):
    key = f"https://example.com/page/{i}".encode()
    resp = stub.CheckAndSet(dedup_pb2.CheckAndSetRequest(
        namespace=ns,
        key=key,
        expire_at_ms=9999999999999,
    ))
    if resp.result == dedup_pb2.CheckResult.DUPLICATE:
        dup_count += 1

print(f"  重复写入 10 个已存在的 key，检测到重复: {dup_count}/10 (期望 10)")

# 新 key 应该能插入
new_resp = stub.CheckAndSet(dedup_pb2.CheckAndSetRequest(
    namespace=ns,
    key=b"https://example.com/page/99999_new_after_restart",
    expire_at_ms=9999999999999,
))
print(f"  新 key 插入: {new_resp.result == dedup_pb2.CheckResult.INSERTED} (期望 True)")

print("\n✅ 重启恢复测试完成！")
channel.close()
