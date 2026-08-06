"""
去重服务集成测试

验证功能：
1. 基本去重功能（新插入/重复检测）
2. 布隆过滤器自动扩容
3. 服务重启后数据持久化 + 布隆过滤器重建
4. 过期数据自动清理
"""

import time
import sys

from dedup_client import DedupClient


def print_stats(client, label=""):
    """打印服务状态"""
    s = client.stats()
    print(f"  [{label}] total_keys={s['total_keys']:,}, "
          f"bloom_cap={s['bloom_capacity']:,}, "
          f"bloom_ins={s['bloom_insertions']:,}, "
          f"bloom_fpp={s['bloom_fpp']:.4%}, "
          f"cleaned={s['cleaned_total']:,}, "
          f"backlog={s['backlog_estimate']:,}")
    return s


def test_basic_dedup(client):
    """测试 1: 基本去重功能"""
    print("\n" + "=" * 60)
    print("测试 1: 基本去重功能")
    print("=" * 60)

    ns = "test_basic"
    now_ms = int(time.time() * 1000)
    expire_at = now_ms + 3600_000  # 1小时后过期

    # 写入 100 个新 key
    inserted = 0
    for i in range(100):
        key = f"https://example.com/page/{i}".encode()
        result = client.check_and_set(ns, key, expire_at)
        if result == "inserted":
            inserted += 1

    print(f"  写入 100 个新 key，实际插入: {inserted}")
    assert inserted == 100, f"期望插入 100，实际 {inserted}"

    # 重复写入，应该全部重复
    duplicates = 0
    for i in range(100):
        key = f"https://example.com/page/{i}".encode()
        result = client.check_and_set(ns, key, expire_at)
        if result == "duplicate":
            duplicates += 1

    print(f"  重复写入 100 个 key，实际重复: {duplicates}")
    assert duplicates == 100, f"期望重复 100，实际 {duplicates}"

    # 测试 exists 接口
    exists = client.exists(ns, b"https://example.com/page/50")
    print(f"  exists(存在的key): {exists}")
    assert exists

    not_exists = client.exists(ns, b"https://example.com/page/99999")
    print(f"  exists(不存在的key): {not_exists}")
    assert not not_exists

    # 测试 get 接口
    value = client.get(ns, b"https://example.com/page/42")
    print(f"  get(存在的key): found={value is not None}, expire={value['expire_at_ms']}")
    assert value is not None
    assert value["expire_at_ms"] == expire_at

    # 测试 delete 接口
    deleted = client.delete(ns, b"https://example.com/page/42")
    print(f"  delete: {deleted}")
    assert deleted

    # 删除后应该不存在了
    exists_after = client.exists(ns, b"https://example.com/page/42")
    assert not exists_after
    print(f"  删除后 exists: {exists_after}")

    print("  ✅ 基本去重功能测试通过")


def test_bloom_expansion(client):
    """测试 2: 布隆过滤器自动扩容"""
    print("\n" + "=" * 60)
    print("测试 2: 布隆过滤器自动扩容")
    print("=" * 60)

    ns = "test_expansion"
    now_ms = int(time.time() * 1000)
    expire_at = now_ms + 86400_000  # 1天后过期

    # 初始状态
    stats_before = print_stats(client, "初始")
    initial_cap = stats_before["bloom_capacity"]
    print(f"  初始容量: {initial_cap:,}")

    # 写入超过容量的数据（初始容量 10000，写入 15000 条）
    target_count = 15000
    print(f"  写入 {target_count:,} 条数据（超过初始容量 {initial_cap:,}）...")

    batch_size = 1000
    for batch_start in range(0, target_count, batch_size):
        batch_end = min(batch_start + batch_size, target_count)
        items = [
            (ns, f"https://expand-test.com/{i}".encode(), expire_at, None)
            for i in range(batch_start, batch_end)
        ]
        results = client.batch_check_and_set(items)
        inserted = sum(1 for r in results if r == "inserted")
        assert inserted == batch_size, f"批次 {batch_start} 插入数不对: {inserted}"

    # 写入后状态
    print_stats(client, "写入后")

    # 等待扩容检查任务触发（每 60 秒检查一次）
    print("  等待扩容检查任务触发（最多 70 秒）...")
    expanded = False
    for i in range(70):
        time.sleep(1)
        stats = print_stats(client, f"等待中 {i+1}s")
        if stats["bloom_capacity"] > initial_cap:
            print(f"  ✅ 检测到扩容！容量从 {initial_cap:,} 变为 {stats['bloom_capacity']:,}")
            expanded = True
            break

    assert expanded, "布隆过滤器没有自动扩容！"

    # 扩容后使用率应该下降
    stats_final = print_stats(client, "扩容后")
    usage = stats_final["bloom_insertions"] / stats_final["bloom_capacity"]
    print(f"  扩容后使用率: {usage:.2%}")
    assert usage < 0.8, f"扩容后使用率应该低于 80%，实际 {usage:.2%}"

    print("  ✅ 布隆过滤器自动扩容测试通过")


def test_restart_prepare(client):
    """测试 3: 服务重启前数据准备"""
    print("\n" + "=" * 60)
    print("测试 3: 服务重启前数据准备")
    print("=" * 60)

    ns = "test_restart"
    now_ms = int(time.time() * 1000)
    expire_at = now_ms + 86400_000

    # 先写入一些数据
    print("  写入 500 条数据...")
    for i in range(500):
        key = f"https://restart-test.com/{i}".encode()
        result = client.check_and_set(ns, key, expire_at)
        assert result == "inserted"

    stats_before = print_stats(client, "重启前")
    print(f"  重启前 total_keys: {stats_before['total_keys']:,}")

    # 验证数据存在
    for i in range(10):
        key = f"https://restart-test.com/{i}".encode()
        assert client.exists(ns, key), f"重启前 key {i} 应该存在"

    print("  ✅ 重启前数据验证通过")
    print("  （请重启服务后运行 restart_verify 继续验证）")

    return stats_before["total_keys"]


def test_restart_verify(client):
    """验证重启后数据"""
    print("\n" + "=" * 60)
    print("测试 3: 服务重启后数据验证")
    print("=" * 60)

    ns = "test_restart"

    stats_after = print_stats(client, "重启后")
    print(f"  重启后 total_keys: {stats_after['total_keys']:,}")

    # 验证数据仍然存在
    missing = 0
    for i in range(500):
        key = f"https://restart-test.com/{i}".encode()
        if not client.exists(ns, key):
            missing += 1

    print(f"  重启后丢失的 key 数: {missing}")
    assert missing == 0, f"重启后丢失了 {missing} 条数据！"

    # 验证布隆过滤器重建成功（容量应该大于 0）
    assert stats_after["bloom_capacity"] > 0, "布隆过滤器容量为 0，可能重建失败"
    print(f"  布隆过滤器容量: {stats_after['bloom_capacity']:,}")

    # 验证布隆过滤器重建后仍然能正确去重
    print("  验证布隆过滤器重建后去重正确...")
    dup_count = 0
    for i in range(10):
        key = f"https://restart-test.com/{i}".encode()
        result = client.check_and_set(ns, key, 9999999999999)
        if result == "duplicate":
            dup_count += 1
    print(f"  重复写入 10 个已存在的 key，检测到重复: {dup_count}/10 (期望 10)")
    assert dup_count == 10

    print("  ✅ 重启恢复测试通过")


def test_expiry_cleanup(client):
    """测试 4: 过期数据自动清理"""
    print("\n" + "=" * 60)
    print("测试 4: 过期数据自动清理")
    print("=" * 60)

    ns = "test_expiry"
    now_ms = int(time.time() * 1000)

    # 写入一批很快过期的数据（2 秒后过期）
    short_expire = now_ms + 2000  # 2秒后过期
    count = 500
    print(f"  写入 {count} 条 2 秒后过期的数据...")

    for i in range(count):
        key = f"https://expiry-test.com/short/{i}".encode()
        result = client.check_and_set(ns, key, short_expire)
        assert result == "inserted"

    # 写入一批长期有效的数据
    long_expire = now_ms + 86400_000  # 1天后过期
    long_count = 100
    print(f"  写入 {long_count} 条长期有效的数据...")
    for i in range(long_count):
        key = f"https://expiry-test.com/long/{i}".encode()
        result = client.check_and_set(ns, key, long_expire)
        assert result == "inserted"

    stats_before = print_stats(client, "过期前")

    # 等待过期 + 清理器运行
    # 清理器默认每 60 秒运行一次，我们等 65 秒
    print("  等待过期 + 清理器运行（最多 70 秒）...")
    cleaned = False
    for i in range(70):
        time.sleep(1)
        stats = print_stats(client, f"等待中 {i+1}s")
        if stats["cleaned_total"] >= count:
            print(f"  ✅ 检测到清理！已清理 {stats['cleaned_total']:,} 条")
            cleaned = True
            break

    if not cleaned:
        print("  清理器还没运行，再等一会儿...")
        for i in range(30):
            time.sleep(1)
            stats = print_stats(client, f"等待中 {70+i+1}s")
            if stats["cleaned_total"] >= count:
                print(f"  ✅ 检测到清理！已清理 {stats['cleaned_total']:,} 条")
                cleaned = True
                break

    # 验证短期数据已过期（查不到了）
    expired_missing = 0
    for i in range(count):
        key = f"https://expiry-test.com/short/{i}".encode()
        if not client.exists(ns, key):
            expired_missing += 1

    print(f"  短期数据已过期数: {expired_missing}/{count}")
    assert expired_missing == count, f"应该全部过期，实际过期 {expired_missing}"

    # 验证长期数据仍然存在
    long_existing = 0
    for i in range(long_count):
        key = f"https://expiry-test.com/long/{i}".encode()
        if client.exists(ns, key):
            long_existing += 1

    print(f"  长期数据仍存在数: {long_existing}/{long_count}")
    assert long_existing == long_count, f"长期数据应该全部存在，实际 {long_existing}"

    print("  ✅ 过期清理测试通过")


def main():
    if len(sys.argv) < 2:
        print("用法: python test_integration.py <test_name>")
        print("  test_name: basic | expansion | restart_prepare | restart_verify | expiry | all")
        print()
        print("重启测试流程:")
        print("  1. python test_integration.py restart_prepare")
        print("  2. 手动重启去重服务")
        print("  3. python test_integration.py restart_verify")
        sys.exit(1)

    test_name = sys.argv[1]
    addr = "127.0.0.1:50051"

    client = DedupClient(addr)

    try:
        # 先确认服务可用
        client.stats()
        print(f"已连接到去重服务: {addr}")
    except Exception as e:
        print(f"无法连接到去重服务 {addr}: {e}")
        sys.exit(1)

    try:
        if test_name == "basic":
            test_basic_dedup(client)
        elif test_name == "expansion":
            test_bloom_expansion(client)
        elif test_name == "restart_prepare":
            test_restart_prepare(client)
        elif test_name == "restart_verify":
            test_restart_verify(client)
        elif test_name == "expiry":
            test_expiry_cleanup(client)
        elif test_name == "all":
            test_basic_dedup(client)
            test_bloom_expansion(client)
            test_expiry_cleanup(client)
            print("\n" + "=" * 60)
            print("🎉 所有测试通过！")
            print("=" * 60)
        else:
            print(f"未知测试: {test_name}")
            sys.exit(1)
    finally:
        client.close()


if __name__ == "__main__":
    main()
