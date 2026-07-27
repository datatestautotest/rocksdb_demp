# RocksDB 去重引擎交付文档

## 1. 项目概述

基于 Rust + RocksDB 的高并发持久化去重引擎，专为爬虫场景设计。采用"双 Column Family + 内存布隆过滤器 + 后台过期清理"的三层架构，支持万亿级数据量下的微秒级查重。

**交付状态**：✅ 已完成，23 个单元测试全部通过

---

## 2. 目录结构

```
rocksdb_demp/
├── Cargo.toml              # 项目依赖配置
├── 去重系统.txt             # 原始设计文档
├── DELIVERY.md             # 本文档（交付说明）
├── src/
│   ├── lib.rs              # 库入口，导出公共 API
│   ├── main.rs             # 示例程序
│   ├── config.rs           # 配置模块（DedupConfig）
│   ├── error.rs            # 错误类型定义
│   ├── key.rs              # Key/Value 编解码（大端字节序）
│   ├── storage.rs          # RocksDB 双 CF 存储层
│   ├── bloom.rs            # 布隆过滤器加速层
│   ├── cleaner.rs          # 过期清理后台任务
│   └── engine.rs           # DedupEngine 主入口
└── benches/
    └── dedup_bench.rs      # 基准测试
```

---

## 3. 核心架构

### 3.1 三层架构

```
请求 → 布隆过滤器（内存，微秒级）
       ├── 不存在 → 直接返回"不重复"（快速路径，>90% 请求）
       └── 可能存在 → RocksDB cf_dedup（精确确认）
                      ├── 不存在 → 写入 cf_dedup + cf_expire（原子）
                      └── 存在 → 比对原始 Key（防 Hash 冲突），检查过期
```

### 3.2 双 Column Family 设计

| Column Family | Key 结构 | Value | 用途 |
|---|---|---|---|
| `cf_dedup` | `namespace_hash:u64 (BE)` + `key_hash:u128 (BE)` | `expire_at_ms + created_at_ms + original_key + metadata` | 精确查重主表 |
| `cf_expire` | `expire_at_ms:u64 (BE)` + `namespace_hash:u64 (BE)` + `key_hash:u128 (BE)` | 空 | 过期时间索引，支持范围扫描 |

**关键约束**：所有多字节整数统一使用大端字节序（`.to_be_bytes()`），保证字典序与数值序一致。

### 3.3 布隆过滤器管理

- **双缓冲切换**：`ArcSwap<CountingBloom>`，后台重建完成后原子替换
- **动态扩容**：使用率 > 80% 或误判率 > 1.5× 设计值时自动扩容 1.5 倍
- **定时重建**：默认每 24 小时全量重建一次，自动剔除过期 Key
- **冷启动恢复**：引擎启动时从 RocksDB 全量重建，保证查询正确性

### 3.4 过期清理机制

- 利用 `cf_expire` 的 Key 有序性，按时间范围顺序扫描
- 批量删除（默认每批 10000 条），批次间休眠限速
- 后台任务独立运行，不阻塞在线查询

---

## 4. 公共 API

### 4.1 核心查重接口

```rust
/// 检查并写入（原子操作）
pub fn check_and_set(
    &self,
    namespace: &[u8],      // 命名空间，区分不同业务/爬虫
    key: &[u8],            // 待查重 Key（如 URL）
    expire_at_ms: u64,     // 绝对过期时间戳（毫秒）
    metadata: Option<Vec<u8>>, // 可选业务元数据
) -> Result<CheckResult>

// 返回值：
// - CheckResult::Inserted           新写入成功
// - CheckResult::Duplicate          已存在且未过期（含 existing 记录）
// - CheckResult::ExpiredAndReplaced 已过期，已覆盖为新值（含 old 记录）
```

### 4.2 其他接口

| 方法 | 说明 |
|---|---|
| `exists(namespace, key)` | 仅检查是否存在且未过期 |
| `get(namespace, key)` | 获取完整记录（含元数据） |
| `delete(namespace, key)` | 删除一条记录 |
| `batch_check_and_set(items)` | 批量查重写入 |
| `bloom_stats()` | 获取布隆过滤器统计 |
| `cleaner_stats()` | 获取清理器统计 |
| `count_valid_keys()` | 统计有效 Key 数量（全表扫描，慎用） |
| `rebuild_bloom()` | 手动触发布隆过滤器重建 |
| `clean_expired()` | 手动触发过期清理 |
| `shutdown()` | 优雅关闭 |

### 4.3 配置项（DedupConfig）

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `db_path` | `./data/dedup` | RocksDB 数据目录 |
| `bloom_capacity` | 100,000,000 | 布隆过滤器设计容量（条） |
| `bloom_false_positive_rate` | 0.01 | 目标误判率（1%） |
| `bloom_rebuild_interval_secs` | 86400 (24h) | 定时重建周期 |
| `bloom_expand_threshold` | 0.8 | 扩容阈值（使用率） |
| `bloom_expand_factor` | 1.5 | 扩容倍数 |
| `cleaner_interval_secs` | 60 | 清理任务执行间隔 |
| `cleaner_batch_size` | 10,000 | 基础每批清理数量（自适应调整） |
| `cleaner_batch_sleep_ms` | 10 | 基础批间休眠（自适应调整） |

> **关于积压清理不完的问题**：清理器采用**自适应限速**机制，根据过期数据积压量动态调整清理速度：
> - 积压 < 1万：低速（基础 batch + 正常休眠）
> - 积压 1万~10万：中速（1.5× batch + 半速休眠）
> - 积压 10万~100万：高速（3× batch + 不休眠）
> - 积压 > 100万：全速（10× batch + 不休眠，全力清理）
>
> 同时提供 `backlog_estimate` 和 `current_speed` 监控指标，可接入告警。

---

## 5. 技术栈

| 组件 | 选型 | 版本 |
|---|---|---|
| 语言 | Rust | edition 2021 |
| 存储引擎 | rocksdb crate | 0.22（底层 RocksDB 8.10.0） |
| 布隆过滤器 | bloomfilter crate | 1.0 |
| 异步运行时 | tokio | 1.x (full) |
| Hash 函数 | xxhash-rust (xxh3/xxh64) | 0.8 |
| 原子指针切换 | arc-swap | 1.x |
| 轻量级锁 | parking_lot | 0.12 |
| 错误处理 | thiserror + anyhow | 1.x |
| 时间 | chrono | 0.4 |
| 序列化 | serde + serde_json | 1.x |

---

## 6. 设计要点与正确性保证

### 6.1 Hash 冲突处理

Value 中存储完整的 `original_key`。布隆过滤器命中后查 RocksDB，Hash 匹配后进一步比对原始 Key，彻底避免 Hash 冲突误判。

### 6.2 原子性

所有写入通过 `WriteBatch` 同时操作 `cf_dedup` 和 `cf_expire`，保证两表数据一致。

### 6.3 字节序正确性

Key 中所有整数统一使用大端字节序编码，保证 RocksDB 字典序与数值序一致，是范围扫描正确的前提。已通过单元测试 `test_expire_key_ordering` 验证。

### 6.4 过期语义

- 过期判断：`expire_at_ms <= now_ms`（等于即过期）
- 过期数据不影响查重：`check_and_set` 遇到过期记录视为新 key，覆盖写入
- 过期清理是异步的：即使清理任务积压，在线查重仍然正确

### 6.5 布隆过滤器与存储的一致性

- 布隆过滤器是"有损"的：可能有误判（说存在但实际不存在），但不会漏判（说不存在一定不存在）
- 布隆过滤器不支持删除：Key 删除后仍留在过滤器中，下次重建时自动排除
- 冷启动重建：引擎启动时从 RocksDB 全量重建，保证重启后查询正确

---

## 7. 测试覆盖

### 7.1 单元测试（26 个，全部通过）

| 模块 | 测试用例 | 覆盖点 |
|---|---|---|
| key | `test_dedup_key_roundtrip` | cf_dedup Key 编解码 |
| key | `test_expire_key_roundtrip` | cf_expire Key 编解码 |
| key | `test_expire_key_ordering` | 大端字节序排序正确性 |
| key | `test_value_roundtrip` | Value 编解码 |
| key | `test_value_no_metadata` | 无元数据情况 |
| key | `test_is_expired` | 过期判断 |
| storage | `test_put_and_get` | 基础读写 |
| storage | `test_delete` | 删除操作 |
| storage | `test_scan_expired` | 过期范围扫描 |
| storage | `test_delete_expired_batch` | 批量过期删除 |
| bloom | `test_bloom_basic` | 布隆过滤器基础操作 |
| bloom | `test_bloom_stats` | 统计信息 |
| bloom | `test_bloom_manager_rebuild` | 从 RocksDB 重建 |
| cleaner | `test_clean_all_sync` | 同步全量清理 |
| cleaner | `test_cleaner_start_stop` | 后台任务启停 |
| cleaner | `test_adaptive_speed` | 自适应限速算法 |
| cleaner | `test_estimate_backlog` | 积压量估算 |
| cleaner | `test_stats_share_state` | 状态共享一致性 |
| engine | `test_check_and_set_basic` | 核心查重逻辑 |
| engine | `test_exists` | exists 接口 |
| engine | `test_expired_and_replaced` | 过期覆盖逻辑 |
| engine | `test_delete` | 删除接口 |
| engine | `test_metadata` | 元数据存取 |
| engine | `test_batch` | 批量操作 |
| engine | `test_concurrent_access` | 并发访问正确性 |
| engine | `test_shutdown` | 持久化与重启恢复 |

### 7.2 运行测试

```bash
# 运行所有单元测试
cargo test --lib

# 运行基准测试
cargo bench
```

---

## 8. 性能预期

### 8.1 内存占用

布隆过滤器内存 ≈ `容量 × 9.6 bit`（1% 误判率下）

| 日增量 | 内存占用 |
|---|---|
| 1000 万 | ~12 MB |
| 1 亿 | ~120 MB |
| 10 亿 | ~1.2 GB |

### 8.2 查询性能

- **快速路径**（布隆过滤器说不存在）：纯内存操作，纳秒级
- **慢速路径**（布隆过滤器命中 → RocksDB 查询）：微秒级，取决于磁盘 IO
- **整体 QPS**：在 99% 快速路径命中率下，单线程可达数百万 QPS

### 8.3 写入性能

- 基于 LSM-Tree，写入性能极高
- 批量写入可进一步提升吞吐

---

## 9. 使用示例

```rust
use rocksdb_dedup::{DedupConfig, DedupEngine};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = DedupConfig::default();
    let engine = DedupEngine::open(config).await?;

    let ns = b"crawler_news";
    let expire_at = chrono::Utc::now().timestamp_millis() as u64 + 30 * 86400_000;

    let url = "https://example.com/news/123";
    match engine.check_and_set(ns, url.as_bytes(), expire_at, None)? {
        rocksdb_dedup::CheckResult::Inserted => {
            println!("新 URL，开始抓取");
        }
        rocksdb_dedup::CheckResult::Duplicate { existing } => {
            println!("重复 URL，跳过。创建于: {:?}", existing.created_at_ms);
        }
        rocksdb_dedup::CheckResult::ExpiredAndReplaced { old } => {
            println!("URL 已过期，重新抓取。旧过期时间: {:?}", old.expire_at_ms);
        }
    }

    engine.shutdown().await?;
    Ok(())
}
```

---

## 10. 后续优化方向

1. **布隆过滤器重建优化**：并行扫描 RocksDB，缩短大数量级下的重建时间
2. **分层布隆过滤器**：热数据 + 冷数据多层设计，进一步降低内存和重建频率
3. **自适应清理限速**：基于系统负载动态调整清理速度，高峰期暂停
4. **监控告警**：集成 metrics / prometheus，监控命中率、延迟、数据积压
5. **多线程写入优化**：使用 `WriteBatch` + 多线程流水线进一步提升写入吞吐
6. **压缩配置**：针对爬虫 URL 数据特点，优化 RocksDB 压缩算法

---

## 11. 验证命令

```bash
# 编译检查
cargo check

# 单元测试
cargo test --lib

# 运行示例
cargo run

# 基准测试
cargo bench
```

---

*文档生成时间：2026-07-27*
*项目版本：v0.1.0*
