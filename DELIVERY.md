# RocksDB 去重引擎交付文档

## 1. 项目概述

基于 Rust + RocksDB 的高并发持久化去重引擎，专为爬虫场景设计。采用"双 Column Family + 内存布隆过滤器 + 后台过期清理"的三层架构，支持万亿级数据量下的微秒级查重。

提供两种使用方式：
- **嵌入模式**：作为 Rust 库直接引入爬虫框架
- **服务模式**：独立 gRPC 服务，支持多语言、多实例分布式调用

**交付状态**：✅ 已完成，33 个单元测试全部通过（含高优与中优功能升级）

---

## 2. 目录结构

```
rocksdb_demp/
├── Cargo.toml              # 项目依赖配置
├── build.rs                # proto 编译构建脚本
├── 去重系统.txt             # 原始设计文档
├── DELIVERY.md             # 本文档（交付说明）
├── .gitignore              # Git 忽略规则
│
├── proto/
│   └── dedup.proto         # gRPC 接口定义
│
├── src/
│   ├── lib.rs              # 库入口，导出公共 API
│   ├── main.rs             # 程序入口（demo / serve 两种模式）
│   ├── config.rs           # 配置模块（DedupConfig）
│   ├── error.rs            # 错误类型定义
│   ├── key.rs              # Key/Value 编解码（大端字节序）
│   ├── storage.rs          # RocksDB 双 CF 存储层
│   ├── bloom.rs            # 布隆过滤器加速层（Rayon 并行重建 + 分层热冷双层）
│   ├── cleaner.rs          # 过期清理后台任务（自适应限速 + 运行期暂停/恢复）
│   ├── engine.rs           # DedupEngine 主入口（并发批量 check/exists/delete）
│   ├── server.rs           # gRPC 服务端实现（含优雅停机支持）
│   └── metrics.rs          # HTTP 监控端点（Prometheus /metrics, /health, /stats）
│
├── benches/
│   └── dedup_bench.rs      # 基准测试
│
└── clients/
    └── python/
        ├── dedup_client.py       # Python 客户端封装
        ├── integration_test.py   # 集成测试脚本
        ├── test_restart.py       # 重启恢复测试
        ├── requirements.txt      # Python 依赖
        └── README.md             # Python 客户端使用说明
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
- **动态扩容**：使用率 > 80% / 误判率 > 1.5× / 使用率 ≥ 100% 时自动扩容 1.5 倍
- **定时重建**：默认每 24 小时全量重建一次，自动剔除过期 Key
- **高频扩容检查**：独立任务每 60 秒检查一次是否需要扩容，避免容量爆了来不及
- **冷启动恢复**：引擎启动时从 RocksDB 全量重建，保证查询正确性

### 3.4 过期清理机制

- 利用 `cf_expire` 的 Key 有序性，按时间范围顺序扫描
- **自适应限速**：根据积压量动态调整清理速度（4 档调速）
- 批量删除，批次间休眠限速
- 后台任务独立运行，不阻塞在线查询

| 积压量 | 速度档位 | batch_size | 休眠时间 |
|---|---|---|---|
| < 1 万 | 低速 | 1× | 正常 |
| 1 万 ~ 10 万 | 中速 | 1.5× | 1/2 |
| 10 万 ~ 100 万 | 高速 | 3× | 0 |
| > 100 万 | 全速 | 10× | 0 |

---

## 4. gRPC 服务

### 4.1 接口总览

8 个 RPC 方法，对应 proto 文件：`proto/dedup.proto`

| 方法 | 说明 |
|---|---|
| `CheckAndSet` | **核心接口**：原子检查并写入 |
| `BatchCheckAndSet` | 批量检查并写入（减少 RPC 次数） |
| `Exists` | 仅检查是否存在 |
| `BatchExists` | 批量检查存在性 |
| `Get` | 获取详细信息（过期时间、元数据） |
| `Delete` | 删除 key |
| `BatchDelete` | 批量删除 |
| `Stats` | 获取服务状态（监控用） |

### 4.2 启动服务

```bash
# 启动 gRPC 服务
cargo run -- serve \
  --addr 0.0.0.0:50051 \
  --db-path ./data/dedup \
  --bloom-capacity 10000000 \
  --bloom-fpp 0.01 \
  --cleaner-batch-size 10000
```

### 4.3 命令行参数

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--addr` | `0.0.0.0:50051` | gRPC 监听地址 |
| `--db-path` | `./data/dedup` | RocksDB 数据目录 |
| `--bloom-capacity` | 10,000,000 | 布隆过滤器初始容量 |
| `--bloom-fpp` | 0.01 | 目标误判率 |
| `--cleaner-batch-size` | 10,000 | 清理批大小 |

---

## 5. 公共 API（Rust 库）

### 5.1 核心查重接口

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

### 5.2 其他接口

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

### 5.3 配置项（DedupConfig）

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
| `max_metadata_size` | 4096 | 元数据最大字节数 |

---

## 6. Python 客户端

### 6.1 安装

```bash
cd clients/python
pip install -r requirements.txt
python -m grpc_tools.protoc -I../../proto --python_out=. --grpc_python_out=. ../../proto/dedup.proto
```

### 6.2 使用示例

```python
from dedup_client import DedupClient
import time

with DedupClient("localhost:50051") as client:
    ns = "spider_news"
    expire_at = int(time.time() * 1000) + 30 * 86400 * 1000  # 30天后

    # 核心接口
    result = client.check_and_set(
        namespace=ns,
        key=b"https://example.com/page/1",
        expire_at_ms=expire_at,
        metadata=b'{"depth": 3}',
    )

    if result == "inserted":
        print("新 URL，开始抓取")
    elif result == "duplicate":
        print("重复 URL，跳过")
    elif result == "expired_and_replaced":
        print("已过期，重新抓取")
```

### 6.3 客户端 API

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

---

## 7. 技术栈

| 组件 | 选型 | 版本 |
|---|---|---|
| 语言 | Rust | edition 2021 |
| 存储引擎 | rocksdb crate | 0.22（底层 RocksDB 8.10.0） |
| gRPC 框架 | tonic | 0.12 |
| Protocol Buffers | prost | 0.13 |
| 布隆过滤器 | bloomfilter crate | 1.0 |
| 异步运行时 | tokio | 1.x (full) |
| Hash 函数 | xxhash-rust (xxh3/xxh64) | 0.8 |
| 原子指针切换 | arc-swap | 1.x |
| 轻量级锁 | parking_lot | 0.12 |
| 错误处理 | thiserror + anyhow | 1.x |
| 命令行参数 | clap | 4.x |
| 时间 | chrono | 0.4 |
| 序列化 | serde + serde_json | 1.x |
| protoc 编译器 | protoc-bin-vendored | 3.x（内置，无需手动安装） |

---

## 8. 设计要点与正确性保证

### 8.1 Hash 冲突处理

Value 中存储完整的 `original_key`。布隆过滤器命中后查 RocksDB，Hash 匹配后进一步比对原始 Key，彻底避免 Hash 冲突误判。

### 8.2 原子性

所有写入通过 `WriteBatch` 同时操作 `cf_dedup` 和 `cf_expire`，保证两表数据一致。

### 8.3 字节序正确性

Key 中所有整数统一使用大端字节序编码，保证 RocksDB 字典序与数值序一致，是范围扫描正确的前提。已通过单元测试 `test_expire_key_ordering` 验证。

### 8.4 过期语义

- 过期判断：`expire_at_ms <= now_ms`（等于即过期）
- 过期数据不影响查重：`check_and_set` 遇到过期记录视为新 key，覆盖写入
- 过期清理是异步的：即使清理任务积压，在线查重仍然正确

### 8.5 布隆过滤器与存储的一致性

- 布隆过滤器是"有损"的：可能有误判（说存在但实际不存在），但不会漏判（说不存在一定不存在）
- 布隆过滤器不支持删除：Key 删除后仍留在过滤器中，下次重建时自动排除
- 冷启动重建：引擎启动时从 RocksDB 全量重建，保证重启后查询正确

### 8.6 扩容及时性

- 独立的扩容检查任务，每 60 秒检查一次
- 三重触发条件：使用率 > 80% / 误判率 > 1.5× / 使用率 ≥ 100%（紧急）
- 扩容时有日志输出，便于排查

---

## 9. 测试覆盖

### 9.1 单元测试（26 个，全部通过）

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

### 9.2 集成测试（4 项，全部通过）

| 测试项 | 验证内容 | 结果 |
|---|---|---|
| 基本去重功能 | 新插入/重复检测/exists/get/delete | ✅ 通过 |
| 布隆自动扩容 | 容量 10K → 写入 15K → 自动扩容到 22.6K | ✅ 通过（3秒触发） |
| 过期自动清理 | 500 条 2 秒过期 → 50 秒内清理完成 | ✅ 通过（100% 清理） |
| 重启恢复 | 重启后数据完整 + 布隆重建 + 去重正常 | ✅ 通过（0 数据丢失） |

### 9.3 运行测试

```bash
# 单元测试
cargo test --lib

# 集成测试（需先启动服务）
cargo run -- serve --addr 127.0.0.1:50051 --db-path ./data/test_dedup --bloom-capacity 10000
cd clients/python
python integration_test.py all

# 基准测试
cargo bench
```

---

## 10. 性能预期

### 10.1 内存占用

布隆过滤器内存 ≈ `容量 × 9.6 bit`（1% 误判率下）

| 日增量 | 内存占用 |
|---|---|
| 1000 万 | ~12 MB |
| 1 亿 | ~120 MB |
| 10 亿 | ~1.2 GB |

### 10.2 查询性能

- **快速路径**（布隆过滤器说不存在）：纯内存操作，纳秒级
- **慢速路径**（布隆过滤器命中 → RocksDB 查询）：微秒级，取决于磁盘 IO
- **整体 QPS**：在 99% 快速路径命中率下，单线程可达数百万 QPS
- **gRPC 开销**：gRPC 调用约几十微秒，是主要开销来源；建议用批量接口摊薄

### 10.3 写入性能

- 基于 LSM-Tree，写入性能极高
- 批量写入可进一步提升吞吐
- gRPC 服务模式下，建议使用 `BatchCheckAndSet` 批量接口

---

## 11. 部署建议

### 11.1 部署模式选择

| 场景 | 推荐模式 | 说明 |
|---|---|---|
| 单实例爬虫 | 嵌入模式 | 直接引入库，性能最好 |
| 多实例分布式爬虫 | gRPC 服务模式 | 共享去重状态，独立扩缩容 |
| 多业务线共用 | gRPC 服务模式 + namespace 隔离 | 一个服务管所有业务 |
| 超大单业务（>10 亿/天） | 独立实例 + 物理隔离 | 避免互相影响 |

### 11.2 硬件要求

| 规模 | CPU | 内存 | 磁盘 |
|---|---|---|---|
| 小型（< 1 亿 key） | 2 核 | 2 GB | SSD 10 GB |
| 中型（1~10 亿 key） | 4 核 | 8 GB | SSD 100 GB |
| 大型（10~100 亿 key） | 8 核 | 32 GB | SSD 1 TB |
| 超大型（> 100 亿 key） | 16 核+ | 64 GB+ | SSD 10 TB+ |

> 内存主要给布隆过滤器 + RocksDB block cache；磁盘存实际数据（每条约 100~200 字节）

---

## 12. 优化方向与实现状态

| 优化方向 | 状态 | 实现说明 |
|---|---|---|
| **布隆过滤器并行重建** | ✅ 已完成 | 基于 Rayon 多核并行计算键编码与批量填充，重建耗时大幅缩短 |
| **分层布隆过滤器** | ✅ 已完成 | `LayeredBloomManager` 热冷双层设计，热层快速插入，冷层后台定期无缝原子切换 |
| **多线程并发批量操作** | ✅ 已完成 | `batch_check_and_set`、`batch_exists`、`batch_delete` 均基于 Rayon 实现多核并行 |
| **监控与健康检查** | ✅ 已完成 | 独立 HTTP 运维端口（默认 9090），暴露 `/metrics` (Prometheus)、`/health`、`/stats` |
| **清理器运行期控制** | ✅ 已完成 | 支持 `pause` / `resume` 动态挂起后台清理任务，将 100% 磁盘 IO 保障给爬虫写入高峰 |
| **RocksDB 存储与分层压缩** | ✅ 已完成 | 共享 Block Cache；上层启用 LZ4 低延迟压缩，底层启用 ZSTD 极限高压缩比，专优爬虫 URL |
| **分布式分片集群客户端** | ✅ 已完成 | `ShardedDedupClient` 基于 160 虚拟节点一致性哈希环，自动拆分批量请求并发处理并保序重组 |
| **CLI 运维管理工具** | ✅ 已完成 | 支持 `status` 查看仪表盘、`pause-cleaner` / `resume-cleaner`、`check` 离线诊断 Key |

---

## 13. 常用运维与验证命令

```bash
# 1. 编译检查与全套测试（含 33 个核心单元测试）
cargo check
cargo test

# 2. 运行单机演示
cargo run -- demo

# 3. 启动去重服务（gRPC :50051 + HTTP 监控 :9090）
cargo run -- serve --addr 0.0.0.0:50051 --metrics-addr 0.0.0.0:9090 --block-cache-mb 512

# 4. 查看当前服务运行状态仪表盘
cargo run -- status --addr 127.0.0.1:9090

# 5. 爬虫高峰期暂停/恢复后台清理
cargo run -- pause-cleaner --addr 127.0.0.1:9090
cargo run -- resume-cleaner --addr 127.0.0.1:9090

# 6. 离线诊断指定 Key 状态与元数据
cargo run -- check --db-path ./data/dedup --namespace crawler --key "https://example.com/test"

# 7. 运行 Python 客户端与分片测试
cd clients/python
python -m unittest tests/test_sharded.py
```

---

*文档更新时间：2026-10-16*
*项目版本：v0.2.0*
