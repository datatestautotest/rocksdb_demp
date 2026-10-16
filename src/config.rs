//! 配置模块

use serde::{Deserialize, Serialize};

/// 去重引擎配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DedupConfig {
    /// RocksDB 数据目录
    pub db_path: String,

    /// 布隆过滤器预期容量（每次重建时的设计容量，单位：条）
    pub bloom_capacity: u64,

    /// 布隆过滤器误判率（0~1，推荐 0.01）
    pub bloom_false_positive_rate: f64,

    /// 布隆过滤器重建周期（秒）
    pub bloom_rebuild_interval_secs: u64,

    /// 布隆过滤器自动扩容阈值（插入量/容量 > 该值时触发扩容重建）
    pub bloom_expand_threshold: f64,

    /// 布隆过滤器扩容倍数
    pub bloom_expand_factor: f64,

    /// 过期清理扫描间隔（秒）
    pub cleaner_interval_secs: u64,

    /// 过期清理每批次处理的 Key 数量
    pub cleaner_batch_size: usize,

    /// 过期清理每批次之间的休眠时间（毫秒），用于限速
    pub cleaner_batch_sleep_ms: u64,

    /// 单条 Value 中 metadata 的最大字节数
    pub max_metadata_size: usize,

    /// RocksDB Block Cache 缓存大小（MB），默认 256MB
    pub block_cache_size_mb: usize,

    /// 是否开启 LSM-Tree 分层压缩（LZ4 + 底层 ZSTD）
    pub enable_compression: bool,

    /// 数据块大小（KB），默认 16KB（适合随机点查）
    pub block_size_kb: usize,
}

impl Default for DedupConfig {
    fn default() -> Self {
        Self {
            db_path: "./data/dedup".to_string(),
            // 默认按日增量 1 亿条设计，约 960MB 内存（1e8 * 9.6 bit）
            bloom_capacity: 100_000_000,
            bloom_false_positive_rate: 0.01,
            bloom_rebuild_interval_secs: 24 * 3600, // 24 小时
            bloom_expand_threshold: 0.8,
            bloom_expand_factor: 1.5,
            cleaner_interval_secs: 60,  // 每分钟
            cleaner_batch_size: 10_000,
            cleaner_batch_sleep_ms: 10,
            max_metadata_size: 1024,
            block_cache_size_mb: 256,
            enable_compression: true,
            block_size_kb: 16,
        }
    }
}

impl DedupConfig {
    /// 快速构造一个小容量配置，用于测试
    pub fn for_test(db_path: impl Into<String>) -> Self {
        Self {
            db_path: db_path.into(),
            bloom_capacity: 10_000,
            bloom_false_positive_rate: 0.01,
            bloom_rebuild_interval_secs: 60,
            bloom_expand_threshold: 0.8,
            bloom_expand_factor: 1.5,
            cleaner_interval_secs: 1,
            cleaner_batch_size: 100,
            cleaner_batch_sleep_ms: 1,
            max_metadata_size: 256,
            block_cache_size_mb: 16,
            enable_compression: false,
            block_size_kb: 4,
        }
    }
}
