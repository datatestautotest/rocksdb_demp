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

    /// 过期数据清理累计删除数量达到该阈值时，自动触发 RocksDB 范围 Compaction 回收墓碑与磁盘碎片（0 表示禁用自动压缩）
    pub cleaner_compact_threshold: u64,

    /// API 访问密钥 Token（用于 gRPC 客户端鉴权，None 或空表示不启用鉴权）
    #[serde(default)]
    pub auth_token: Option<String>,
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
            cleaner_compact_threshold: 100_000,
            auth_token: None,
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
            cleaner_compact_threshold: 500,
            auth_token: None,
        }
    }

    /// 从 TOML 配置文件加载配置
    pub fn from_file(path: impl AsRef<std::path::Path>) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: Self = toml::from_str(&content)?;
        Ok(config)
    }

    /// 将配置保存为 TOML 文件
    pub fn to_file(&self, path: impl AsRef<std::path::Path>) -> anyhow::Result<()> {
        let content = toml::to_string_pretty(self)?;
        std::fs::write(path, content)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_config_file_roundtrip() {
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path();

        let mut config = DedupConfig::default();
        config.bloom_capacity = 50_000_000;
        config.cleaner_compact_threshold = 200_000;
        config.to_file(path).unwrap();

        let loaded = DedupConfig::from_file(path).unwrap();
        assert_eq!(loaded.bloom_capacity, 50_000_000);
        assert_eq!(loaded.cleaner_compact_threshold, 200_000);
        assert_eq!(loaded.cleaner_batch_size, 10_000);
    }

    #[test]
    fn test_example_toml_valid() {
        let example_path = std::path::Path::new("dedup.example.toml");
        if example_path.exists() {
            let config = DedupConfig::from_file(example_path).unwrap();
            assert_eq!(config.bloom_capacity, 100_000_000);
            assert_eq!(config.cleaner_compact_threshold, 100_000);
        }
    }
}
