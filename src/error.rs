//! 错误类型定义

use thiserror::Error;

#[derive(Error, Debug)]
pub enum DedupError {
    #[error("RocksDB 错误: {0}")]
    RocksDb(#[from] rocksdb::Error),

    #[error("序列化/反序列化错误: {0}")]
    Codec(String),

    #[error("配置错误: {0}")]
    Config(String),

    #[error("IO 错误: {0}")]
    Io(#[from] std::io::Error),

    #[error("引擎已关闭")]
    Shutdown,
}

pub type Result<T> = std::result::Result<T, DedupError>;
