//! 基于 RocksDB 与布隆过滤器的高并发持久化去重引擎
//!
//! 核心设计：
//! - 双 Column Family：`cf_dedup` 精确查重，`cf_expire` 过期扫描
//! - 内存布隆过滤器：拦截绝大多数不存在 Key 的查询，避免磁盘 IO
//! - 后台过期清理：基于 cf_expire 的有序 Key 做范围扫描删除
//! - 大端字节序：保证字典序与数值序一致，范围扫描正确

pub mod config;
pub mod error;
pub mod key;
pub mod bloom;
pub mod storage;
pub mod cleaner;
pub mod engine;
pub mod server;

pub use config::DedupConfig;
pub use engine::DedupEngine;
pub use error::DedupError;
pub use key::{DedupValue, NamespaceId};
pub use server::{serve, DedupServiceServer, DedupServiceImpl};
