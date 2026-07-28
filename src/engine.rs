//! 去重引擎主入口
//!
//! 整合 RocksDB 存储层、布隆过滤器加速层、过期清理后台任务，
//! 提供简洁的对外 API。
//!
//! 典型用法：
//! ```ignore
//! use rocksdb_dedup::{DedupEngine, DedupConfig};
//!
//! let config = DedupConfig::default();
//! let engine = DedupEngine::open(config).await?;
//!
//! // 检查并设置（原子操作）
//! let is_dup = engine.check_and_set(
//!     b"crawler_ns",
//!     b"https://example.com/page/1",
//!     expire_at_ms,
//!     Some(b"metadata".to_vec()),
//! ).await?;
//!
//! if is_dup {
//!     println!("重复 URL，跳过");
//! } else {
//!     println!("新 URL，开始抓取");
//! }
//! ```

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::bloom::{BloomManager, BloomStats};
use crate::cleaner::{CleanerStats, ExpiryCleaner};
use crate::config::DedupConfig;
use crate::error::Result;
use crate::key::{hash_key, hash_namespace, DedupValue};
use crate::storage::DedupStore;

/// 查重结果
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckResult {
    /// Key 不存在，已成功写入
    Inserted,
    /// Key 已存在且未过期（重复）
    Duplicate {
        /// 已存在的记录
        existing: DedupValue,
    },
    /// Key 已存在但已过期，已被覆盖为新值
    ExpiredAndReplaced {
        /// 旧的过期记录
        old: DedupValue,
    },
}

impl CheckResult {
    /// 是否为重复（未过期的重复）
    pub fn is_duplicate(&self) -> bool {
        matches!(self, CheckResult::Duplicate { .. })
    }

    /// 是否为新插入
    pub fn is_inserted(&self) -> bool {
        matches!(self, CheckResult::Inserted)
    }
}

/// 去重引擎
pub struct DedupEngine {
    store: DedupStore,
    bloom: Arc<BloomManager>,
    cleaner: ExpiryCleaner,
    config: DedupConfig,
    /// 布隆过滤器重建任务句柄
    rebuild_handle: Option<JoinHandle<()>>,
    /// 布隆过滤器重建停止信号
    rebuild_stop_tx: Option<mpsc::Sender<()>>,
    /// 布隆过滤器扩容检查任务句柄（高频检查）
    expand_handle: Option<JoinHandle<()>>,
    /// 扩容检查停止信号
    expand_stop_tx: Option<mpsc::Sender<()>>,
}

impl DedupEngine {
    /// 打开去重引擎
    pub async fn open(config: DedupConfig) -> Result<Self> {
        let store = DedupStore::open(&config)?;
        let bloom = Arc::new(BloomManager::new(&config));
        let cleaner = ExpiryCleaner::new(store.clone(), &config);

        let mut engine = Self {
            store,
            bloom,
            cleaner,
            config,
            rebuild_handle: None,
            rebuild_stop_tx: None,
            expand_handle: None,
            expand_stop_tx: None,
        };

        // 启动后台任务
        engine.start_background_tasks().await?;

        // 启动时从 RocksDB 重建布隆过滤器，保证冷启动正确性
        // 对于空数据库，这一步很快；对于已有数据，保证查询正确性
        let now_ms = chrono::Utc::now().timestamp_millis() as u64;
        engine.bloom.rebuild_from_store(&engine.store, now_ms)?;

        Ok(engine)
    }

    /// 启动所有后台任务
    async fn start_background_tasks(&mut self) -> Result<()> {
        // 启动过期清理器
        self.cleaner.start();

        // 启动布隆过滤器定时重建任务
        self.start_bloom_rebuild_task();

        // 启动布隆过滤器扩容检查任务（高频，每 60 秒检查一次）
        self.start_bloom_expand_check_task();

        Ok(())
    }

    /// 启动布隆过滤器扩容检查任务（高频检查容量，及时扩容）
    fn start_bloom_expand_check_task(&mut self) {
        let (stop_tx, mut stop_rx) = mpsc::channel::<()>(1);
        let bloom = self.bloom.clone();
        let store = self.store.clone();

        let handle = tokio::spawn(async move {
            // 每 60 秒检查一次是否需要扩容
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            // 第一帧跳过
            interval.tick().await;

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if bloom.needs_rebuild() {
                            let now_ms = chrono::Utc::now().timestamp_millis() as u64;
                            let stats = bloom.stats();
                            tracing_info(&format!(
                                "[bloom] 检测到需要扩容 (使用率 {:.1}%, 观测误判率 {:.4}%)，开始扩容重建...",
                                stats.usage_ratio * 100.0,
                                stats.estimated_fpr * 100.0
                            ));
                            match bloom.expand_rebuild(&store, now_ms) {
                                Ok(count) => {
                                    let new_stats = bloom.stats();
                                    tracing_info(&format!(
                                        "[bloom] 扩容重建完成，插入 {} 条，新容量 {} (使用率 {:.1}%)",
                                        count,
                                        new_stats.capacity,
                                        new_stats.usage_ratio * 100.0
                                    ));
                                }
                                Err(e) => {
                                    eprintln!("[bloom] 扩容重建失败: {}", e);
                                }
                            }
                        }
                    }
                    _ = stop_rx.recv() => {
                        break;
                    }
                }
            }
        });

        self.expand_stop_tx = Some(stop_tx);
        self.expand_handle = Some(handle);
    }

    /// 启动布隆过滤器定时重建 + 自动扩容任务
    fn start_bloom_rebuild_task(&mut self) {
        let (stop_tx, mut stop_rx) = mpsc::channel::<()>(1);
        let bloom = self.bloom.clone();
        let store = self.store.clone();
        let interval_secs = self.config.bloom_rebuild_interval_secs;

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
            // 第一帧跳过，避免启动时立即重建
            interval.tick().await;

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let now_ms = chrono::Utc::now().timestamp_millis() as u64;

                        // 检查是否需要扩容重建
                        if bloom.needs_rebuild() {
                            match bloom.expand_rebuild(&store, now_ms) {
                                Ok(count) => {
                                    tracing_info(&format!("[bloom] 扩容重建完成，插入 {} 条", count));
                                }
                                Err(e) => {
                                    eprintln!("[bloom] 扩容重建失败: {}", e);
                                }
                            }
                        } else {
                            // 定时全量重建
                            match bloom.rebuild_from_store(&store, now_ms) {
                                Ok(count) => {
                                    tracing_info(&format!("[bloom] 定时重建完成，插入 {} 条", count));
                                }
                                Err(e) => {
                                    eprintln!("[bloom] 定时重建失败: {}", e);
                                }
                            }
                        }
                    }
                    _ = stop_rx.recv() => {
                        break;
                    }
                }
            }
        });

        self.rebuild_stop_tx = Some(stop_tx);
        self.rebuild_handle = Some(handle);
    }

    // -----------------------------------------------------------------------
    // 核心 API
    // -----------------------------------------------------------------------

    /// 检查 Key 是否存在，若不存在则写入（核心查重接口）
    ///
    /// # 参数
    /// - `namespace`: 命名空间（用于区分不同业务/爬虫任务）
    /// - `key`: 待查重的 Key（如 URL）
    /// - `expire_at_ms`: 绝对过期时间戳（毫秒）
    /// - `metadata`: 可选的业务元数据
    ///
    /// # 返回
    /// - `CheckResult::Inserted`: 新写入成功
    /// - `CheckResult::Duplicate`: 已存在且未过期
    /// - `CheckResult::ExpiredAndReplaced`: 已过期，已覆盖为新值
    pub fn check_and_set(
        &self,
        namespace: &[u8],
        key: &[u8],
        expire_at_ms: u64,
        metadata: Option<Vec<u8>>,
    ) -> Result<CheckResult> {
        let ns_hash = hash_namespace(namespace);
        let key_hash = hash_key(key);
        let now_ms = chrono::Utc::now().timestamp_millis() as u64;

        // 快速路径：布隆过滤器
        if !self.bloom.may_exist(ns_hash, key_hash) {
            // 布隆过滤器说不存在 → 一定不存在，直接写入
            let value = DedupValue {
                expire_at_ms,
                created_at_ms: now_ms,
                original_key: key.to_vec(),
                metadata: metadata.unwrap_or_default(),
            };
            self.store.put(ns_hash, key_hash, &value)?;
            self.bloom.insert(ns_hash, key_hash);
            return Ok(CheckResult::Inserted);
        }

        // 慢速路径：布隆过滤器说可能存在 → 查 RocksDB 确认
        match self.store.get_dedup(ns_hash, key_hash)? {
            Some(existing) => {
                // Hash 匹配后，必须比对原始 Key 以避免 Hash 冲突
                if existing.original_key != key {
                    // Hash 冲突！当作不存在处理
                    self.bloom.record_real_hit(); // 注意：这里不算真实命中
                    let value = DedupValue {
                        expire_at_ms,
                        created_at_ms: now_ms,
                        original_key: key.to_vec(),
                        metadata: metadata.unwrap_or_default(),
                    };
                    self.store.put(ns_hash, key_hash, &value)?;
                    // 布隆过滤器已经有这个 hash 了，不需要再插入
                    return Ok(CheckResult::Inserted);
                }

                self.bloom.record_real_hit();

                if !existing.is_expired(now_ms) {
                    // 未过期 → 重复
                    Ok(CheckResult::Duplicate { existing })
                } else {
                    // 已过期 → 覆盖
                    // 需要先删除旧的 expire 索引（旧的 expire_at）
                    self.store.delete(ns_hash, key_hash, existing.expire_at_ms)?;
                    let value = DedupValue {
                        expire_at_ms,
                        created_at_ms: now_ms,
                        original_key: key.to_vec(),
                        metadata: metadata.unwrap_or_default(),
                    };
                    self.store.put(ns_hash, key_hash, &value)?;
                    // 布隆过滤器已有，无需更新
                    Ok(CheckResult::ExpiredAndReplaced { old: existing })
                }
            }
            None => {
                // 布隆过滤器误判 → 实际不存在，写入
                let value = DedupValue {
                    expire_at_ms,
                    created_at_ms: now_ms,
                    original_key: key.to_vec(),
                    metadata: metadata.unwrap_or_default(),
                };
                self.store.put(ns_hash, key_hash, &value)?;
                self.bloom.insert(ns_hash, key_hash);
                Ok(CheckResult::Inserted)
            }
        }
    }

    /// 仅检查 Key 是否存在且未过期（不写入）
    pub fn exists(&self, namespace: &[u8], key: &[u8]) -> Result<bool> {
        let ns_hash = hash_namespace(namespace);
        let key_hash = hash_key(key);
        let now_ms = chrono::Utc::now().timestamp_millis() as u64;

        // 快速路径
        if !self.bloom.may_exist(ns_hash, key_hash) {
            return Ok(false);
        }

        // 慢速路径
        match self.store.get_dedup(ns_hash, key_hash)? {
            Some(v) => {
                if v.original_key == key && !v.is_expired(now_ms) {
                    self.bloom.record_real_hit();
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            None => Ok(false),
        }
    }

    /// 获取 Key 的详细信息（不存在返回 None）
    pub fn get(&self, namespace: &[u8], key: &[u8]) -> Result<Option<DedupValue>> {
        let ns_hash = hash_namespace(namespace);
        let key_hash = hash_key(key);

        if !self.bloom.may_exist(ns_hash, key_hash) {
            return Ok(None);
        }

        match self.store.get_dedup(ns_hash, key_hash)? {
            Some(v) => {
                if v.original_key == key {
                    self.bloom.record_real_hit();
                    Ok(Some(v))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    /// 删除一个 Key
    pub fn delete(&self, namespace: &[u8], key: &[u8]) -> Result<bool> {
        let ns_hash = hash_namespace(namespace);
        let key_hash = hash_key(key);

        // 先查值获取 expire_at
        match self.store.get_dedup(ns_hash, key_hash)? {
            Some(v) => {
                if v.original_key == key {
                    self.store.delete(ns_hash, key_hash, v.expire_at_ms)?;
                    // 注意：布隆过滤器不支持删除，这里只能忽略
                    // 下一次重建时会自动排除
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            None => Ok(false),
        }
    }

    /// 批量查重并写入（提高吞吐）
    ///
    /// 返回与输入等长的结果向量
    pub fn batch_check_and_set(
        &self,
        items: &[(&[u8], &[u8], u64, Option<Vec<u8>>)],
    ) -> Result<Vec<CheckResult>> {
        let mut results = Vec::with_capacity(items.len());
        for (ns, key, expire_at, meta) in items {
            results.push(self.check_and_set(ns, key, *expire_at, meta.clone())?);
        }
        Ok(results)
    }

    // -----------------------------------------------------------------------
    // 统计与监控
    // -----------------------------------------------------------------------

    /// 获取布隆过滤器统计
    pub fn bloom_stats(&self) -> BloomStats {
        self.bloom.stats()
    }

    /// 获取清理器统计
    pub fn cleaner_stats(&self) -> CleanerStats {
        self.cleaner.stats()
    }

    /// 统计有效 Key 数量（全表扫描，慎用！）
    pub fn count_valid_keys(&self) -> Result<usize> {
        let now_ms = chrono::Utc::now().timestamp_millis() as u64;
        self.store.count_valid_keys(now_ms)
    }

    /// 手动触发一次布隆过滤器重建（同步阻塞）
    pub fn rebuild_bloom(&self) -> Result<u64> {
        let now_ms = chrono::Utc::now().timestamp_millis() as u64;
        self.bloom.rebuild_from_store(&self.store, now_ms)
    }

    /// 手动触发一次全量过期清理（同步阻塞）
    pub fn clean_expired(&self) -> Result<u64> {
        let now_ms = chrono::Utc::now().timestamp_millis() as u64;
        self.cleaner.clean_all_sync(now_ms)
    }

    // -----------------------------------------------------------------------
    // 生命周期
    // -----------------------------------------------------------------------

    /// 优雅关闭：停止后台任务，刷写数据
    pub async fn shutdown(mut self) -> Result<()> {
        // 停止布隆过滤器扩容检查
        if let Some(tx) = self.expand_stop_tx.take() {
            let _ = tx.send(()).await;
        }
        if let Some(handle) = self.expand_handle.take() {
            let _ = handle.await;
        }

        // 停止布隆过滤器重建
        if let Some(tx) = self.rebuild_stop_tx.take() {
            let _ = tx.send(()).await;
        }
        if let Some(handle) = self.rebuild_handle.take() {
            let _ = handle.await;
        }

        // 停止清理器
        self.cleaner.stop().await;

        // 刷写 RocksDB
        self.store.db().flush_wal(true)?;

        Ok(())
    }

    /// 获取底层存储引用（高级用法）
    pub fn store(&self) -> &DedupStore {
        &self.store
    }
}

// 简单的日志输出包装（避免引入 tracing 依赖）
fn tracing_info(msg: &str) {
    let now = chrono::Local::now().format("%Y-%m-%d %H:%M:%S");
    println!("[{}] {}", now, msg);
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn setup_engine() -> (DedupEngine, TempDir) {
        let dir = TempDir::new().unwrap();
        let config = DedupConfig::for_test(dir.path().to_str().unwrap().to_string());
        let engine = DedupEngine::open(config).await.unwrap();
        (engine, dir)
    }

    #[tokio::test]
    async fn test_check_and_set_basic() {
        let (engine, _dir) = setup_engine().await;
        let ns = b"test_ns";
        let key = b"https://example.com/1";
        let expire_at = chrono::Utc::now().timestamp_millis() as u64 + 3600_000;

        // 第一次：新插入
        let r1 = engine.check_and_set(ns, key, expire_at, None).unwrap();
        assert!(r1.is_inserted());

        // 第二次：重复
        let r2 = engine.check_and_set(ns, key, expire_at, None).unwrap();
        assert!(r2.is_duplicate());

        // 不同命名空间：不重复
        let r3 = engine.check_and_set(b"other_ns", key, expire_at, None).unwrap();
        assert!(r3.is_inserted());

        // 不同 key：不重复
        let r4 = engine.check_and_set(ns, b"https://example.com/2", expire_at, None).unwrap();
        assert!(r4.is_inserted());
    }

    #[tokio::test]
    async fn test_exists() {
        let (engine, _dir) = setup_engine().await;
        let ns = b"test";
        let key = b"hello";
        let expire_at = chrono::Utc::now().timestamp_millis() as u64 + 3600_000;

        assert!(!engine.exists(ns, key).unwrap());
        engine.check_and_set(ns, key, expire_at, None).unwrap();
        assert!(engine.exists(ns, key).unwrap());
    }

    #[tokio::test]
    async fn test_expired_and_replaced() {
        let (engine, _dir) = setup_engine().await;
        let ns = b"test";
        let key = b"expiring_key";
        let now = chrono::Utc::now().timestamp_millis() as u64;

        // 写入一个很快过期的 key
        let expire_soon = now + 100; // 100ms 后过期
        let r1 = engine.check_and_set(ns, key, expire_soon, None).unwrap();
        assert!(r1.is_inserted());

        // 立即查：重复
        let r2 = engine.check_and_set(ns, key, expire_soon, None).unwrap();
        assert!(r2.is_duplicate());

        // 等待过期
        tokio::time::sleep(Duration::from_millis(200)).await;

        // 再查：已过期，被替换
        let new_expire = now + 100_000;
        let r3 = engine.check_and_set(ns, key, new_expire, Some(b"new_meta".to_vec())).unwrap();
        match r3 {
            CheckResult::ExpiredAndReplaced { old } => {
                assert_eq!(old.expire_at_ms, expire_soon);
            }
            _ => panic!("期望 ExpiredAndReplaced, 实际: {:?}", r3),
        }
    }

    #[tokio::test]
    async fn test_delete() {
        let (engine, _dir) = setup_engine().await;
        let ns = b"test";
        let key = b"delete_me";
        let expire_at = chrono::Utc::now().timestamp_millis() as u64 + 3600_000;

        engine.check_and_set(ns, key, expire_at, None).unwrap();
        assert!(engine.exists(ns, key).unwrap());

        assert!(engine.delete(ns, key).unwrap());
        assert!(!engine.exists(ns, key).unwrap());

        // 再次删除返回 false
        assert!(!engine.delete(ns, key).unwrap());
    }

    #[tokio::test]
    async fn test_metadata() {
        let (engine, _dir) = setup_engine().await;
        let ns = b"test";
        let key = b"with_meta";
        let expire_at = chrono::Utc::now().timestamp_millis() as u64 + 3600_000;
        let meta = b"{\"depth\":3,\"source\":\"seed\"}".to_vec();

        engine.check_and_set(ns, key, expire_at, Some(meta.clone())).unwrap();

        let got = engine.get(ns, key).unwrap().unwrap();
        assert_eq!(got.metadata, meta);
    }

    #[tokio::test]
    async fn test_batch() {
        let (engine, _dir) = setup_engine().await;
        let ns = b"batch_test";
        let expire_at = chrono::Utc::now().timestamp_millis() as u64 + 3600_000;

        let keys: Vec<Vec<u8>> = (0..10)
            .map(|i| format!("key_{}", i).into_bytes())
            .collect();

        let items: Vec<(&[u8], &[u8], u64, Option<Vec<u8>>)> = keys
            .iter()
            .map(|k| (ns.as_ref(), k.as_slice(), expire_at, None))
            .collect();

        let results = engine.batch_check_and_set(&items).unwrap();
        assert_eq!(results.len(), 10);
        for r in &results {
            assert!(r.is_inserted());
        }

        // 再次批量：全部重复
        let results2 = engine.batch_check_and_set(&items).unwrap();
        for r in &results2 {
            assert!(r.is_duplicate());
        }
    }

    #[tokio::test]
    async fn test_concurrent_access() {
        let (engine, _dir) = setup_engine().await;
        let engine = Arc::new(engine);
        let expire_at = chrono::Utc::now().timestamp_millis() as u64 + 3600_000;

        let mut handles = vec![];
        for t in 0..4 {
            let engine = engine.clone();
            let expire_at = expire_at;
            handles.push(tokio::spawn(async move {
                let ns = format!("thread_{}", t);
                for i in 0..100u64 {
                    let key = format!("key_{}", i);
                    let r = engine.check_and_set(ns.as_bytes(), key.as_bytes(), expire_at, None).unwrap();
                    assert!(r.is_inserted(), "thread {} key {} should be inserted", t, i);
                    // 第二次应该重复
                    let r2 = engine.check_and_set(ns.as_bytes(), key.as_bytes(), expire_at, None).unwrap();
                    assert!(r2.is_duplicate(), "thread {} key {} should be duplicate", t, i);
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        // 验证总数：4 线程 * 100 key = 400
        let count = engine.count_valid_keys().unwrap();
        assert_eq!(count, 400);
    }

    #[tokio::test]
    async fn test_shutdown() {
        let (engine, dir) = setup_engine().await;
        let ns = b"test";
        let key = b"persistent";
        let expire_at = chrono::Utc::now().timestamp_millis() as u64 + 3600_000;

        engine.check_and_set(ns, key, expire_at, None).unwrap();

        // 关闭
        engine.shutdown().await.unwrap();

        // 重新打开，数据应持久化
        let config = DedupConfig::for_test(dir.path().to_str().unwrap().to_string());
        let engine2 = DedupEngine::open(config).await.unwrap();
        assert!(engine2.exists(ns, key).unwrap());
        engine2.shutdown().await.unwrap();
    }
}
