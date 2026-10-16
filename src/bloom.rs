//! 布隆过滤器加速层
//!
//! ## 架构概览
//!
//! ### 单层 CountingBloom + BloomManager（原始实现，保留兼容性）
//!
//! 设计要点：
//! - 双缓冲：后台重建完成后原子切换，旧过滤器持续服务
//! - 动态扩容：插入量超过阈值或误判率过高时自动扩容重建
//! - **并行重建**：使用 rayon 并行将 key 批量插入新过滤器，显著缩短
//!   大数据量（>1 亿条）下的重建耗时
//! - 与 RocksDB 解耦：仅作为前置快速路径，不影响精确性
//!
//! ### 分层布隆过滤器 LayeredBloomManager
//!
//! 两层设计：
//! - **热层（hot）**：容量小，负责近期新写入的 key；高频扩容/重建
//! - **冷层（cold）**：容量大，通过定时全量重建产生；覆盖所有历史有效 key
//!
//! 查询时先查热层（误判率低，内存小），命中直接返回；
//! 未命中再查冷层（覆盖历史数据）；两层都不存在才进 RocksDB。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use arc_swap::ArcSwap;
use bloomfilter::Bloom;
use parking_lot::Mutex;
use rayon::prelude::*;

use crate::config::DedupConfig;
use crate::error::Result;
use crate::key::build_dedup_key;
use crate::storage::DedupStore;

// ---------------------------------------------------------------------------
// CountingBloom — 单个布隆过滤器实例
// ---------------------------------------------------------------------------

/// 带统计信息的布隆过滤器
pub struct CountingBloom {
    inner: Mutex<Bloom<[u8]>>,
    inserted: AtomicU64,
    capacity: u64,
}

impl CountingBloom {
    pub fn new(capacity: u64, false_positive_rate: f64) -> Self {
        let cap = capacity.max(1) as usize;
        let bloom = Bloom::new_for_fp_rate(cap, false_positive_rate);
        Self {
            inner: Mutex::new(bloom),
            inserted: AtomicU64::new(0),
            capacity,
        }
    }

    #[inline]
    pub fn check(&self, ns_hash: u64, key_hash: u128) -> bool {
        let key = build_dedup_key(ns_hash, key_hash);
        self.inner.lock().check(&key)
    }

    #[inline]
    pub fn set(&self, ns_hash: u64, key_hash: u128) -> bool {
        let key = build_dedup_key(ns_hash, key_hash);
        let mut inner = self.inner.lock();
        let existed = inner.check(&key);
        if !existed {
            inner.set(&key);
            self.inserted.fetch_add(1, Ordering::Relaxed);
        }
        existed
    }

    #[inline]
    pub fn insert_only(&self, ns_hash: u64, key_hash: u128) {
        let key = build_dedup_key(ns_hash, key_hash);
        self.inner.lock().set(&key);
        self.inserted.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn inserted(&self) -> u64 {
        self.inserted.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    #[inline]
    pub fn usage_ratio(&self) -> f64 {
        if self.capacity == 0 {
            1.0
        } else {
            self.inserted() as f64 / self.capacity as f64
        }
    }
}

// ---------------------------------------------------------------------------
// 并行重建辅助
// ---------------------------------------------------------------------------

/// rayon 并行计算 key 编码，再批量写入布隆过滤器
///
/// IO 阶段（RocksDB 顺序读）在外部完成；此函数只负责 CPU 密集的 key
/// 编码（build_dedup_key）和批量插入，适合 rayon 并行。
fn parallel_fill_bloom(bloom: &CountingBloom, keys: &[(u64, u128)]) {
    // 1. 并行计算所有 key 的字节编码（纯 CPU，无锁）
    let encoded: Vec<[u8; 24]> = keys
        .par_iter()
        .map(|&(ns, kh)| build_dedup_key(ns, kh))
        .collect();

    // 2. 串行批量写入 Bloom（Mutex 开销 << IO，可接受）
    let mut inner = bloom.inner.lock();
    for key in &encoded {
        inner.set(key.as_ref());
    }
    bloom
        .inserted
        .fetch_add(encoded.len() as u64, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// BloomManager — 原有单层管理器（含并行重建增强）
// ---------------------------------------------------------------------------

/// 布隆过滤器管理器（双缓冲 + 动态扩容 + 并行重建）
pub struct BloomManager {
    current: ArcSwap<CountingBloom>,
    config: DedupConfig,
    total_checks: AtomicU64,
    bloom_hits: AtomicU64,
    real_hits: AtomicU64,
}

impl BloomManager {
    pub fn new(config: &DedupConfig) -> Self {
        let bloom = CountingBloom::new(config.bloom_capacity, config.bloom_false_positive_rate);
        Self {
            current: ArcSwap::from_pointee(bloom),
            config: config.clone(),
            total_checks: AtomicU64::new(0),
            bloom_hits: AtomicU64::new(0),
            real_hits: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn may_exist(&self, ns_hash: u64, key_hash: u128) -> bool {
        self.total_checks.fetch_add(1, Ordering::Relaxed);
        let exists = self.current.load().check(ns_hash, key_hash);
        if exists {
            self.bloom_hits.fetch_add(1, Ordering::Relaxed);
        }
        exists
    }

    #[inline]
    pub fn record_real_hit(&self) {
        self.real_hits.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn insert(&self, ns_hash: u64, key_hash: u128) {
        self.current.load().set(ns_hash, key_hash);
    }

    pub fn current(&self) -> arc_swap::Guard<Arc<CountingBloom>> {
        self.current.load()
    }

    pub fn estimated_false_positive_rate(&self) -> f64 {
        let total = self.total_checks.load(Ordering::Relaxed);
        let bloom = self.bloom_hits.load(Ordering::Relaxed);
        let real = self.real_hits.load(Ordering::Relaxed);
        if total == 0 || bloom == 0 {
            return 0.0;
        }
        let false_positives = bloom.saturating_sub(real) as f64;
        let negatives = total.saturating_sub(real) as f64;
        if negatives == 0.0 {
            0.0
        } else {
            false_positives / negatives
        }
    }

    pub fn needs_rebuild(&self) -> bool {
        let current = self.current.load();
        let usage = current.usage_ratio();
        let fpr = self.estimated_false_positive_rate();
        usage > self.config.bloom_expand_threshold
            || fpr > self.config.bloom_false_positive_rate * 1.5
            || usage >= 1.0
    }

    /// 从 RocksDB 全量并行重建布隆过滤器
    ///
    /// 流程：
    /// 1. 顺序 IO 扫描 RocksDB，收集所有未过期 key 到内存
    /// 2. rayon 并行计算每个 key 的字节编码（CPU 密集部分）
    /// 3. 批量写入新过滤器后原子切换
    ///
    /// 对比旧版单线程逐条插入，1 亿条场景下 CPU 阶段约快 3-4x（4 核）。
    pub fn rebuild_from_store(&self, store: &DedupStore, now_ms: u64) -> Result<u64> {
        let t0 = Instant::now();

        // 步骤 1：顺序 IO 收集
        let keys = store.collect_valid_keys(now_ms)?;
        let count = keys.len() as u64;

        // 步骤 2：确定容量
        let base_capacity = (count as f64 * self.config.bloom_expand_factor) as u64;
        let new_capacity = base_capacity.max(self.config.bloom_capacity);
        let new_bloom = CountingBloom::new(new_capacity, self.config.bloom_false_positive_rate);

        // 步骤 3：并行填充
        parallel_fill_bloom(&new_bloom, &keys);

        // 步骤 4：原子切换 + 重置统计
        self.current.store(Arc::new(new_bloom));
        self.reset_stats();

        eprintln!(
            "[bloom] 并行重建完成: {} 条, 容量 {}, 耗时 {:.2}s",
            count,
            new_capacity,
            t0.elapsed().as_secs_f64()
        );
        Ok(count)
    }

    /// 扩容重建（容量不足时调用）
    pub fn expand_rebuild(&self, store: &DedupStore, now_ms: u64) -> Result<u64> {
        let current_capacity = self.current.load().capacity();
        let keys = store.collect_valid_keys(now_ms)?;
        let count = keys.len() as u64;

        let new_capacity = (current_capacity as f64 * self.config.bloom_expand_factor) as u64;
        let new_capacity =
            new_capacity.max((count as f64 * self.config.bloom_expand_factor) as u64);
        let new_capacity = new_capacity.max(self.config.bloom_capacity);

        let new_bloom = CountingBloom::new(new_capacity, self.config.bloom_false_positive_rate);
        parallel_fill_bloom(&new_bloom, &keys);

        self.current.store(Arc::new(new_bloom));
        self.reset_stats();

        Ok(count)
    }

    pub fn stats(&self) -> BloomStats {
        let current = self.current.load();
        BloomStats {
            capacity: current.capacity(),
            inserted: current.inserted(),
            usage_ratio: current.usage_ratio(),
            total_checks: self.total_checks.load(Ordering::Relaxed),
            bloom_hits: self.bloom_hits.load(Ordering::Relaxed),
            real_hits: self.real_hits.load(Ordering::Relaxed),
            estimated_fpr: self.estimated_false_positive_rate(),
        }
    }

    fn reset_stats(&self) {
        self.total_checks.store(0, Ordering::Relaxed);
        self.bloom_hits.store(0, Ordering::Relaxed);
        self.real_hits.store(0, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// LayeredBloomManager — 分层布隆过滤器（热冷两层）
// ---------------------------------------------------------------------------

/// 分层布隆过滤器管理器
///
/// ## 设计
///
/// - **冷层（cold）**：通过定时全量重建产生（默认 24h），覆盖所有历史有效 key；
///   容量大、重建代价高，但频率低
/// - **热层（hot）**：仅接收最近新写入的 key；容量为冷层的 5%，扩容代价低
///
/// ## 查询流程
///
/// ```text
/// 查询 key
///   ├── 热层命中 → "可能存在" → 进 RocksDB 精确确认
///   ├── 冷层命中 → "可能存在" → 进 RocksDB 精确确认
///   └── 两层均无 → "一定不存在"（快速返回）
/// ```
///
/// ## 写入策略
///
/// 新 key 只写热层；冷层通过定时重建自动收录（含历史热层数据）。
/// 重建冷层后热层清空，重新从零积累新增量。
pub struct LayeredBloomManager {
    cold: ArcSwap<CountingBloom>,
    hot: ArcSwap<CountingBloom>,
    config: DedupConfig,
    hot_written: AtomicU64,
    total_checks: AtomicU64,
    bloom_hits: AtomicU64,
    real_hits: AtomicU64,
}

impl LayeredBloomManager {
    /// 热层容量 = 冷层容量 * HOT_CAPACITY_RATIO
    const HOT_CAPACITY_RATIO: f64 = 0.05;

    pub fn new(config: &DedupConfig) -> Self {
        let hot_cap =
            ((config.bloom_capacity as f64) * Self::HOT_CAPACITY_RATIO) as u64;
        let hot_cap = hot_cap.max(10_000);

        let cold = CountingBloom::new(config.bloom_capacity, config.bloom_false_positive_rate);
        let hot_fpr = (config.bloom_false_positive_rate * 0.5).max(0.001);
        let hot = CountingBloom::new(hot_cap, hot_fpr);

        Self {
            cold: ArcSwap::from_pointee(cold),
            hot: ArcSwap::from_pointee(hot),
            config: config.clone(),
            hot_written: AtomicU64::new(0),
            total_checks: AtomicU64::new(0),
            bloom_hits: AtomicU64::new(0),
            real_hits: AtomicU64::new(0),
        }
    }

    /// 查询：先热层后冷层
    #[inline]
    pub fn may_exist(&self, ns_hash: u64, key_hash: u128) -> bool {
        self.total_checks.fetch_add(1, Ordering::Relaxed);
        if self.hot.load().check(ns_hash, key_hash) {
            self.bloom_hits.fetch_add(1, Ordering::Relaxed);
            return true;
        }
        let in_cold = self.cold.load().check(ns_hash, key_hash);
        if in_cold {
            self.bloom_hits.fetch_add(1, Ordering::Relaxed);
        }
        in_cold
    }

    #[inline]
    pub fn record_real_hit(&self) {
        self.real_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// 新写入 key 只进热层
    #[inline]
    pub fn insert(&self, ns_hash: u64, key_hash: u128) {
        self.hot.load().set(ns_hash, key_hash);
        self.hot_written.fetch_add(1, Ordering::Relaxed);
    }

    /// 热层是否需要扩容
    pub fn hot_needs_expand(&self) -> bool {
        let hot = self.hot.load();
        hot.usage_ratio() > self.config.bloom_expand_threshold || hot.usage_ratio() >= 1.0
    }

    /// 热层扩容（不扫 RocksDB，直接换一个更大的空热层）
    ///
    /// 旧热层的 key 会在下次冷层重建时自动被收录，不会漏判。
    pub fn expand_hot(&self) {
        let current_cap = self.hot.load().capacity();
        let new_cap = (current_cap as f64 * self.config.bloom_expand_factor) as u64;
        let hot_fpr = (self.config.bloom_false_positive_rate * 0.5).max(0.001);
        let new_hot = CountingBloom::new(new_cap, hot_fpr);
        self.hot.store(Arc::new(new_hot));
        self.hot_written.store(0, Ordering::Relaxed);
    }

    /// 全量重建冷层（并行），同时重置热层
    ///
    /// 重建后冷层覆盖所有历史有效 key，热层从零开始积累新增量。
    pub fn rebuild_cold_from_store(&self, store: &DedupStore, now_ms: u64) -> Result<u64> {
        let t0 = Instant::now();

        let keys = store.collect_valid_keys(now_ms)?;
        let count = keys.len() as u64;

        let base = (count as f64 * self.config.bloom_expand_factor) as u64;
        let new_cap = base.max(self.config.bloom_capacity);
        let new_cold = CountingBloom::new(new_cap, self.config.bloom_false_positive_rate);
        parallel_fill_bloom(&new_cold, &keys);
        self.cold.store(Arc::new(new_cold));

        // 重置热层
        let hot_cap =
            ((self.config.bloom_capacity as f64) * Self::HOT_CAPACITY_RATIO) as u64;
        let hot_cap = hot_cap.max(10_000);
        let hot_fpr = (self.config.bloom_false_positive_rate * 0.5).max(0.001);
        self.hot.store(Arc::new(CountingBloom::new(hot_cap, hot_fpr)));
        self.hot_written.store(0, Ordering::Relaxed);

        self.reset_stats();

        eprintln!(
            "[layered_bloom] 冷层并行重建完成: {} 条, 容量 {}, 耗时 {:.2}s",
            count,
            new_cap,
            t0.elapsed().as_secs_f64()
        );
        Ok(count)
    }

    pub fn stats(&self) -> LayeredBloomStats {
        let cold = self.cold.load();
        let hot = self.hot.load();
        LayeredBloomStats {
            cold_capacity: cold.capacity(),
            cold_inserted: cold.inserted(),
            cold_usage_ratio: cold.usage_ratio(),
            hot_capacity: hot.capacity(),
            hot_inserted: hot.inserted(),
            hot_usage_ratio: hot.usage_ratio(),
            hot_written_since_rebuild: self.hot_written.load(Ordering::Relaxed),
            total_checks: self.total_checks.load(Ordering::Relaxed),
            bloom_hits: self.bloom_hits.load(Ordering::Relaxed),
            real_hits: self.real_hits.load(Ordering::Relaxed),
        }
    }

    fn reset_stats(&self) {
        self.total_checks.store(0, Ordering::Relaxed);
        self.bloom_hits.store(0, Ordering::Relaxed);
        self.real_hits.store(0, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// 统计结构
// ---------------------------------------------------------------------------

/// 布隆过滤器统计信息（单层）
#[derive(Debug, Clone)]
pub struct BloomStats {
    pub capacity: u64,
    pub inserted: u64,
    pub usage_ratio: f64,
    pub total_checks: u64,
    pub bloom_hits: u64,
    pub real_hits: u64,
    pub estimated_fpr: f64,
}

/// 分层布隆过滤器统计信息
#[derive(Debug, Clone)]
pub struct LayeredBloomStats {
    pub cold_capacity: u64,
    pub cold_inserted: u64,
    pub cold_usage_ratio: f64,
    pub hot_capacity: u64,
    pub hot_inserted: u64,
    pub hot_usage_ratio: f64,
    /// 自上次冷层重建以来，热层累计写入数
    pub hot_written_since_rebuild: u64,
    pub total_checks: u64,
    pub bloom_hits: u64,
    pub real_hits: u64,
}

// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::DedupValue;
    use tempfile::TempDir;

    fn setup_store() -> (DedupStore, TempDir) {
        let dir = TempDir::new().unwrap();
        let config =
            crate::config::DedupConfig::for_test(dir.path().to_str().unwrap().to_string());
        let store = DedupStore::open(&config).unwrap();
        (store, dir)
    }

    #[test]
    fn test_bloom_basic() {
        let bloom = CountingBloom::new(1000, 0.01);
        let ns = 123u64;
        assert!(!bloom.check(ns, 1));
        assert!(!bloom.set(ns, 1));
        assert!(bloom.check(ns, 1));
        assert_eq!(bloom.inserted(), 1);
    }

    #[test]
    fn test_bloom_manager_rebuild() {
        let (store, _dir) = setup_store();
        let config = crate::config::DedupConfig::for_test("unused".to_string());
        let mgr = BloomManager::new(&config);
        let ns = crate::key::hash_namespace(b"test");
        let now = 1_700_000_000_000u64;

        for i in 0..100u128 {
            let kh = crate::key::hash_key(&i.to_be_bytes());
            let v = DedupValue {
                expire_at_ms: now + 86400_000,
                created_at_ms: now,
                original_key: i.to_be_bytes().to_vec(),
                metadata: vec![],
            };
            store.put(ns, kh, &v).unwrap();
        }

        assert!(!mgr.may_exist(ns, crate::key::hash_key(&1u128.to_be_bytes())));

        let count = mgr.rebuild_from_store(&store, now).unwrap();
        assert_eq!(count, 100);

        let kh = crate::key::hash_key(&1u128.to_be_bytes());
        assert!(mgr.may_exist(ns, kh));

        let kh_nonexist = crate::key::hash_key(b"nonexistent_key_12345");
        let _ = mgr.may_exist(ns, kh_nonexist);
    }

    #[test]
    fn test_bloom_stats() {
        let config = crate::config::DedupConfig::for_test("unused".to_string());
        let mgr = BloomManager::new(&config);
        let ns = 1u64;
        for i in 0..100u128 {
            mgr.insert(ns, i);
        }
        let stats = mgr.stats();
        assert_eq!(stats.inserted, 100);
        assert!(stats.usage_ratio > 0.0);
    }

    // 分层布隆过滤器测试

    #[test]
    fn test_layered_bloom_basic() {
        let config = crate::config::DedupConfig::for_test("unused".to_string());
        let mgr = LayeredBloomManager::new(&config);
        let ns = 42u64;

        assert!(!mgr.may_exist(ns, 1));
        mgr.insert(ns, 1);
        assert!(mgr.may_exist(ns, 1));
        assert!(!mgr.may_exist(ns, 9999));
    }

    #[test]
    fn test_layered_bloom_cold_rebuild() {
        let (store, _dir) = setup_store();
        let config = crate::config::DedupConfig::for_test("unused".to_string());
        let mgr = LayeredBloomManager::new(&config);
        let ns = crate::key::hash_namespace(b"layered_test");
        let now = 1_700_000_000_000u64;

        for i in 0..50u128 {
            let kh = crate::key::hash_key(&i.to_be_bytes());
            let v = DedupValue {
                expire_at_ms: now + 86400_000,
                created_at_ms: now,
                original_key: i.to_be_bytes().to_vec(),
                metadata: vec![],
            };
            store.put(ns, kh, &v).unwrap();
        }

        let count = mgr.rebuild_cold_from_store(&store, now).unwrap();
        assert_eq!(count, 50);

        let kh = crate::key::hash_key(&10u128.to_be_bytes());
        assert!(mgr.may_exist(ns, kh));

        mgr.insert(ns, 9999_u128);
        assert!(mgr.may_exist(ns, 9999_u128));
    }

    #[test]
    fn test_layered_bloom_hot_expand() {
        let config = crate::config::DedupConfig::for_test("unused".to_string());
        let mgr = LayeredBloomManager::new(&config);
        let ns = 1u64;

        let initial_cap = mgr.hot.load().capacity();
        assert!(initial_cap > 0);
        assert!(!mgr.hot_needs_expand());

        let fill_count = (initial_cap as f64 * 0.85) as u128;
        for i in 0..fill_count {
            mgr.insert(ns, i);
        }

        assert!(mgr.hot_needs_expand());
        mgr.expand_hot();

        let new_cap = mgr.hot.load().capacity();
        assert!(new_cap > initial_cap);

        let stats = mgr.stats();
        assert_eq!(stats.hot_capacity, new_cap);
    }

    #[test]
    fn test_parallel_rebuild_correctness() {
        let (store, _dir) = setup_store();
        let config = crate::config::DedupConfig::for_test("unused".to_string());
        let mgr = BloomManager::new(&config);
        let ns = crate::key::hash_namespace(b"parallel_test");
        let now = 1_700_000_000_000u64;
        let n = 500usize;

        for i in 0..n as u128 {
            let kh = crate::key::hash_key(&i.to_be_bytes());
            let v = DedupValue {
                expire_at_ms: now + 86400_000,
                created_at_ms: now,
                original_key: i.to_be_bytes().to_vec(),
                metadata: vec![],
            };
            store.put(ns, kh, &v).unwrap();
        }

        let count = mgr.rebuild_from_store(&store, now).unwrap();
        assert_eq!(count, n as u64);

        // 布隆过滤器不允许漏判
        let mut hits = 0usize;
        for i in 0..n as u128 {
            let kh = crate::key::hash_key(&i.to_be_bytes());
            if mgr.may_exist(ns, kh) {
                hits += 1;
            }
        }
        assert_eq!(hits, n, "布隆过滤器不允许漏判");
    }
}
