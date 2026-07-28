//! 布隆过滤器加速层
//!
//! 设计要点：
//! - 双缓冲：后台重建完成后原子切换，旧过滤器持续服务
//! - 动态扩容：插入量超过阈值或误判率过高时自动扩容重建
//! - 与 RocksDB 解耦：仅作为前置快速路径，不影响精确性

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;
use bloomfilter::Bloom;
use parking_lot::Mutex;

use crate::config::DedupConfig;
use crate::error::Result;
use crate::key::build_dedup_key;
use crate::storage::DedupStore;

/// 带统计信息的布隆过滤器
///
/// 使用 Mutex 提供内部可变性，因为 Bloom::set 需要 &mut self。
/// 由于布隆过滤器操作是纯内存且极快，Mutex 开销可忽略。
pub struct CountingBloom {
    inner: Mutex<Bloom<[u8]>>,
    /// 已插入数量
    inserted: AtomicU64,
    /// 设计容量
    capacity: u64,
}

impl CountingBloom {
    /// 创建新的布隆过滤器
    pub fn new(capacity: u64, false_positive_rate: f64) -> Self {
        let bloom = Bloom::new_for_fp_rate(capacity as usize, false_positive_rate);
        Self {
            inner: Mutex::new(bloom),
            inserted: AtomicU64::new(0),
            capacity,
        }
    }

    /// 检查 Key 是否可能存在
    #[inline]
    pub fn check(&self, ns_hash: u64, key_hash: u128) -> bool {
        let key = build_dedup_key(ns_hash, key_hash);
        self.inner.lock().check(&key)
    }

    /// 插入 Key（返回 false 表示之前可能不存在，true 表示可能已存在）
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

    /// 仅插入，不检查（重建时使用，已知数据都是新的）
    #[inline]
    pub fn insert_only(&self, ns_hash: u64, key_hash: u128) {
        let key = build_dedup_key(ns_hash, key_hash);
        self.inner.lock().set(&key);
        self.inserted.fetch_add(1, Ordering::Relaxed);
    }

    /// 已插入数量
    #[inline]
    pub fn inserted(&self) -> u64 {
        self.inserted.load(Ordering::Relaxed)
    }

    /// 设计容量
    #[inline]
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// 使用率
    #[inline]
    pub fn usage_ratio(&self) -> f64 {
        if self.capacity == 0 {
            1.0
        } else {
            self.inserted() as f64 / self.capacity as f64
        }
    }
}

/// 布隆过滤器管理器（双缓冲 + 动态扩容）
pub struct BloomManager {
    current: ArcSwap<CountingBloom>,
    config: DedupConfig,
    /// 累计查询次数（用于误判率估算）
    total_checks: AtomicU64,
    /// 累计布隆过滤器"命中"次数（其中一部分是误判）
    bloom_hits: AtomicU64,
    /// 累计实际命中次数（RocksDB确认存在）
    real_hits: AtomicU64,
}

impl BloomManager {
    /// 创建新的布隆过滤器管理器
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

    /// 快速路径检查：返回 true 表示"可能存在"，false 表示"一定不存在"
    #[inline]
    pub fn may_exist(&self, ns_hash: u64, key_hash: u128) -> bool {
        self.total_checks.fetch_add(1, Ordering::Relaxed);
        let exists = self.current.load().check(ns_hash, key_hash);
        if exists {
            self.bloom_hits.fetch_add(1, Ordering::Relaxed);
        }
        exists
    }

    /// 记录一次实际命中（RocksDB 确认存在）
    #[inline]
    pub fn record_real_hit(&self) {
        self.real_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// 插入一个 Key 到当前布隆过滤器
    #[inline]
    pub fn insert(&self, ns_hash: u64, key_hash: u128) {
        self.current.load().set(ns_hash, key_hash);
    }

    /// 获取当前布隆过滤器
    pub fn current(&self) -> arc_swap::Guard<Arc<CountingBloom>> {
        self.current.load()
    }

    /// 估算的误判率（bloom_hits 中不是 real_hits 的比例）
    pub fn estimated_false_positive_rate(&self) -> f64 {
        let total = self.total_checks.load(Ordering::Relaxed);
        let bloom = self.bloom_hits.load(Ordering::Relaxed);
        let real = self.real_hits.load(Ordering::Relaxed);
        if total == 0 || bloom == 0 {
            0.0
        } else {
            // 误判数 = bloom命中 - 真实命中（注意 real_hits 是精确存在的数量
            // 但 real_hits 只统计了布隆过滤器命中后去查DB的真实存在数
            // 更准确的方式：误判率 ≈ (bloom_hits - real_hits) / (total_checks - real_hits)
            let false_positives = bloom.saturating_sub(real) as f64;
            let negatives = total.saturating_sub(real) as f64;
            if negatives == 0.0 {
                0.0
            } else {
                false_positives / negatives
            }
        }
    }

    /// 是否需要扩容重建
    pub fn needs_rebuild(&self) -> bool {
        let current = self.current.load();
        let usage = current.usage_ratio();
        let fpr = self.estimated_false_positive_rate();

        // 三个触发条件（满足任一就扩容）：
        // 1. 使用率超过扩容阈值（默认 80%）
        // 2. 观测误判率超过配置的 1.5 倍
        // 3. 使用率超过 100%（紧急情况，必须立即扩容）
        usage > self.config.bloom_expand_threshold
            || fpr > self.config.bloom_false_positive_rate * 1.5
            || usage >= 1.0
    }

    /// 从 RocksDB 全量重建布隆过滤器（在后台线程调用
    ///
    /// 遍历 cf_dedup 中所有未过期数据，构建新的布隆过滤器，然后原子切换。
    pub fn rebuild_from_store(&self, store: &DedupStore, now_ms: u64) -> Result<u64> {
        // 先统计有效数据量，决定容量
        let count = store.count_valid_keys(now_ms)?;

        // 计算新容量：取「配置容量」与「实际数量 * 扩容系数」的较大值
        let base_capacity = (count as f64 * self.config.bloom_expand_factor) as u64;
        let new_capacity = base_capacity.max(self.config.bloom_capacity);

        let new_bloom = CountingBloom::new(new_capacity, self.config.bloom_false_positive_rate);

        // 填充数据
        let inserted = store.for_each_valid_key(now_ms, |ns, kh| {
            new_bloom.insert_only(ns, kh);
            true
        })?;

        // 原子切换
        self.current.store(Arc::new(new_bloom));

        // 重置统计
        self.total_checks.store(0, Ordering::Relaxed);
        self.bloom_hits.store(0, Ordering::Relaxed);
        self.real_hits.store(0, Ordering::Relaxed);

        Ok(inserted as u64)
    }

    /// 强制扩容重建（容量不足时调用）
    pub fn expand_rebuild(&self, store: &DedupStore, now_ms: u64) -> Result<u64> {
        let current = self.current.load();
        let current_capacity = current.capacity();

        // 统计当前有效数据量
        let count = store.count_valid_keys(now_ms)?;

        // 新容量 = max(当前容量 * 扩容系数, 实际数量 * 扩容系数, 配置容量)
        let new_capacity = (current_capacity as f64 * self.config.bloom_expand_factor) as u64;
        let new_capacity = new_capacity.max((count as f64 * self.config.bloom_expand_factor) as u64);
        let new_capacity = new_capacity.max(self.config.bloom_capacity);

        let new_bloom = CountingBloom::new(new_capacity, self.config.bloom_false_positive_rate);

        let inserted = store.for_each_valid_key(now_ms, |ns, kh| {
            new_bloom.insert_only(ns, kh);
            true
        })?;

        self.current.store(Arc::new(new_bloom));

        self.total_checks.store(0, Ordering::Relaxed);
        self.bloom_hits.store(0, Ordering::Relaxed);
        self.real_hits.store(0, Ordering::Relaxed);

        Ok(inserted as u64)
    }

    /// 获取统计信息
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
}

/// 布隆过滤器统计信息
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::DedupValue;
    use tempfile::TempDir;

    fn setup_store() -> (DedupStore, TempDir) {
        let dir = TempDir::new().unwrap();
        let config = DedupConfig::for_test(dir.path().to_str().unwrap().to_string());
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
        let config = DedupConfig::for_test("unused".to_string());
        let mgr = BloomManager::new(&config);

        let ns = crate::key::hash_namespace(b"test");
        let now = 1_700_000_000_000u64;

        // 写入 100 条数据
        for i in 0..100u128 {
            let kh = crate::key::hash_key(&i.to_be_bytes());
            let v = DedupValue {
                expire_at_ms: now + 86400_000, // 一天后过期
                created_at_ms: now,
                original_key: i.to_be_bytes().to_vec(),
                metadata: vec![],
            };
            store.put(ns, kh, &v).unwrap();
        }

        // 重建前布隆过滤器为空
        assert!(!mgr.may_exist(ns, crate::key::hash_key(&1u128.to_be_bytes())));

        // 重建
        let count = mgr.rebuild_from_store(&store, now).unwrap();
        assert_eq!(count, 100);

        // 重建后应该能检测到
        let kh = crate::key::hash_key(&1u128.to_be_bytes());
        assert!(mgr.may_exist(ns, kh));

        // 不存在的 key 大概率返回 false
        let kh_nonexist = crate::key::hash_key(b"nonexistent_key_12345");
        // 极低概率误判，这里不做严格断言
        let _ = mgr.may_exist(ns, kh_nonexist);
    }

    #[test]
    fn test_bloom_stats() {
        let config = DedupConfig::for_test("unused".to_string());
        let mgr = BloomManager::new(&config);

        let ns = 1u64;
        for i in 0..100u128 {
            mgr.insert(ns, i);
        }

        let stats = mgr.stats();
        assert_eq!(stats.inserted, 100);
        assert!(stats.usage_ratio > 0.0);
    }
}
