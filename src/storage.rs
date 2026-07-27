//! RocksDB 存储层：双 Column Family 设计
//!
//! - `cf_dedup`：精确查重主表
//! - `cf_expire`：过期时间索引表
//!
//! 所有写入通过 WriteBatch 原子提交，保证两表一致性。

use std::path::Path;
use std::sync::Arc;

use rocksdb::{
    BlockBasedOptions, ColumnFamily, ColumnFamilyDescriptor, DBIterator, IteratorMode, Options,
    WriteBatch, DB,
};

use crate::config::DedupConfig;
use crate::error::{DedupError, Result};
use crate::key::{
    build_dedup_key, build_expire_key, expire_key_prefix, parse_expire_key, DedupValue,
    DEDUP_KEY_LEN,
};

/// Column Family 名称
pub const CF_DEDUP: &str = "cf_dedup";
pub const CF_EXPIRE: &str = "cf_expire";

/// RocksDB 存储封装
#[derive(Clone)]
pub struct DedupStore {
    db: Arc<DB>,
}

impl DedupStore {
    /// 打开或创建数据库
    pub fn open(config: &DedupConfig) -> Result<Self> {
        let path = Path::new(&config.db_path);
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }

        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        // 优化写性能：增大 WAL 和 memtable
        db_opts.set_max_total_wal_size(1 << 30); // 1GB
        db_opts.set_write_buffer_size(256 << 20); // 256MB
        db_opts.set_max_write_buffer_number(4);
        db_opts.set_level_zero_file_num_compaction_trigger(8);
        db_opts.set_level_zero_slowdown_writes_trigger(24);
        db_opts.set_level_zero_stop_writes_trigger(36);
        // 开启并行 compaction
        db_opts.set_max_background_jobs(4);
        db_opts.increase_parallelism(4);
        // 优化读性能：布隆过滤器在 cf 级别设置

        let cf_dedup = ColumnFamilyDescriptor::new(CF_DEDUP, Self::dedup_cf_options());
        let cf_expire = ColumnFamilyDescriptor::new(CF_EXPIRE, Self::expire_cf_options());

        let db = DB::open_cf_descriptors(&db_opts, path, vec![cf_dedup, cf_expire])?;

        Ok(Self { db: Arc::new(db) })
    }

    /// cf_dedup 的选项
    fn dedup_cf_options() -> Options {
        let mut opts = Options::default();
        // 前缀布隆过滤器：加速 point lookup
        opts.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(DEDUP_KEY_LEN));
        opts.set_memtable_prefix_bloom_ratio(0.1);
        // LSM 每层也加布隆过滤器（通过 BlockBasedOptions）
        let mut block_opts = BlockBasedOptions::default();
        block_opts.set_bloom_filter(10.0, false);
        opts.set_block_based_table_factory(&block_opts);
        opts.set_level_compaction_dynamic_level_bytes(true);
        opts
    }

    /// cf_expire 的选项
    fn expire_cf_options() -> Options {
        let mut opts = Options::default();
        // 过期索引主要是范围扫描，但保留 point lookup 能力（删除时用）
        let mut block_opts = BlockBasedOptions::default();
        block_opts.set_bloom_filter(10.0, false);
        opts.set_block_based_table_factory(&block_opts);
        opts.set_level_compaction_dynamic_level_bytes(true);
        opts
    }

    /// 获取 cf_dedup 句柄
    #[inline]
    pub fn cf_dedup(&self) -> &ColumnFamily {
        self.db.cf_handle(CF_DEDUP).expect("cf_dedup must exist")
    }

    /// 获取 cf_expire 句柄
    #[inline]
    pub fn cf_expire(&self) -> &ColumnFamily {
        self.db.cf_handle(CF_EXPIRE).expect("cf_expire must exist")
    }

    /// 底层 DB 引用（供高级用法）
    #[inline]
    pub fn db(&self) -> &DB {
        &self.db
    }

    // -----------------------------------------------------------------------
    // 基础读写
    // -----------------------------------------------------------------------

    /// 查询 cf_dedup 中的值
    pub fn get_dedup(&self, ns_hash: u64, key_hash: u128) -> Result<Option<DedupValue>> {
        let key = build_dedup_key(ns_hash, key_hash);
        let cf = self.cf_dedup();
        match self.db.get_cf(cf, &key)? {
            Some(bytes) => Ok(Some(DedupValue::decode(&bytes)?)),
            None => Ok(None),
        }
    }

    /// 原子写入一条记录（cf_dedup + cf_expire）
    pub fn put(
        &self,
        ns_hash: u64,
        key_hash: u128,
        value: &DedupValue,
    ) -> Result<()> {
        let mut batch = WriteBatch::default();
        self.add_put_to_batch(&mut batch, ns_hash, key_hash, value);
        self.db.write(batch)?;
        Ok(())
    }

    /// 将 put 操作添加到现有 WriteBatch
    pub fn add_put_to_batch(
        &self,
        batch: &mut WriteBatch,
        ns_hash: u64,
        key_hash: u128,
        value: &DedupValue,
    ) {
        let dedup_key = build_dedup_key(ns_hash, key_hash);
        let expire_key = build_expire_key(value.expire_at_ms, ns_hash, key_hash);
        let encoded = value.encode().expect("value encode should not fail");
        batch.put_cf(self.cf_dedup(), &dedup_key, &encoded);
        batch.put_cf(self.cf_expire(), &expire_key, b"");
    }

    /// 原子删除一条记录（cf_dedup + cf_expire）
    pub fn delete(
        &self,
        ns_hash: u64,
        key_hash: u128,
        expire_at_ms: u64,
    ) -> Result<()> {
        let mut batch = WriteBatch::default();
        self.add_delete_to_batch(&mut batch, ns_hash, key_hash, expire_at_ms);
        self.db.write(batch)?;
        Ok(())
    }

    /// 将 delete 操作添加到现有 WriteBatch
    pub fn add_delete_to_batch(
        &self,
        batch: &mut WriteBatch,
        ns_hash: u64,
        key_hash: u128,
        expire_at_ms: u64,
    ) {
        let dedup_key = build_dedup_key(ns_hash, key_hash);
        let expire_key = build_expire_key(expire_at_ms, ns_hash, key_hash);
        batch.delete_cf(self.cf_dedup(), &dedup_key);
        batch.delete_cf(self.cf_expire(), &expire_key);
    }

    /// 批量写入
    pub fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        self.db.write(batch)?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // 过期扫描
    // -----------------------------------------------------------------------

    /// 扫描 cf_expire 中过期时间 <= deadline_ms 的所有 Key
    ///
    /// 返回迭代器，调用者负责消费并删除。
    /// 迭代器产出 `(expire_at_ms, ns_hash, key_hash)` 元组。
    pub fn scan_expired(&self, deadline_ms: u64) -> ExpireIterator<'_> {
        let cf = self.cf_expire();
        let prefix = expire_key_prefix(0); // 从 0 开始
        let iter = self.db.iterator_cf(cf, IteratorMode::From(&prefix, rocksdb::Direction::Forward));
        ExpireIterator {
            inner: iter,
            deadline_ms,
            done: false,
        }
    }

    /// 批量删除过期记录：从 cf_expire 扫描一批，同时从 cf_dedup 删除
    ///
    /// 返回实际删除的数量
    pub fn delete_expired_batch(&self, deadline_ms: u64, limit: usize) -> Result<usize> {
        let mut count = 0;
        let mut batch = WriteBatch::default();

        for item in self.scan_expired(deadline_ms) {
            let (expire_at_ms, ns_hash, key_hash) = item?;
            self.add_delete_to_batch(&mut batch, ns_hash, key_hash, expire_at_ms);
            count += 1;
            if count >= limit {
                break;
            }
        }

        if count > 0 {
            self.db.write(batch)?;
        }
        Ok(count)
    }

    // -----------------------------------------------------------------------
    // 全量遍历（供布隆过滤器重建使用）
    // -----------------------------------------------------------------------

    /// 遍历 cf_dedup 中所有未过期的 Key（用于布隆过滤器重建）
    ///
    /// 闭包返回 false 时提前终止遍历。
    pub fn for_each_valid_key<F>(&self, now_ms: u64, mut f: F) -> Result<usize>
    where
        F: FnMut(u64, u128) -> bool,
    {
        let cf = self.cf_dedup();
        let iter = self.db.iterator_cf(cf, IteratorMode::Start);
        let mut count = 0;

        for item in iter {
            let (key_bytes, value_bytes) = item?;
            let (ns_hash, key_hash) = crate::key::parse_dedup_key(&key_bytes)?;
            let value = DedupValue::decode(&value_bytes)?;
            if !value.is_expired(now_ms) {
                count += 1;
                if !f(ns_hash, key_hash) {
                    break;
                }
            }
        }
        Ok(count)
    }

    /// 统计 cf_dedup 中未过期的 Key 数量（近似，用于监控）
    pub fn count_valid_keys(&self, now_ms: u64) -> Result<usize> {
        self.for_each_valid_key(now_ms, |_, _| true)
    }

    /// 统计 cf_dedup 中所有 Key 数量（包括过期的，用于测试）
    pub fn count_all_keys(&self) -> Result<usize> {
        let cf = self.cf_dedup();
        let iter = self.db.iterator_cf(cf, IteratorMode::Start);
        let mut count = 0;
        for item in iter {
            item?;
            count += 1;
        }
        Ok(count)
    }
}

/// cf_expire 迭代器封装
pub struct ExpireIterator<'a> {
    inner: DBIterator<'a>,
    deadline_ms: u64,
    done: bool,
}

impl<'a> Iterator for ExpireIterator<'a> {
    type Item = Result<(u64, u64, u128)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        match self.inner.next() {
            Some(Ok((key_bytes, _value))) => {
                match parse_expire_key(&key_bytes) {
                    Ok((ts, ns, kh)) => {
                        if ts <= self.deadline_ms {
                            Some(Ok((ts, ns, kh)))
                        } else {
                            // 已超过 deadline，后续都更大，结束
                            self.done = true;
                            None
                        }
                    }
                    Err(e) => {
                        self.done = true;
                        Some(Err(e))
                    }
                }
            }
            Some(Err(e)) => {
                self.done = true;
                Some(Err(DedupError::RocksDb(e)))
            }
            None => {
                self.done = true;
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup() -> (DedupStore, TempDir) {
        let dir = TempDir::new().unwrap();
        let config = DedupConfig::for_test(dir.path().to_str().unwrap().to_string());
        let store = DedupStore::open(&config).unwrap();
        (store, dir)
    }

    #[test]
    fn test_put_and_get() {
        let (store, _dir) = setup();
        let ns = crate::key::hash_namespace(b"test");
        let kh = crate::key::hash_key(b"hello");

        let v = DedupValue {
            expire_at_ms: 1_700_000_000_000,
            created_at_ms: 1_699_000_000_000,
            original_key: b"hello".to_vec(),
            metadata: b"meta".to_vec(),
        };

        assert!(store.get_dedup(ns, kh).unwrap().is_none());
        store.put(ns, kh, &v).unwrap();
        let got = store.get_dedup(ns, kh).unwrap().unwrap();
        assert_eq!(got, v);
    }

    #[test]
    fn test_delete() {
        let (store, _dir) = setup();
        let ns = crate::key::hash_namespace(b"test");
        let kh = crate::key::hash_key(b"hello");

        let v = DedupValue {
            expire_at_ms: 1000,
            created_at_ms: 500,
            original_key: b"hello".to_vec(),
            metadata: vec![],
        };
        store.put(ns, kh, &v).unwrap();
        assert!(store.get_dedup(ns, kh).unwrap().is_some());

        store.delete(ns, kh, 1000).unwrap();
        assert!(store.get_dedup(ns, kh).unwrap().is_none());
    }

    #[test]
    fn test_scan_expired() {
        let (store, _dir) = setup();
        let ns = crate::key::hash_namespace(b"test");

        for i in 0..10u128 {
            let kh = crate::key::hash_key(&i.to_be_bytes());
            let v = DedupValue {
                expire_at_ms: i as u64 * 1000,
                created_at_ms: 0,
                original_key: i.to_be_bytes().to_vec(),
                metadata: vec![],
            };
            store.put(ns, kh, &v).unwrap();
        }

        // 扫描 <= 5000ms 的记录（0,1,2,3,4,5 共 6 条）
        let items: Vec<_> = store.scan_expired(5000).collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(items.len(), 6);
        for (ts, _, _) in &items {
            assert!(*ts <= 5000);
        }
    }

    #[test]
    fn test_delete_expired_batch() {
        let (store, _dir) = setup();
        let ns = crate::key::hash_namespace(b"test");

        for i in 0..10u128 {
            let kh = crate::key::hash_key(&i.to_be_bytes());
            let v = DedupValue {
                expire_at_ms: i as u64 * 1000,
                created_at_ms: 0,
                original_key: i.to_be_bytes().to_vec(),
                metadata: vec![],
            };
            store.put(ns, kh, &v).unwrap();
        }

        let deleted = store.delete_expired_batch(3000, 100).unwrap();
        assert_eq!(deleted, 4); // 0,1000,2000,3000

        // 剩余应该是 6 条
        let remaining = store.count_all_keys().unwrap();
        assert_eq!(remaining, 6);
    }
}
