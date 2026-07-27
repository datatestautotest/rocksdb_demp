//! 基准测试

use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use rocksdb_dedup::{DedupConfig, DedupEngine};
use tempfile::TempDir;

fn bench_check_and_set(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let dir = TempDir::new().unwrap();
    let config = DedupConfig::for_test(dir.path().to_str().unwrap().to_string());
    let engine = rt.block_on(DedupEngine::open(config)).unwrap();

    let ns = b"bench";
    let expire_at = chrono::Utc::now().timestamp_millis() as u64 + 86400_000;

    let mut group = c.benchmark_group("dedup");
    group.throughput(Throughput::Elements(1));
    group.sample_size(100);
    group.measurement_time(Duration::from_secs(10));

    // 新 Key 写入基准
    let mut counter = 0u64;
    group.bench_function("check_and_set_new", |b| {
        b.iter(|| {
            counter += 1;
            let key = format!("new_key_{}", counter);
            let result = engine.check_and_set(ns, key.as_bytes(), expire_at, None).unwrap();
            assert!(result.is_inserted());
        })
    });

    // 重复 Key 查询基准（布隆过滤器命中）
    // 先写入一批 key
    for i in 0..10_000 {
        let key = format!("dup_key_{}", i);
        engine.check_and_set(ns, key.as_bytes(), expire_at, None).unwrap();
    }

    let mut dup_counter = 0u64;
    group.bench_function("check_and_set_duplicate", |b| {
        b.iter(|| {
            dup_counter = (dup_counter + 1) % 10_000;
            let key = format!("dup_key_{}", dup_counter);
            let result = engine.check_and_set(ns, key.as_bytes(), expire_at, None).unwrap();
            assert!(result.is_duplicate());
        })
    });

    // 纯 exists 查询
    let mut exists_counter = 0u64;
    group.bench_function("exists_hit", |b| {
        b.iter(|| {
            exists_counter = (exists_counter + 1) % 10_000;
            let key = format!("dup_key_{}", exists_counter);
            assert!(engine.exists(ns, key.as_bytes()).unwrap());
        })
    });

    // 不存在的 Key 查询（布隆过滤器快速路径）
    let mut miss_counter = 100_000u64;
    group.bench_function("exists_miss_bloom", |b| {
        b.iter(|| {
            miss_counter += 1;
            let key = format!("nonexist_key_{}", miss_counter);
            assert!(!engine.exists(ns, key.as_bytes()).unwrap());
        })
    });

    group.finish();

    rt.block_on(engine.shutdown()).unwrap();
}

criterion_group!(benches, bench_check_and_set);
criterion_main!(benches);
