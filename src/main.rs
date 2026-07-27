//! 去重引擎示例程序

use rocksdb_dedup::{DedupConfig, DedupEngine};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("=== RocksDB 去重引擎示例 ===");

    let config = DedupConfig {
        db_path: "./data/dedup_demo".to_string(),
        bloom_capacity: 1_000_000,
        bloom_false_positive_rate: 0.01,
        bloom_rebuild_interval_secs: 3600,
        bloom_expand_threshold: 0.8,
        bloom_expand_factor: 1.5,
        cleaner_interval_secs: 60,
        cleaner_batch_size: 1000,
        cleaner_batch_sleep_ms: 5,
        max_metadata_size: 1024,
    };

    println!("打开数据库: {}", config.db_path);
    let engine = DedupEngine::open(config).await?;

    let ns = b"crawler_news";
    let expire_at = chrono::Utc::now().timestamp_millis() as u64 + 30 * 86400_000; // 30天后过期

    println!("\n--- 写入 1000 个 URL ---");
    let start = std::time::Instant::now();
    for i in 0..1000 {
        let url = format!("https://example.com/news/{}", i);
        let result = engine.check_and_set(ns, url.as_bytes(), expire_at, None)?;
        if !result.is_inserted() {
            println!("  意外重复: {}", url);
        }
    }
    let elapsed = start.elapsed();
    println!("耗时: {:?}, QPS: {:.0}", elapsed, 1000.0 / elapsed.as_secs_f64());

    println!("\n--- 重复查询 1000 个 URL ---");
    let start = std::time::Instant::now();
    let mut dup_count = 0;
    for i in 0..1000 {
        let url = format!("https://example.com/news/{}", i);
        if engine.exists(ns, url.as_bytes())? {
            dup_count += 1;
        }
    }
    let elapsed = start.elapsed();
    println!(
        "重复数: {}, 耗时: {:?}, QPS: {:.0}",
        dup_count,
        elapsed,
        1000.0 / elapsed.as_secs_f64()
    );

    println!("\n--- 布隆过滤器统计 ---");
    let stats = engine.bloom_stats();
    println!("  容量: {}", stats.capacity);
    println!("  已插入: {}", stats.inserted);
    println!("  使用率: {:.2}%", stats.usage_ratio * 100.0);
    println!("  总查询: {}", stats.total_checks);
    println!("  布隆命中: {}", stats.bloom_hits);
    println!("  真实命中: {}", stats.real_hits);
    println!("  估算误判率: {:.4}%", stats.estimated_fpr * 100.0);

    println!("\n--- 清理器统计 ---");
    let cstats = engine.cleaner_stats();
    println!("  运行次数: {}", cstats.run_count);
    println!("  累计删除: {}", cstats.total_deleted);

    println!("\n--- 有效 Key 总数 ---");
    let count = engine.count_valid_keys()?;
    println!("  {} 条", count);

    println!("\n关闭引擎...");
    engine.shutdown().await?;
    println!("完成。");

    Ok(())
}
