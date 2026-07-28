//! 去重引擎入口程序
//!
//! 支持两种运行模式：
//! - `demo`：运行内置示例（默认）
//! - `serve`：启动 gRPC 服务

use std::sync::Arc;

use clap::{Parser, Subcommand};
use rocksdb_dedup::{DedupConfig, DedupEngine};

#[derive(Parser)]
#[command(name = "rocksdb-dedup", version, about = "基于 RocksDB 的高并发去重服务")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// 运行演示程序
    Demo {
        /// 数据目录
        #[arg(long, default_value = "./data/dedup_demo")]
        db_path: String,
    },
    /// 启动 gRPC 服务
    Serve {
        /// 监听地址
        #[arg(long, default_value = "0.0.0.0:50051")]
        addr: String,

        /// 数据目录
        #[arg(long, default_value = "./data/dedup")]
        db_path: String,

        /// 布隆过滤器容量
        #[arg(long, default_value_t = 10_000_000)]
        bloom_capacity: u64,

        /// 布隆过滤器误判率
        #[arg(long, default_value_t = 0.01)]
        bloom_fpp: f64,

        /// 清理批大小
        #[arg(long, default_value_t = 10_000)]
        cleaner_batch_size: usize,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command.unwrap_or(Commands::Demo {
        db_path: "./data/dedup_demo".to_string(),
    }) {
        Commands::Demo { db_path } => run_demo(db_path).await,
        Commands::Serve {
            addr,
            db_path,
            bloom_capacity,
            bloom_fpp,
            cleaner_batch_size,
        } => run_serve(addr, db_path, bloom_capacity, bloom_fpp, cleaner_batch_size).await,
    }
}

async fn run_serve(
    addr: String,
    db_path: String,
    bloom_capacity: u64,
    bloom_fpp: f64,
    cleaner_batch_size: usize,
) -> anyhow::Result<()> {
    let config = DedupConfig {
        db_path,
        bloom_capacity,
        bloom_false_positive_rate: bloom_fpp,
        bloom_rebuild_interval_secs: 3600,
        bloom_expand_threshold: 0.8,
        bloom_expand_factor: 1.5,
        cleaner_interval_secs: 60,
        cleaner_batch_size,
        cleaner_batch_sleep_ms: 5,
        max_metadata_size: 4096,
    };

    println!("=== RocksDB 去重 gRPC 服务 ===");
    println!("数据目录: {}", config.db_path);
    println!("布隆容量: {}", config.bloom_capacity);
    println!("误判率: {}", config.bloom_false_positive_rate);

    let engine = DedupEngine::open(config).await?;
    let engine = Arc::new(engine);

    let socket_addr: std::net::SocketAddr = addr.parse()?;
    rocksdb_dedup::serve(engine, socket_addr).await?;

    Ok(())
}

async fn run_demo(db_path: String) -> anyhow::Result<()> {
    println!("=== RocksDB 去重引擎示例 ===");

    let config = DedupConfig {
        db_path,
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
