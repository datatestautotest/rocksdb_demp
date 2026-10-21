//! 去重引擎入口程序
//!
//! 支持运行模式：
//! - `demo`：运行内置示例（默认）
//! - `serve`：启动 gRPC 服务与 HTTP 监控端点
//! - `status`：查看运行中服务的监控状态
//! - `pause-cleaner`：暂停指定服务的后台清理器
//! - `resume-cleaner`：恢复指定服务的后台清理器
//! - `check`：诊断/检查指定 Key 的去重状态

use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
    /// 启动 gRPC 服务与 HTTP 运维监控服务
    Serve {
        /// 配置文件路径（TOML 格式），指定时优先从文件读取配置
        #[arg(short, long)]
        config: Option<String>,

        /// gRPC 监听地址
        #[arg(long, default_value = "0.0.0.0:50051")]
        addr: String,

        /// HTTP 监控与健康检查监听地址（Prometheus /metrics, /health, /stats）
        #[arg(long, default_value = "0.0.0.0:9090")]
        metrics_addr: String,

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

        /// RocksDB Block Cache 缓存大小（MB）
        #[arg(long, default_value_t = 256)]
        block_cache_mb: usize,

        /// 是否开启 LSM-Tree 分层压缩（LZ4 + ZSTD）
        #[arg(long, default_value_t = true)]
        enable_compression: bool,
    },
    /// 查看运行中服务的监控状态
    Status {
        /// 目标 HTTP 监控地址（如 127.0.0.1:9090）
        #[arg(long, default_value = "127.0.0.1:9090")]
        addr: String,
    },
    /// 暂停指定服务实例的后台过期清理器
    PauseCleaner {
        /// 目标 HTTP 监控地址（如 127.0.0.1:9090）
        #[arg(long, default_value = "127.0.0.1:9090")]
        addr: String,
    },
    /// 恢复指定服务实例的后台过期清理器
    ResumeCleaner {
        /// 目标 HTTP 监控地址（如 127.0.0.1:9090）
        #[arg(long, default_value = "127.0.0.1:9090")]
        addr: String,
    },
    /// 触发指定服务实例的 RocksDB 物理 Compaction 整理
    Compact {
        /// 目标 HTTP 监控地址（如 127.0.0.1:9090）
        #[arg(long, default_value = "127.0.0.1:9090")]
        addr: String,
    },
    /// 诊断/检查单条 Key 是否存在并显示元数据
    Check {
        /// 数据目录
        #[arg(long, default_value = "./data/dedup")]
        db_path: String,

        /// 业务命名空间
        #[arg(long, default_value = "crawler")]
        namespace: String,

        /// 待查 Key（如 URL）
        #[arg(long)]
        key: String,
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
            config,
            addr,
            metrics_addr,
            db_path,
            bloom_capacity,
            bloom_fpp,
            cleaner_batch_size,
            block_cache_mb,
            enable_compression,
        } => {
            run_serve(
                config,
                addr,
                metrics_addr,
                db_path,
                bloom_capacity,
                bloom_fpp,
                cleaner_batch_size,
                block_cache_mb,
                enable_compression,
            )
            .await
        }
        Commands::Status { addr } => run_status(&addr).await,
        Commands::PauseCleaner { addr } => run_pause_cleaner(&addr).await,
        Commands::ResumeCleaner { addr } => run_resume_cleaner(&addr).await,
        Commands::Compact { addr } => run_compact(&addr).await,
        Commands::Check {
            db_path,
            namespace,
            key,
        } => run_check(&db_path, &namespace, &key).await,
    }
}

async fn run_serve(
    config_file: Option<String>,
    addr: String,
    metrics_addr: String,
    db_path: String,
    bloom_capacity: u64,
    bloom_fpp: f64,
    cleaner_batch_size: usize,
    block_cache_mb: usize,
    enable_compression: bool,
) -> anyhow::Result<()> {
    let config = if let Some(ref path) = config_file {
        println!("正在从配置文件加载: {}", path);
        DedupConfig::from_file(path)?
    } else {
        DedupConfig {
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
            block_cache_size_mb: block_cache_mb,
            enable_compression,
            block_size_kb: 16,
            cleaner_compact_threshold: 100_000,
        }
    };

    println!("=== RocksDB 去重服务启动 ===");
    println!("数据目录:     {}", config.db_path);
    println!("布隆容量:     {}", config.bloom_capacity);
    println!("误判率:       {}", config.bloom_false_positive_rate);
    println!("Block Cache:  {} MB", config.block_cache_size_mb);
    println!("分层压缩:     {}", if config.enable_compression { "开启 (LZ4 + ZSTD)" } else { "关闭" });
    println!("自动 Compaction: 每删除 {} 条触发", config.cleaner_compact_threshold);
    println!("gRPC 监听:    {}", addr);
    println!("HTTP 监控:    http://{}", metrics_addr);

    let engine = DedupEngine::open(config).await?;
    let engine = Arc::new(engine);

    let socket_addr: std::net::SocketAddr = addr.parse()?;
    let http_addr: std::net::SocketAddr = metrics_addr.parse()?;

    // 优雅停机广播通知
    let (shutdown_tx, shutdown_rx_grpc) = tokio::sync::watch::channel(());
    let shutdown_rx_http = shutdown_tx.subscribe();

    // 监听 Ctrl+C 信号
    let shutdown_tx_clone = shutdown_tx.clone();
    tokio::spawn(async move {
        if let Ok(()) = tokio::signal::ctrl_c().await {
            println!("\n[main] 接收到退出信号 (Ctrl+C)，开始优雅退出...");
            let _ = shutdown_tx_clone.send(());
        }
    });

    let shutdown_tx_for_err = shutdown_tx.clone();
    let grpc_engine = engine.clone();
    let grpc_handle = tokio::spawn(async move {
        let mut rx = shutdown_rx_grpc;
        let signal = async move {
            let _ = rx.changed().await;
        };
        let res = rocksdb_dedup::serve_with_shutdown(grpc_engine, socket_addr, signal).await;
        if let Err(ref e) = res {
            eprintln!("[grpc] 服务启动或运行异常: {}", e);
            let _ = shutdown_tx_for_err.send(());
        }
        res
    });

    let shutdown_tx_for_http = shutdown_tx.clone();
    let http_engine = engine.clone();
    let http_handle = tokio::spawn(async move {
        let res = rocksdb_dedup::serve_metrics(http_engine, http_addr, shutdown_rx_http).await;
        if let Err(ref e) = res {
            eprintln!("[http] 监控服务启动或运行异常: {}", e);
            let _ = shutdown_tx_for_http.send(());
        }
        res
    });

    // 等待服务正常退出
    let (grpc_res, http_res) = tokio::join!(grpc_handle, http_handle);
    grpc_res??;
    http_res??;

    println!("[main] 正在刷新 RocksDB WAL 及 Memtable 落盘...");
    engine.flush()?;
    println!("[main] RocksDB 去重服务已安全优雅停止。");

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
        block_cache_size_mb: 64,
        enable_compression: true,
        block_size_kb: 16,
        cleaner_compact_threshold: 1000,
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

    println!("\n--- 统计信息 ---");
    let bloom = engine.bloom_stats();
    println!("布隆容量: {}", bloom.capacity);
    println!("已插入: {}", bloom.inserted);
    println!("理论误判率: {:.4}%", bloom.estimated_fpr * 100.0);
    println!("查询拦截: {}/{}", bloom.bloom_hits, bloom.total_checks);

    let cleaner = engine.cleaner_stats();
    println!("\n清理运行次数: {}", cleaner.run_count);
    println!("累计清理: {}", cleaner.total_deleted);

    println!("\n关闭引擎...");
    engine.shutdown().await?;
    println!("完成！");

    Ok(())
}

// ---------------------------------------------------------------------------
// CLI HTTP 客户端辅助函数
// ---------------------------------------------------------------------------

async fn http_get(addr: &str, path: &str) -> anyhow::Result<String> {
    let mut stream = tokio::net::TcpStream::connect(addr).await?;
    let req = format!("GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n", path, addr);
    stream.write_all(req.as_bytes()).await?;
    let mut response = String::new();
    stream.read_to_string(&mut response).await?;
    if let Some(idx) = response.find("\r\n\r\n") {
        Ok(response[idx + 4..].to_string())
    } else {
        Ok(response)
    }
}

async fn http_post(addr: &str, path: &str) -> anyhow::Result<String> {
    let mut stream = tokio::net::TcpStream::connect(addr).await?;
    let req = format!("POST {} HTTP/1.1\r\nHost: {}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n", path, addr);
    stream.write_all(req.as_bytes()).await?;
    let mut response = String::new();
    stream.read_to_string(&mut response).await?;
    if let Some(idx) = response.find("\r\n\r\n") {
        Ok(response[idx + 4..].to_string())
    } else {
        Ok(response)
    }
}

async fn run_status(addr: &str) -> anyhow::Result<()> {
    println!("正在查询服务状态: http://{}", addr);
    let body = match http_get(addr, "/stats").await {
        Ok(b) => b,
        Err(e) => {
            eprintln!("❌ 无法连接到去重服务监控端点 http://{}: {}", addr, e);
            eprintln!("请确认服务是否已启动（`cargo run -- serve`）且端口正确。");
            return Err(e);
        }
    };

    let val: serde_json::Value = serde_json::from_str(&body)?;
    let bloom = &val["bloom"];
    let cleaner = &val["cleaner"];
    let rocksdb = &val["rocksdb"];

    println!("\n==================================================");
    println!("           RocksDB 去重服务运行状态监控");
    println!("==================================================");
    println!("[RocksDB 存储]");
    println!("  • 预估 Key 总数:     {}", rocksdb["estimated_keys"]);

    println!("\n[布隆过滤器 (Bloom Filter)]");
    println!("  • 设计容量:          {}", bloom["capacity"]);
    println!("  • 当前插入量:        {}", bloom["inserted"]);
    println!("  • 估算误判率:        {:.4}%", bloom["estimated_fpr"].as_f64().unwrap_or(0.0) * 100.0);
    println!("  • 累计检查次数:      {}", bloom["total_checks"]);
    println!("  • 布隆拦截命中:      {}", bloom["bloom_hits"]);
    println!("  • 存储确认命中:      {}", bloom["real_hits"]);

    println!("\n[过期清理器 (Cleaner)]");
    println!("  • 累计清理条数:      {}", cleaner["total_deleted"]);
    println!("  • 积压过期估算:      {}", cleaner["backlog_estimate"]);
    println!("  • 当前清理速度:      {:.1} keys/s", cleaner["current_speed"].as_f64().unwrap_or(0.0));
    println!("  • 当前批次大小:      {}", cleaner["current_batch_size"]);
    println!("  • 物理整理次数:      {}", cleaner["total_compacts"]);
    println!("  • 运行状态:          {}", if cleaner["is_paused"].as_bool().unwrap_or(false) { "⚠️ 已暂停 (Paused)" } else { "✅ 运行中 (Running)" });
    println!("==================================================");

    Ok(())
}

async fn run_pause_cleaner(addr: &str) -> anyhow::Result<()> {
    println!("正在向 http://{}/cleaner/pause 发送暂停请求...", addr);
    let body = http_post(addr, "/cleaner/pause").await?;
    println!("✅ 响应: {}", body.trim());
    println!("清理器已挂起，磁盘 IO 资源已释放给写入流程。");
    Ok(())
}

async fn run_resume_cleaner(addr: &str) -> anyhow::Result<()> {
    println!("正在向 http://{}/cleaner/resume 发送恢复请求...", addr);
    let body = http_post(addr, "/cleaner/resume").await?;
    println!("✅ 响应: {}", body.trim());
    println!("清理器已恢复正常后台调度。");
    Ok(())
}

async fn run_compact(addr: &str) -> anyhow::Result<()> {
    println!("正在向 http://{}/compact 发送物理 Compaction 触发请求...", addr);
    let body = http_post(addr, "/compact").await?;
    println!("✅ 响应: {}", body.trim());
    println!("RocksDB 范围物理整理已触发，删除标记（Tombstone）将被物理清除并释放磁盘空间。");
    Ok(())
}

async fn run_check(db_path: &str, namespace: &str, key: &str) -> anyhow::Result<()> {
    println!("=== 离线诊断 Key 状态 ===");
    println!("数据库:   {}", db_path);
    println!("命名空间: {}", namespace);
    println!("检查键:   {}", key);

    let config = DedupConfig {
        db_path: db_path.to_string(),
        bloom_capacity: 10_000,
        bloom_false_positive_rate: 0.01,
        bloom_rebuild_interval_secs: 3600,
        bloom_expand_threshold: 0.8,
        bloom_expand_factor: 1.5,
        cleaner_interval_secs: 60,
        cleaner_batch_size: 1000,
        cleaner_batch_sleep_ms: 5,
        max_metadata_size: 4096,
        block_cache_size_mb: 32,
        enable_compression: false,
        block_size_kb: 16,
        cleaner_compact_threshold: 0,
    };

    let engine = DedupEngine::open(config).await?;
    let now_ms = chrono::Utc::now().timestamp_millis() as u64;

    match engine.get(namespace.as_bytes(), key.as_bytes())? {
        Some(val) => {
            let is_expired = val.is_expired(now_ms);
            println!("\n🔍 检查结果: [已存在]");
            println!("  • 是否已过期: {}", if is_expired { "⚠️ 已过期" } else { "✅ 有效（未过期）" });
            println!("  • 过期时间戳: {} (当前水位: {})", val.expire_at_ms, now_ms);
            println!("  • 创建时间戳: {}", val.created_at_ms);
            if !val.metadata.is_empty() {
                println!("  • 附加元数据: {} bytes (文本 preview: {})", val.metadata.len(), String::from_utf8_lossy(&val.metadata));
            }
        }
        None => {
            println!("\n🔍 检查结果: [不存在]（未在库中找到此 Key）");
        }
    }

    engine.shutdown().await?;
    Ok(())
}
