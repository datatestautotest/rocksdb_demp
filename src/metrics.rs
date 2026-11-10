//! 运维监控与 HTTP 端点
//!
//! 提供：
//! - 业务 RESTful API：`/api/v1/check`、`/api/v1/batch_check`、`/api/v1/exists`、`/api/v1/batch_exists`、`/api/v1/get`、`/api/v1/delete`、`/api/v1/batch_delete`
//! - `/health`：服务健康检查（用于 K8s liveness/readiness probe）
//! - `/metrics`：Prometheus 格式指标（用于 Prometheus 抓取）
//! - `/stats`：详细 JSON 状态接口（用于 CLI / 诊断工具）
//! - `/cleaner/pause` 与 `/cleaner/resume`：运行时控制清理器挂起/恢复
//! - `/compact`：手动触发 RocksDB 物理 Compaction

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::json;

use crate::api;
use crate::engine::DedupEngine;

/// HTTP 服务状态
#[derive(Clone)]
pub struct MetricsServerState {
    pub engine: Arc<DedupEngine>,
}

/// 构建包含监控与业务 RESTful API 的统一 HTTP 路由 Router
pub fn create_metrics_router(engine: Arc<DedupEngine>) -> Router {
    let state = MetricsServerState { engine };

    Router::new()
        // 运维与监控端点
        .route("/health", get(health_handler))
        .route("/healthz", get(health_handler))
        .route("/metrics", get(prometheus_metrics_handler))
        .route("/stats", get(stats_handler))
        .route("/cleaner/pause", post(cleaner_pause_handler))
        .route("/cleaner/resume", post(cleaner_resume_handler))
        .route("/compact", post(compact_handler))
        // 业务 RESTful API (v1)
        .route("/api/v1/check", post(api::check_handler))
        .route("/api/v1/batch_check", post(api::batch_check_handler))
        .route("/api/v1/exists", post(api::exists_handler))
        .route("/api/v1/batch_exists", post(api::batch_exists_handler))
        .route("/api/v1/get", post(api::get_handler))
        .route("/api/v1/delete", post(api::delete_handler))
        .route("/api/v1/batch_delete", post(api::batch_delete_handler))
        .with_state(state)
}

/// 健康检查接口
pub async fn health_handler(State(state): State<MetricsServerState>) -> (StatusCode, Json<serde_json::Value>) {
    let is_paused = state.engine.is_cleaner_paused();
    let now_ms = chrono::Utc::now().timestamp_millis() as u64;

    let body = json!({
        "status": "UP",
        "service": "rocksdb-dedup",
        "cleaner_paused": is_paused,
        "timestamp_ms": now_ms,
    });

    (StatusCode::OK, Json(body))
}

/// Prometheus 纯文本指标抓取接口
pub async fn prometheus_metrics_handler(State(state): State<MetricsServerState>) -> Response {
    let bloom = state.engine.bloom_stats();
    let cleaner = state.engine.cleaner_stats();
    let total_keys = state.engine.store().estimate_total_keys();

    let hit_rate = if bloom.total_checks > 0 {
        bloom.bloom_hits as f64 / bloom.total_checks as f64
    } else {
        0.0
    };

    let mut out = String::with_capacity(1024);

    // 布隆过滤器指标
    out.push_str("# HELP rocksdb_dedup_bloom_capacity Total capacity of Bloom filter.\n");
    out.push_str("# TYPE rocksdb_dedup_bloom_capacity gauge\n");
    out.push_str(&format!("rocksdb_dedup_bloom_capacity {}\n", bloom.capacity));

    out.push_str("# HELP rocksdb_dedup_bloom_inserted Current number of elements inserted in Bloom filter.\n");
    out.push_str("# TYPE rocksdb_dedup_bloom_inserted gauge\n");
    out.push_str(&format!("rocksdb_dedup_bloom_inserted {}\n", bloom.inserted));

    out.push_str("# HELP rocksdb_dedup_bloom_fpr_estimated Estimated false positive rate of Bloom filter.\n");
    out.push_str("# TYPE rocksdb_dedup_bloom_fpr_estimated gauge\n");
    out.push_str(&format!("rocksdb_dedup_bloom_fpr_estimated {:.6}\n", bloom.estimated_fpr));

    out.push_str("# HELP rocksdb_dedup_bloom_checks_total Total Bloom filter check operations.\n");
    out.push_str("# TYPE rocksdb_dedup_bloom_checks_total counter\n");
    out.push_str(&format!("rocksdb_dedup_bloom_checks_total {}\n", bloom.total_checks));

    out.push_str("# HELP rocksdb_dedup_bloom_hits_total Total Bloom filter positive hits.\n");
    out.push_str("# TYPE rocksdb_dedup_bloom_hits_total counter\n");
    out.push_str(&format!("rocksdb_dedup_bloom_hits_total {}\n", bloom.bloom_hits));

    out.push_str("# HELP rocksdb_dedup_real_hits_total Total confirmed hits verified against RocksDB.\n");
    out.push_str("# TYPE rocksdb_dedup_real_hits_total counter\n");
    out.push_str(&format!("rocksdb_dedup_real_hits_total {}\n", bloom.real_hits));

    out.push_str("# HELP rocksdb_dedup_bloom_hit_rate Ratio of Bloom hits to total checks.\n");
    out.push_str("# TYPE rocksdb_dedup_bloom_hit_rate gauge\n");
    out.push_str(&format!("rocksdb_dedup_bloom_hit_rate {:.4}\n", hit_rate));

    // 清理器指标
    out.push_str("# HELP rocksdb_dedup_cleaner_deleted_total Total expired keys deleted by cleaner.\n");
    out.push_str("# TYPE rocksdb_dedup_cleaner_deleted_total counter\n");
    out.push_str(&format!("rocksdb_dedup_cleaner_deleted_total {}\n", cleaner.total_deleted));

    out.push_str("# HELP rocksdb_dedup_cleaner_backlog_estimate Estimated number of expired keys pending deletion.\n");
    out.push_str("# TYPE rocksdb_dedup_cleaner_backlog_estimate gauge\n");
    out.push_str(&format!("rocksdb_dedup_cleaner_backlog_estimate {}\n", cleaner.backlog_estimate));

    out.push_str("# HELP rocksdb_dedup_cleaner_speed Keys deleted per second in recent cleanup rounds.\n");
    out.push_str("# TYPE rocksdb_dedup_cleaner_speed gauge\n");
    out.push_str(&format!("rocksdb_dedup_cleaner_speed {:.2}\n", cleaner.current_speed));

    out.push_str("# HELP rocksdb_dedup_cleaner_batch_size Current adaptive cleanup batch size.\n");
    out.push_str("# TYPE rocksdb_dedup_cleaner_batch_size gauge\n");
    out.push_str(&format!("rocksdb_dedup_cleaner_batch_size {}\n", cleaner.current_batch_size));

    out.push_str("# HELP rocksdb_dedup_cleaner_runs_total Total cleanup loop execution rounds.\n");
    out.push_str("# TYPE rocksdb_dedup_cleaner_runs_total counter\n");
    out.push_str(&format!("rocksdb_dedup_cleaner_runs_total {}\n", cleaner.run_count));

    out.push_str("# HELP rocksdb_dedup_cleaner_paused Whether the background cleaner is currently paused (1=paused, 0=running).\n");
    out.push_str("# TYPE rocksdb_dedup_cleaner_paused gauge\n");
    out.push_str(&format!("rocksdb_dedup_cleaner_paused {}\n", if cleaner.is_paused { 1 } else { 0 }));

    out.push_str("# HELP rocksdb_dedup_compacts_total Total RocksDB manual compaction operations triggered.\n");
    out.push_str("# TYPE rocksdb_dedup_compacts_total counter\n");
    out.push_str(&format!("rocksdb_dedup_compacts_total {}\n", cleaner.total_compacts));

    // RocksDB 存储指标
    out.push_str("# HELP rocksdb_dedup_total_keys Estimated total keys in RocksDB cf_dedup.\n");
    out.push_str("# TYPE rocksdb_dedup_total_keys gauge\n");
    out.push_str(&format!("rocksdb_dedup_total_keys {}\n", total_keys));

    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        .body(out.into())
        .unwrap()
}

/// 详细运行期统计 JSON 端点
pub async fn stats_handler(State(state): State<MetricsServerState>) -> (StatusCode, Json<serde_json::Value>) {
    let bloom = state.engine.bloom_stats();
    let cleaner = state.engine.cleaner_stats();
    let total_keys = state.engine.store().estimate_total_keys();

    let body = json!({
        "rocksdb": {
            "estimated_keys": total_keys,
        },
        "bloom": {
            "capacity": bloom.capacity,
            "inserted": bloom.inserted,
            "estimated_fpr": bloom.estimated_fpr,
            "total_checks": bloom.total_checks,
            "bloom_hits": bloom.bloom_hits,
            "real_hits": bloom.real_hits,
        },
        "cleaner": {
            "total_deleted": cleaner.total_deleted,
            "backlog_estimate": cleaner.backlog_estimate,
            "current_speed": cleaner.current_speed,
            "current_batch_size": cleaner.current_batch_size,
            "run_count": cleaner.run_count,
            "is_paused": cleaner.is_paused,
            "total_compacts": cleaner.total_compacts,
        }
    });

    (StatusCode::OK, Json(body))
}

/// 暂停清理器
pub async fn cleaner_pause_handler(State(state): State<MetricsServerState>) -> (StatusCode, Json<serde_json::Value>) {
    state.engine.pause_cleaner();
    (StatusCode::OK, Json(json!({"status": "cleaner paused"})))
}

/// 恢复清理器
pub async fn cleaner_resume_handler(State(state): State<MetricsServerState>) -> (StatusCode, Json<serde_json::Value>) {
    state.engine.resume_cleaner();
    (StatusCode::OK, Json(json!({"status": "cleaner resumed"})))
}

/// 手动触发 RocksDB Compaction
pub async fn compact_handler(State(state): State<MetricsServerState>) -> (StatusCode, Json<serde_json::Value>) {
    let engine = state.engine.clone();
    tokio::task::spawn_blocking(move || {
        engine.compact();
    })
    .await
    .ok();

    (StatusCode::OK, Json(json!({
        "status": "COMPACTED",
        "message": "RocksDB manual compaction triggered successfully",
        "timestamp_ms": chrono::Utc::now().timestamp_millis(),
    })))
}

/// 启动 HTTP 服务（包含 RESTful API 与运维监控端点，带优雅关闭监听）
pub async fn serve_metrics(
    engine: Arc<DedupEngine>,
    addr: SocketAddr,
    shutdown_rx: tokio::sync::watch::Receiver<()>,
) -> anyhow::Result<()> {
    let app = create_metrics_router(engine);

    println!("[http] HTTP 服务已启动，监听 http://{}", addr);
    println!("       ├── RESTful API: http://{}/api/v1 (单条/批量查重与删除)", addr);
    println!("       └── 监控运维:   http://{0}/health, http://{0}/stats, http://{0}/metrics", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let mut rx = shutdown_rx;
            let _ = rx.changed().await;
        })
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DedupConfig;
    use tempfile::TempDir;

    async fn setup_engine() -> (DedupEngine, TempDir) {
        let dir = TempDir::new().unwrap();
        let config = DedupConfig::for_test(dir.path().to_str().unwrap().to_string());
        let engine = DedupEngine::open(config).await.unwrap();
        (engine, dir)
    }

    #[tokio::test]
    async fn test_metrics_handlers() {
        let (engine, _dir) = setup_engine().await;
        let engine = Arc::new(engine);
        let state = MetricsServerState { engine: engine.clone() };

        // 1. 测试 health_handler
        let (code, json_val) = health_handler(State(state.clone())).await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(json_val["status"], "UP");
        assert_eq!(json_val["cleaner_paused"], false);

        // 2. 测试 stats_handler
        let (code, stats_val) = stats_handler(State(state.clone())).await;
        assert_eq!(code, StatusCode::OK);
        assert!(stats_val.get("bloom").is_some());
        assert!(stats_val.get("cleaner").is_some());
        assert!(stats_val.get("rocksdb").is_some());

        // 3. 测试 prometheus_metrics_handler
        let resp = prometheus_metrics_handler(State(state.clone())).await;
        assert_eq!(resp.status(), StatusCode::OK);

        // 4. 测试 cleaner_pause_handler 与 cleaner_resume_handler
        let (code, pause_val) = cleaner_pause_handler(State(state.clone())).await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(pause_val["status"], "cleaner paused");
        assert!(engine.is_cleaner_paused());

        let (code, resume_val) = cleaner_resume_handler(State(state.clone())).await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(resume_val["status"], "cleaner resumed");
        assert!(!engine.is_cleaner_paused());
    }
}
