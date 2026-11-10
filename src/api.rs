//! RESTful HTTP 业务接口定义与 Handlers
//!
//! 提供 `/api/v1` 业务路由，支持单条与批量查重、存在性检查、详情获取与删除。

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::engine::CheckResult;
use crate::key::DedupValue;
use crate::metrics::MetricsServerState;

// -----------------------------------------------------------------------------
// 请求与响应数据结构定义
// -----------------------------------------------------------------------------

/// 单条查重并写入请求
#[derive(Debug, Clone, Deserialize)]
pub struct CheckRequest {
    /// 业务命名空间（如 "spider_news"）
    pub namespace: String,
    /// 待去重的 Key（如 URL 或唯一标识）
    pub key: String,
    /// 绝对过期时间戳（毫秒），0 或不传表示永不过期
    #[serde(default)]
    pub expire_at_ms: u64,
    /// 附加业务元数据（可选，如抓取深度、任务来源等 JSON 字符串）
    pub metadata: Option<String>,
}

/// Key 详细信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiValue {
    pub expire_at_ms: u64,
    pub created_at_ms: u64,
    pub metadata: Option<String>,
}

impl From<&DedupValue> for ApiValue {
    fn from(v: &DedupValue) -> Self {
        let meta_str = if v.metadata.is_empty() {
            None
        } else {
            match String::from_utf8(v.metadata.clone()) {
                Ok(s) => Some(s),
                Err(_) => Some(String::from_utf8_lossy(&v.metadata).into_owned()),
            }
        };

        Self {
            expire_at_ms: v.expire_at_ms,
            created_at_ms: v.created_at_ms,
            metadata: meta_str,
        }
    }
}

/// 单条查重并写入响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckResponse {
    /// 状态："inserted"（新插入） | "duplicate"（重复） | "expired_and_replaced"（过期替换）
    pub status: String,
    /// 是否重复（当 status 为 duplicate 时为 true）
    pub is_duplicate: bool,
    /// 对应 Key 的元信息（若存在旧值）
    pub value: Option<ApiValue>,
}

/// 批量查重单项
#[derive(Debug, Clone, Deserialize)]
pub struct BatchCheckItem {
    pub key: String,
    #[serde(default)]
    pub expire_at_ms: u64,
    pub metadata: Option<String>,
}

/// 批量查重请求
#[derive(Debug, Clone, Deserialize)]
pub struct BatchCheckRequest {
    pub namespace: String,
    pub items: Vec<BatchCheckItem>,
}

/// 批量查重结果项
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchCheckResultItem {
    pub key: String,
    pub status: String,
    pub is_duplicate: bool,
    pub value: Option<ApiValue>,
}

/// 批量查重响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchCheckResponse {
    pub total: usize,
    pub results: Vec<BatchCheckResultItem>,
}

/// 存在性检查请求
#[derive(Debug, Clone, Deserialize)]
pub struct ExistsRequest {
    pub namespace: String,
    pub key: String,
}

/// 存在性检查响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExistsResponse {
    pub exists: bool,
}

/// 批量存在性检查请求
#[derive(Debug, Clone, Deserialize)]
pub struct BatchExistsRequest {
    pub namespace: String,
    pub keys: Vec<String>,
}

/// 批量存在性检查响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchExistsResponse {
    pub results: Vec<bool>,
}

/// 获取详情请求
#[derive(Debug, Clone, Deserialize)]
pub struct GetRequest {
    pub namespace: String,
    pub key: String,
}

/// 获取详情响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetResponse {
    pub found: bool,
    pub value: Option<ApiValue>,
}

/// 删除 Key 请求
#[derive(Debug, Clone, Deserialize)]
pub struct DeleteRequest {
    pub namespace: String,
    pub key: String,
}

/// 删除 Key 响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteResponse {
    pub deleted: bool,
}

/// 批量删除请求
#[derive(Debug, Clone, Deserialize)]
pub struct BatchDeleteRequest {
    pub namespace: String,
    pub keys: Vec<String>,
}

/// 批量删除响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchDeleteResponse {
    pub results: Vec<bool>,
}

// -----------------------------------------------------------------------------
// 鉴权检查函数
// -----------------------------------------------------------------------------

pub fn verify_auth(
    headers: &HeaderMap,
    expected_token: Option<&str>,
) -> Result<(), (StatusCode, Json<serde_json::Value>)> {
    if let Some(expected) = expected_token {
        if !expected.is_empty() {
            let auth_header = headers.get("authorization").and_then(|h| h.to_str().ok());
            let api_key_header = headers.get("x-api-key").and_then(|h| h.to_str().ok());

            let valid = match (auth_header, api_key_header) {
                (Some(auth), _) => {
                    let token = auth.strip_prefix("Bearer ").unwrap_or(auth).trim();
                    token == expected
                }
                (_, Some(key)) => key.trim() == expected,
                _ => false,
            };

            if !valid {
                return Err((
                    StatusCode::UNAUTHORIZED,
                    Json(serde_json::json!({
                        "error": "Unauthorized",
                        "message": "Invalid or missing authorization token. Please provide 'Authorization: Bearer <token>' or 'X-API-Key: <token>' header."
                    })),
                ));
            }
        }
    }
    Ok(())
}

// -----------------------------------------------------------------------------
// Handler 实现
// -----------------------------------------------------------------------------

/// 单条查重并写入 Handler
pub async fn check_handler(
    State(state): State<MetricsServerState>,
    headers: HeaderMap,
    Json(req): Json<CheckRequest>,
) -> Result<Json<CheckResponse>, (StatusCode, Json<serde_json::Value>)> {
    verify_auth(&headers, state.engine.config().auth_token.as_deref())?;

    let meta_bytes = req.metadata.map(|s| s.into_bytes());
    let res = state
        .engine
        .check_and_set(
            req.namespace.as_bytes(),
            req.key.as_bytes(),
            req.expire_at_ms,
            meta_bytes,
        )
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.to_string()})),
            )
        })?;

    let (status, is_dup, val) = match res {
        CheckResult::Inserted => ("inserted".to_string(), false, None),
        CheckResult::Duplicate { existing } => {
            ("duplicate".to_string(), true, Some(ApiValue::from(&existing)))
        }
        CheckResult::ExpiredAndReplaced { old } => (
            "expired_and_replaced".to_string(),
            false,
            Some(ApiValue::from(&old)),
        ),
    };

    Ok(Json(CheckResponse {
        status,
        is_duplicate: is_dup,
        value: val,
    }))
}

/// 批量查重并写入 Handler
pub async fn batch_check_handler(
    State(state): State<MetricsServerState>,
    headers: HeaderMap,
    Json(req): Json<BatchCheckRequest>,
) -> Result<Json<BatchCheckResponse>, (StatusCode, Json<serde_json::Value>)> {
    verify_auth(&headers, state.engine.config().auth_token.as_deref())?;

    let namespace_bytes = req.namespace.as_bytes();
    let tuples: Vec<(&[u8], &[u8], u64, Option<Vec<u8>>)> = req
        .items
        .iter()
        .map(|item| {
            (
                namespace_bytes,
                item.key.as_bytes(),
                item.expire_at_ms,
                item.metadata.as_ref().map(|s| s.as_bytes().to_vec()),
            )
        })
        .collect();

    let results = state
        .engine
        .batch_check_and_set(&tuples)
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.to_string()})),
            )
        })?;

    let response_results: Vec<BatchCheckResultItem> = req
        .items
        .into_iter()
        .zip(results.into_iter())
        .map(|(item, res)| {
            let (status, is_dup, val) = match res {
                CheckResult::Inserted => ("inserted".to_string(), false, None),
                CheckResult::Duplicate { existing } => {
                    ("duplicate".to_string(), true, Some(ApiValue::from(&existing)))
                }
                CheckResult::ExpiredAndReplaced { old } => (
                    "expired_and_replaced".to_string(),
                    false,
                    Some(ApiValue::from(&old)),
                ),
            };
            BatchCheckResultItem {
                key: item.key,
                status,
                is_duplicate: is_dup,
                value: val,
            }
        })
        .collect();

    Ok(Json(BatchCheckResponse {
        total: response_results.len(),
        results: response_results,
    }))
}

/// 单条存在性检查 Handler
pub async fn exists_handler(
    State(state): State<MetricsServerState>,
    headers: HeaderMap,
    Json(req): Json<ExistsRequest>,
) -> Result<Json<ExistsResponse>, (StatusCode, Json<serde_json::Value>)> {
    verify_auth(&headers, state.engine.config().auth_token.as_deref())?;

    let exists = state
        .engine
        .exists(req.namespace.as_bytes(), req.key.as_bytes())
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.to_string()})),
            )
        })?;

    Ok(Json(ExistsResponse { exists }))
}

/// 批量存在性检查 Handler
pub async fn batch_exists_handler(
    State(state): State<MetricsServerState>,
    headers: HeaderMap,
    Json(req): Json<BatchExistsRequest>,
) -> Result<Json<BatchExistsResponse>, (StatusCode, Json<serde_json::Value>)> {
    verify_auth(&headers, state.engine.config().auth_token.as_deref())?;

    let key_slices: Vec<&[u8]> = req.keys.iter().map(|k| k.as_bytes()).collect();
    let results = state
        .engine
        .batch_exists(req.namespace.as_bytes(), &key_slices)
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.to_string()})),
            )
        })?;

    Ok(Json(BatchExistsResponse { results }))
}

/// 获取详情 Handler
pub async fn get_handler(
    State(state): State<MetricsServerState>,
    headers: HeaderMap,
    Json(req): Json<GetRequest>,
) -> Result<Json<GetResponse>, (StatusCode, Json<serde_json::Value>)> {
    verify_auth(&headers, state.engine.config().auth_token.as_deref())?;

    let value_opt = state
        .engine
        .get(req.namespace.as_bytes(), req.key.as_bytes())
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.to_string()})),
            )
        })?;

    match value_opt {
        Some(v) => Ok(Json(GetResponse {
            found: true,
            value: Some(ApiValue::from(&v)),
        })),
        None => Ok(Json(GetResponse {
            found: false,
            value: None,
        })),
    }
}

/// 删除 Key Handler
pub async fn delete_handler(
    State(state): State<MetricsServerState>,
    headers: HeaderMap,
    Json(req): Json<DeleteRequest>,
) -> Result<Json<DeleteResponse>, (StatusCode, Json<serde_json::Value>)> {
    verify_auth(&headers, state.engine.config().auth_token.as_deref())?;

    let deleted = state
        .engine
        .delete(req.namespace.as_bytes(), req.key.as_bytes())
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.to_string()})),
            )
        })?;

    Ok(Json(DeleteResponse { deleted }))
}

/// 批量删除 Handler
pub async fn batch_delete_handler(
    State(state): State<MetricsServerState>,
    headers: HeaderMap,
    Json(req): Json<BatchDeleteRequest>,
) -> Result<Json<BatchDeleteResponse>, (StatusCode, Json<serde_json::Value>)> {
    verify_auth(&headers, state.engine.config().auth_token.as_deref())?;

    let key_slices: Vec<&[u8]> = req.keys.iter().map(|k| k.as_bytes()).collect();
    let results = state
        .engine
        .batch_delete(req.namespace.as_bytes(), &key_slices)
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.to_string()})),
            )
        })?;

    Ok(Json(BatchDeleteResponse { results }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use crate::config::DedupConfig;
    use crate::engine::DedupEngine;
    use tempfile::TempDir;

    async fn setup_test_engine(token: Option<String>) -> (Arc<DedupEngine>, TempDir) {
        let dir = TempDir::new().unwrap();
        let mut config = DedupConfig::for_test(dir.path().to_str().unwrap().to_string());
        config.auth_token = token;
        let engine = DedupEngine::open(config).await.unwrap();
        (Arc::new(engine), dir)
    }

    #[tokio::test]
    async fn test_api_check_and_batch() {
        let (engine, _dir) = setup_test_engine(None).await;
        let state = MetricsServerState { engine };
        let empty_headers = HeaderMap::new();

        // 1. 首次 check
        let req = CheckRequest {
            namespace: "test_ns".to_string(),
            key: "https://example.com/1".to_string(),
            expire_at_ms: chrono::Utc::now().timestamp_millis() as u64 + 3600000,
            metadata: Some(r#"{"spider":"test"}"#.to_string()),
        };
        let res = check_handler(State(state.clone()), empty_headers.clone(), Json(req))
            .await
            .unwrap();
        assert_eq!(res.status, "inserted");
        assert!(!res.is_duplicate);

        // 2. 二次 check（重复）
        let req2 = CheckRequest {
            namespace: "test_ns".to_string(),
            key: "https://example.com/1".to_string(),
            expire_at_ms: chrono::Utc::now().timestamp_millis() as u64 + 3600000,
            metadata: None,
        };
        let res2 = check_handler(State(state.clone()), empty_headers.clone(), Json(req2))
            .await
            .unwrap();
        assert_eq!(res2.status, "duplicate");
        assert!(res2.is_duplicate);
        assert!(res2.value.is_some());
        assert_eq!(
            res2.value.as_ref().unwrap().metadata.as_deref(),
            Some(r#"{"spider":"test"}"#)
        );

        // 3. exists 测试
        let exists_req = ExistsRequest {
            namespace: "test_ns".to_string(),
            key: "https://example.com/1".to_string(),
        };
        let exists_res = exists_handler(State(state.clone()), empty_headers.clone(), Json(exists_req))
            .await
            .unwrap();
        assert!(exists_res.exists);

        // 4. get 测试
        let get_req = GetRequest {
            namespace: "test_ns".to_string(),
            key: "https://example.com/1".to_string(),
        };
        let get_res = get_handler(State(state.clone()), empty_headers.clone(), Json(get_req))
            .await
            .unwrap();
        assert!(get_res.found);
        assert!(get_res.value.is_some());

        // 5. 批量 check 测试
        let batch_req = BatchCheckRequest {
            namespace: "test_ns".to_string(),
            items: vec![
                BatchCheckItem {
                    key: "https://example.com/1".to_string(), // 重复
                    expire_at_ms: 0,
                    metadata: None,
                },
                BatchCheckItem {
                    key: "https://example.com/batch_new".to_string(), // 新
                    expire_at_ms: 0,
                    metadata: Some("batch_meta".to_string()),
                },
            ],
        };
        let batch_res = batch_check_handler(State(state.clone()), empty_headers.clone(), Json(batch_req))
            .await
            .unwrap();
        assert_eq!(batch_res.total, 2);
        assert_eq!(batch_res.results[0].status, "duplicate");
        assert_eq!(batch_res.results[1].status, "inserted");

        // 6. delete 测试
        let del_req = DeleteRequest {
            namespace: "test_ns".to_string(),
            key: "https://example.com/1".to_string(),
        };
        let del_res = delete_handler(State(state.clone()), empty_headers.clone(), Json(del_req))
            .await
            .unwrap();
        assert!(del_res.deleted);
    }

    #[tokio::test]
    async fn test_api_auth_token() {
        let (engine, _dir) = setup_test_engine(Some("secret123".to_string())).await;
        let state = MetricsServerState { engine };

        let req = CheckRequest {
            namespace: "test_ns".to_string(),
            key: "https://example.com/auth".to_string(),
            expire_at_ms: 0,
            metadata: None,
        };

        // 1. 无 header，预期 401
        let err_no_header = check_handler(State(state.clone()), HeaderMap::new(), Json(req.clone())).await;
        assert!(err_no_header.is_err());
        assert_eq!(err_no_header.unwrap_err().0, StatusCode::UNAUTHORIZED);

        // 2. 错误 token，预期 401
        let mut bad_headers = HeaderMap::new();
        bad_headers.insert("authorization", "Bearer wrong_token".parse().unwrap());
        let err_bad_token = check_handler(State(state.clone()), bad_headers, Json(req.clone())).await;
        assert!(err_bad_token.is_err());
        assert_eq!(err_bad_token.unwrap_err().0, StatusCode::UNAUTHORIZED);

        // 3. 正确 token (Bearer)，预期成功
        let mut ok_headers = HeaderMap::new();
        ok_headers.insert("authorization", "Bearer secret123".parse().unwrap());
        let ok_res = check_handler(State(state.clone()), ok_headers, Json(req.clone())).await;
        assert!(ok_res.is_ok());

        // 4. 正确 token (X-API-Key)，预期成功
        let mut ok_key_headers = HeaderMap::new();
        ok_key_headers.insert("x-api-key", "secret123".parse().unwrap());
        let ok_key_res = check_handler(State(state.clone()), ok_key_headers, Json(req.clone())).await;
        assert!(ok_key_res.is_ok());
    }
}
