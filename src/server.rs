//! gRPC 服务端实现
//!
//! 将 DedupEngine 封装为 gRPC 服务，供多语言客户端调用。
//!
//! 对应 proto: proto/dedup.proto

use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::engine::{CheckResult, DedupEngine};
use crate::error::DedupError;

// tonic 生成的代码
pub mod pb {
    #![allow(non_snake_case)]
    tonic::include_proto!("dedup.v1");
}

pub use pb::dedup_service_server::{DedupService, DedupServiceServer};

/// gRPC 服务实现
pub struct DedupServiceImpl {
    engine: Arc<DedupEngine>,
}

impl DedupServiceImpl {
    pub fn new(engine: Arc<DedupEngine>) -> Self {
        Self { engine }
    }
}

// 将 DedupError 转换为 gRPC Status
impl From<DedupError> for Status {
    fn from(e: DedupError) -> Self {
        match e {
            DedupError::RocksDb(msg) => Status::internal(format!("RocksDB error: {}", msg)),
            DedupError::Codec(msg) => Status::internal(format!("codec error: {}", msg)),
            DedupError::Config(msg) => Status::invalid_argument(format!("config error: {}", msg)),
            DedupError::Io(msg) => Status::internal(format!("IO error: {}", msg)),
            DedupError::Shutdown => Status::unavailable("engine is shutting down"),
        }
    }
}

// 将内部 DedupValue 转换为 proto 的 DedupValue
fn to_pb_value(v: &crate::key::DedupValue) -> pb::DedupValue {
    pb::DedupValue {
        expire_at_ms: v.expire_at_ms,
        created_at_ms: v.created_at_ms,
        metadata: v.metadata.clone(),
    }
}

// 将内部 CheckResult 转换为 proto 的 CheckAndSetResponse
fn to_check_response(result: CheckResult) -> pb::CheckAndSetResponse {
    match result {
        CheckResult::Inserted => pb::CheckAndSetResponse {
            result: pb::CheckResult::Inserted as i32,
            value: None,
        },
        CheckResult::Duplicate { existing } => pb::CheckAndSetResponse {
            result: pb::CheckResult::Duplicate as i32,
            value: Some(to_pb_value(&existing)),
        },
        CheckResult::ExpiredAndReplaced { old } => pb::CheckAndSetResponse {
            result: pb::CheckResult::ExpiredAndReplaced as i32,
            value: Some(to_pb_value(&old)),
        },
    }
}

#[tonic::async_trait]
impl DedupService for DedupServiceImpl {
    async fn check_and_set(
        &self,
        request: Request<pb::CheckAndSetRequest>,
    ) -> Result<Response<pb::CheckAndSetResponse>, Status> {
        let req = request.into_inner();

        let result = self.engine.check_and_set(
            req.namespace.as_bytes(),
            &req.key,
            req.expire_at_ms,
            if req.metadata.is_empty() {
                None
            } else {
                Some(req.metadata)
            },
        )?;

        Ok(Response::new(to_check_response(result)))
    }

    async fn batch_check_and_set(
        &self,
        request: Request<pb::BatchCheckAndSetRequest>,
    ) -> Result<Response<pb::BatchCheckAndSetResponse>, Status> {
        let req = request.into_inner();

        // 构造批量输入
        let items: Vec<(&[u8], &[u8], u64, Option<Vec<u8>>)> = req
            .items
            .iter()
            .map(|item| {
                (
                    item.namespace.as_bytes(),
                    item.key.as_slice(),
                    item.expire_at_ms,
                    if item.metadata.is_empty() {
                        None
                    } else {
                        Some(item.metadata.clone())
                    },
                )
            })
            .collect();

        let results = self.engine.batch_check_and_set(&items)?;

        let responses: Vec<pb::CheckAndSetResponse> =
            results.into_iter().map(to_check_response).collect();

        Ok(Response::new(pb::BatchCheckAndSetResponse {
            results: responses,
        }))
    }

    async fn exists(
        &self,
        request: Request<pb::ExistsRequest>,
    ) -> Result<Response<pb::ExistsResponse>, Status> {
        let req = request.into_inner();
        let exists = self
            .engine
            .exists(req.namespace.as_bytes(), &req.key)?;

        Ok(Response::new(pb::ExistsResponse { exists }))
    }

    async fn batch_exists(
        &self,
        request: Request<pb::BatchExistsRequest>,
    ) -> Result<Response<pb::BatchExistsResponse>, Status> {
        let req = request.into_inner();
        let key_slices: Vec<&[u8]> = req.keys.iter().map(|k| k.as_slice()).collect();
        let exists = self
            .engine
            .batch_exists(req.namespace.as_bytes(), &key_slices)?;

        Ok(Response::new(pb::BatchExistsResponse { exists }))
    }

    async fn get(
        &self,
        request: Request<pb::GetRequest>,
    ) -> Result<Response<pb::GetResponse>, Status> {
        let req = request.into_inner();
        let value = self.engine.get(req.namespace.as_bytes(), &req.key)?;

        match value {
            Some(v) => Ok(Response::new(pb::GetResponse {
                found: true,
                value: Some(to_pb_value(&v)),
            })),
            None => Ok(Response::new(pb::GetResponse {
                found: false,
                value: None,
            })),
        }
    }

    async fn delete(
        &self,
        request: Request<pb::DeleteRequest>,
    ) -> Result<Response<pb::DeleteResponse>, Status> {
        let req = request.into_inner();
        let deleted = self
            .engine
            .delete(req.namespace.as_bytes(), &req.key)?;

        Ok(Response::new(pb::DeleteResponse { deleted }))
    }

    async fn batch_delete(
        &self,
        request: Request<pb::BatchDeleteRequest>,
    ) -> Result<Response<pb::BatchDeleteResponse>, Status> {
        let req = request.into_inner();
        let key_slices: Vec<&[u8]> = req.keys.iter().map(|k| k.as_slice()).collect();
        let deleted = self
            .engine
            .batch_delete(req.namespace.as_bytes(), &key_slices)?;

        Ok(Response::new(pb::BatchDeleteResponse {
            deleted,
        }))
    }

    async fn stats(
        &self,
        request: Request<pb::StatsRequest>,
    ) -> Result<Response<pb::StatsResponse>, Status> {
        let _req = request.into_inner();

        let bloom = self.engine.bloom_stats();
        let cleaner = self.engine.cleaner_stats();

        // 用 RocksDB 的估算 key 数（含过期的，比布隆插入量更准确）
        let total_keys = self.engine.store().estimate_total_keys();

        let resp = pb::StatsResponse {
            total_keys,
            bloom_capacity: bloom.capacity,
            bloom_insertions: bloom.inserted,
            bloom_fpp: bloom.estimated_fpr,
            cleaned_total: cleaner.total_deleted,
            backlog_estimate: cleaner.backlog_estimate,
            current_speed: cleaner.current_speed as u64,
            qps_estimate: bloom.total_checks / 60, // 粗略估算：总查询数 / 60秒
            error_count: 0,
        };

        Ok(Response::new(resp))
    }
}

/// 启动 gRPC 服务（带优雅关闭信号监听）
pub async fn serve_with_shutdown<F>(
    engine: Arc<DedupEngine>,
    addr: std::net::SocketAddr,
    signal: F,
) -> anyhow::Result<()>
where
    F: std::future::Future<Output = ()>,
{
    let svc = DedupServiceImpl::new(engine);

    println!("[grpc] 去重服务启动，监听 {}", addr);

    tonic::transport::Server::builder()
        .add_service(DedupServiceServer::new(svc))
        .serve_with_shutdown(addr, signal)
        .await?;

    Ok(())
}

/// 启动 gRPC 服务（默认持续运行，保留向后兼容）
pub async fn serve(
    engine: Arc<DedupEngine>,
    addr: std::net::SocketAddr,
) -> anyhow::Result<()> {
    serve_with_shutdown(engine, addr, std::future::pending()).await
}
