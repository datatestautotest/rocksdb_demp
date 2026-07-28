fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 使用 vendored protoc，避免用户手动安装
    let protoc_path = protoc_bin_vendored::protoc_bin_path().map_err(|e| {
        format!("获取 vendored protoc 失败: {}", e)
    })?;
    std::env::set_var("PROTOC", protoc_path);

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/dedup.proto"], &["proto"])?;
    Ok(())
}
