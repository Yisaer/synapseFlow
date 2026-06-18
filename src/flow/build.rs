fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Generate kura (yoriito VISS) gRPC client from proto definitions.
    // We only build the client stubs — the server side is not needed.
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(false)
        .compile_protos(&["proto/yoriito/viss/v1/producer.proto"], &["proto"])?;

    // Generate test-only proto message for decoder/integration tests.
    prost_build::Config::new()
        .out_dir(std::env::var("OUT_DIR").unwrap())
        .compile_protos(&["proto/test/simple.proto"], &["proto/test"])?;
    Ok(())
}
