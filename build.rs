//! Compiles `proto/*.proto` into Rust modules under `$OUT_DIR`.
//!
//! Output is included via `tonic::include_proto!("cash.z.wallet.sdk.rpc")`
//! in `src/grpc/lightwalletd.rs`. No generated code is committed to the
//! repo; each `cargo build` regenerates from the vendored `.proto`
//! files.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos = ["proto/compact_formats.proto", "proto/service.proto"];

    // Re-run on proto changes — Cargo doesn't trace these by default
    // because they're not Rust source.
    for p in &protos {
        println!("cargo:rerun-if-changed={p}");
    }

    // Point tonic-build at the vendored protoc so dev machines / CI
    // runners don't need a system protoc install.
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    // SAFETY: build.rs is single-threaded before tonic_build forks
    // protoc; setting an env var here is the standard pattern.
    unsafe {
        std::env::set_var("PROTOC", protoc);
    }

    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .compile_protos(&protos, &["proto"])?;
    Ok(())
}
