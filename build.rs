fn main() -> Result<(), Box<dyn std::error::Error>> {
    // docs.rs doesn't have protoc installed. When the `docs-rs` feature is
    // enabled (set in [package.metadata.docs.rs]), compile protoc from source
    // via protobuf-src and point tonic-build at it.
    #[cfg(feature = "docs-rs")]
    std::env::set_var("PROTOC", protobuf_src::protoc());

    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(&["proto/skiff.proto"], &["proto"])?;

    Ok(())
}
