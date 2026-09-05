fn main() {
    // the window icon is embedded at compile time; make icon edits rebuild
    println!("cargo:rerun-if-changed=icons/icon.png");
    tauri_build::build();

    let mut c = prost_build::Config::new();
    c.compile_protos(&["../../proto/XStore.proto"], &["../../proto/"])
        .expect("failed to compile XStore.proto");
    println!("cargo:rerun-if-changed=../../proto/XStore.proto");

    // #[path] requires a literal, so copy the prost output into src/.
    // prost names the file from the package: XStore_Proto -> x_store_proto.rs.
    let out = std::path::PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR unset"));
    std::fs::copy(out.join("x_store_proto.rs"), "src/proto_gen.rs")
        .expect("failed to copy generated proto");
}
