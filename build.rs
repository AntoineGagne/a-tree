// name=build.rs
use std::env;
use std::fs;
use std::path::Path;

fn main() {
    // 1) Remove any stale cxxbridge-generated crate that might cause lalrpop recursion.
    //    We look under <manifest_dir>/target/*/out/cxxbridge/crate/a-tree and delete it if present.
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    let target_dir = Path::new(&manifest_dir).join("target");
    if target_dir.exists() {
        if let Ok(entries) = fs::read_dir(&target_dir) {
            for entry in entries.filter_map(Result::ok) {
                let out_cxx = entry.path()
                    .join("out")
                    .join("cxxbridge")
                    .join("crate")
                    .join("a-tree");
                if out_cxx.exists() {
                    // ignore errors when removing; we don't want build.rs to panic on transient failures
                    let _ = fs::remove_dir_all(&out_cxx);
                }
            }
        }
    }

    // 2) Run lalrpop first so generated parser sources are present for the Rust compiler.
    lalrpop::process_root().expect("lalrpop failed to process grammar files");

    // 3) Then run cxx_build to generate and compile the cxx bridge sources.
    cxx_build::bridge("src/ffi.rs")
        // .file("src/some_cpp_helper.cpp") // add any extra cpp files if you add them
        .compile("a-tree-cxxbridge");

    // 4) Provide rerun-if-changed hints.
    println!("cargo:rerun-if-changed=src/grammar.lalrpop");
    println!("cargo:rerun-if-changed=src/ffi.rs");
}
