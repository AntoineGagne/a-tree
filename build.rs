fn main() {
    // 1) Generate parser code with lalrpop (existing behavior)
    lalrpop::process_root().expect("lalrpop failed to process grammar files");

    // 2) Build the cxx bridge (generates C++ glue and compiles it into the Rust crate).
    //    This requires adding cxx-build to [build-dependencies] in Cargo.toml.
    cxx_build::bridge("src/ffi.rs")
        // Optionally add extra .cpp files if you add C++ helpers:
        // .file("src/cpp_helpers.cpp")
        .compile("a-tree-cxxbridge");

    // 3) Re-run directives: re-run build if these sources change.
    println!("cargo:rerun-if-changed=src/grammar.lalrpop");
    println!("cargo:rerun-if-changed=src/ffi.rs");
    // If you add other files that affect the build (lexer, headers), add them too:
    // println!("cargo:rerun-if-changed=src/lexer.rs");
}
