use std::env;
use std::path::PathBuf;

fn main() {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let output_file = PathBuf::from(&crate_dir).join("atree.h");

    cbindgen::Builder::new()
        .with_crate(crate_dir)
        .with_language(cbindgen::Language::C)
        .with_documentation(true)
        .with_include_guard("ATREE_H")
        .generate()
        .expect("Unable to generate C bindings")
        .write_to_file(output_file);
}
