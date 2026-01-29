fn main() {
    // Run lalrpop to generate parser sources
    lalrpop::process_root().expect("lalrpop failed to process grammar files");

    // Provide rerun-if-changed hint
    println!("cargo:rerun-if-changed=src/grammar.lalrpop");
}
