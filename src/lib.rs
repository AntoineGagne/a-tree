//! An implementation of the [A-Tree: A Dynamic Data Structure for Efficiently Indexing Arbitrary Boolean Expressions](https://dl.acm.org/doi/10.1145/3448016.3457266) paper.
mod ast;
mod atree;
mod lexer;
mod parser;

pub use crate::atree::ATree;
