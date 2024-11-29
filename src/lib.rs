//! An implementation of the [A-Tree: A Dynamic Data Structure for Efficiently Indexing Arbitrary Boolean Expressions](https://dl.acm.org/doi/10.1145/3448016.3457266) paper.
mod ast;
mod atree;
mod error;
mod events;
mod lexer;
mod parser;
mod predicates;
mod strings;

pub use crate::{
    atree::ATree,
    error::ATreeError,
    events::{AttributeDefinition, Event, EventBuilder, EventError},
};
