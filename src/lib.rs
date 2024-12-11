//! An implementation of the [A-Tree: A Dynamic Data Structure for Efficiently Indexing Arbitrary Boolean Expressions](https://dl.acm.org/doi/10.1145/3448016.3457266) paper.
//!
//! # Domain Specific Language (DSL)
//!
//! The A-Tree crate support a DSL to allow easy creation of arbitrary boolean expressions.
//! The following operators are supported:
//!
//! * Boolean operators: `and` (`&&`), `or` (`||`), `not` (`!`) and `variable` where `variable` is a defined attribute for the A-Tree;
//! * Comparison: `<`, `<=`, `>`, `>=`. They work for `integer` and `float`;
//! * Equality: `=` and `<>`. They work for `integer`, `float` and `string`;
//! * Null: `is null`, `is not null` (for variables) and `is empty` (for lists);
//! * Set: `in` and `not in`. They work for list of `integer` or for list of `string`;
//! * List: `one of`, `none of` and `all of`. They work for list of `integer` and list of `string`.
//!
//! As an example, the following would all be valid arbitrary boolean expressions (ABE):
//!
//! ```text
//! (exchange_id = 1 and deals one of ["deal-1", "deal-2", "deal-3"]) and (segment_ids one of [1, 2, 3]) and (continent = 'NA' and country in ["US", "CA"])
//! (log_level = 'debug') and (month in [1, 2, 3] and day in [15, 16]) or (month in [4, 5, 6] and day in [10, 11])
//! ```
//!
//! # Optimizations
//!
//! The A-Tree is a data structure that can efficiently search a large amount of arbitrary boolean
//! expressions for ones that match a specific event. To achieve this, there are a certain amount
//! of things that the A-Tree will do:
//!
//! * Search for duplicated intermediary boolean expressions nodes (i.e. if there are two
//!   expressions such as `(A ∧ (B ∧ C))` and `(D ∨ (B ∧ C))`, the tree will find the common
//!   sub-expression `(B ∧ C)` and will make both expression refer to the common node).
//! * Convert the strings to IDs to accelerate comparison and search;
//! * Sort the lists and remove duplicates.
//!
mod ast;
mod atree;
mod error;
mod evaluation;
mod events;
mod lexer;
mod parser;
mod predicates;
mod strings;

pub use crate::{
    atree::{ATree, Report},
    error::ATreeError,
    events::{AttributeDefinition, Event, EventBuilder, EventError},
};
