//! An implementation of the [A-Tree: A Dynamic Data Structure for Efficiently Indexing Arbitrary Boolean Expressions](https://dl.acm.org/doi/10.1145/3448016.3457266) paper.
//!
//! # Examples
//!
//! Searching for some boolean expressions:
//!
//! ```
//! use a_tree::{ATree, AttributeDefinition};
//! use std::collections::HashMap;
//!
//! // Create the A-Tree
//! let mut atree = ATree::new(&[
//!     AttributeDefinition::string_list("deal_ids"),
//!     AttributeDefinition::integer("exchange_id"),
//!     AttributeDefinition::boolean("debug"),
//!     AttributeDefinition::integer_list("segment_ids"),
//! ]).unwrap();
//!
//! let expression_1 = r#"deal_ids one of ["deal-1", "deal-2"]"#;
//! let expression_2 = r#"segment_ids one of [1, 2, 3, 4]"#;
//! // Insert the arbitrary boolean expressions
//! let expressions_by_ids = vec![
//!     (1u64, expression_1),
//!     (2u64, expression_2)
//! ];
//! let mut mappings: HashMap<u64, &str> = HashMap::new();
//! for (id, expression) in &expressions_by_ids {
//!     atree.insert(id, expression).unwrap();
//!     mappings.insert(*id, expression);
//! }
//!
//! // Create an event
//! let mut builder = atree.make_event();
//! builder.with_string_list("deal_ids", &["deal-2"]).unwrap();
//! builder.with_integer_list("segment_ids", &[1, 2]).unwrap();
//! builder.with_boolean("debug", false).unwrap();
//! let event = builder.build().unwrap();
//!
//! // Search for matching boolean expressions
//! let report = atree.search(event).unwrap();
//! report.matches().iter().for_each(|id| {
//!     println!(r#"Found ID: {id}, Expression: "{}""#, mappings[id]);
//! });
//! ```
//!
//! # Domain Specific Language (DSL)
//!
//! The A-Tree crate support a DSL to allow easy creation of arbitrary boolean expressions (ABE).
//! The following operators are supported:
//!
//! * Boolean operators: `and` (`&&`), `or` (`||`), `not` (`!`) and `variable` where `variable` is a defined attribute for the A-Tree;
//! * Comparison: `<`, `<=`, `>`, `>=`. They work for `integer` and `float`;
//! * Equality: `=` and `<>`. They work for `integer`, `float` and `string`;
//! * Null: `is null`, `is not null` (for variables), `is empty` and `is not empty` (for lists);
//! * Set: `in` and `not in`. They work for list of `integer` or for list of `string`;
//! * List: `one of`, `none of` and `all of`. They work for list of `integer` and list of `string`.
//!
//! As an example, the following would all be valid ABEs:
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
//!   sub-expression `(B ∧ C)` and will make both expression refer to the common node);
//! * Convert the strings to IDs to accelerate comparison and search;
//! * Sort the lists of strings/integers and remove duplicates;
//! * Sort the sub-expressions by cost:
//!     * variable substitution/null checks/empty checks < set operations < lists operations
//!     * the length of the lists has an impact on that cost too for set operations and lists
//!       operations;
//!     * the cost of binary boolean operators (OR and AND) are the combined cost of their
//!       sub-expressions;
//! * Evaluate the predicates lazily while searching;
//! * _Zero suppression filter_: Reduce the amount of nodes to evaluate by applying
//!   De Morgan's laws and eliminating the NOT nodes;
//! * _Propagation on demand_: Choose an access child for the AND operators and only
//!   propagate the result if the access child is true.
mod ast;
mod atree;
mod error;
mod evaluation;
mod events;
mod lexer;
mod parser;
mod predicates;
mod strings;
#[cfg(test)]
mod test_utils;

pub use crate::{
    atree::{ATree, Report},
    error::ATreeError,
    events::{AttributeDefinition, Event, EventBuilder, EventError},
};
