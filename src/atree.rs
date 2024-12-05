use crate::{
    ast::{self, *},
    error::ATreeError,
    events::{
        AttributeDefinition, AttributeIndex, AttributeKind, AttributeTable, Event, EventBuilder,
        EventError,
    },
    parser,
    predicates::Predicate,
    strings::StringTable,
};
use std::collections::{hash_map::RandomState, HashMap};

/// The A-Tree data structure as described by the paper.
pub struct ATree {
    nodes: Vec<ATreeNode>,
    strings: StringTable,
    attributes: AttributeTable,
    expression_to_node: HashMap<usize, ATreeNode>,
}

impl ATree {
    /// Create a new [`ATree`] with the specified attribute definitions.
    ///
    /// ```rust
    /// use a_tree::{ATree, AttributeDefinition};
    ///
    /// let definitions = [
    ///     AttributeDefinition::boolean("private"),
    ///     AttributeDefinition::integer("exchange_id")
    /// ];
    /// let result = ATree::new(&definitions);
    /// assert!(result.is_ok());
    /// ```
    pub fn new(definitions: &[AttributeDefinition]) -> Result<Self, ATreeError> {
        let attributes = AttributeTable::new(definitions).map_err(ATreeError::Event)?;
        let strings = StringTable::new();
        Ok(Self {
            attributes,
            strings,
            nodes: vec![],
            expression_to_node: HashMap::default(),
        })
    }

    /// Insert an arbitrary boolean expression inside the [`ATree`].
    pub fn insert<'a, 'tree: 'a>(&'tree mut self, abe: &'a str) -> Result<usize, ATreeError<'a>> {
        let ast = parser::parse(abe, &self.attributes, &mut self.strings)
            .map_err(ATreeError::ParseError)?;
        self.insert_root(ast)
    }

    fn insert_root(&mut self, root: Node) -> Result<usize, ATreeError> {
        let rnode = match root {
            Node::And(left, right) | Node::Or(left, right) => {
                unimplemented!();
            }
            Node::Not(child) => {
                unimplemented!();
            }
            Node::Value(value) => RNode {
                operator: Operator::Value,
                level: 2,
                children: vec![],
            },
        };
        let rnode = ATreeNode::RNode(rnode);
        Ok(1)
    }

    fn insert_node(&mut self, node: &Node) -> Result<usize, ATreeError> {
        unimplemented!();
    }

    /// Create a new [`EventBuilder`] to be able to generate an [`Event`] that will be usable for
    /// finding the matching ABEs inside the [`ATree`] via the [`ATree::search()`] function.
    ///
    /// By default, the non-assigned attributes will be undefined.
    ///
    /// ```rust
    /// use a_tree::{ATree, AttributeDefinition};
    ///
    /// let definitions = [
    ///     AttributeDefinition::boolean("private"),
    ///     AttributeDefinition::integer("exchange_id")
    /// ];
    /// let atree = ATree::new(&definitions).unwrap();
    ///
    /// let mut builder = atree.make_event();
    /// assert!(builder.with_integer("exchange_id", 1).is_ok());
    /// assert!(builder.with_boolean("private", false).is_ok());
    /// ```
    pub fn make_event(&self) -> EventBuilder {
        EventBuilder::new(&self.attributes, &self.strings)
    }

    /// Search the [`ATree`] for arbitrary boolean expressions that match the [`Event`].
    pub fn search(&self, _event: Event) -> Result<Report, ATreeError> {
        Ok(Report)
    }
}

enum ATreeNode {
    LNode(LNode),
    INode(INode),
    RNode(RNode),
}

impl ATreeNode {
    const fn level(&self) -> usize {
        match self {
            ATreeNode::RNode(node) => node.level,
            ATreeNode::LNode(node) => node.level,
            ATreeNode::INode(node) => node.level,
        }
    }
}

struct LNode {
    parents: Vec<usize>,
    level: usize,
    predicate: Predicate,
}

struct INode {
    parents: Vec<usize>,
    children: Vec<usize>,
    level: usize,
    operator: Operator,
}

struct RNode {
    children: Vec<usize>,
    level: usize,
    operator: Operator,
}

pub enum Operator {
    And,
    Or,
    Not,
    Value,
}

pub struct Report;

#[cfg(test)]
mod tests {
    use super::*;

    const AN_INVALID_BOOLEAN_EXPRESSION: &str = "invalid in (1, 2, 3 and";

    #[test]
    fn can_build_an_atree() {
        let definitions = [
            AttributeDefinition::boolean("private"),
            AttributeDefinition::string_list("deals"),
            AttributeDefinition::integer("exchange_id"),
            AttributeDefinition::float("bidfloor"),
            AttributeDefinition::string("country"),
            AttributeDefinition::integer_list("segment_ids"),
        ];

        let result = ATree::new(&definitions);

        assert!(result.is_ok());
    }

    #[test]
    fn return_an_error_on_duplicate_definitions() {
        let definitions = [
            AttributeDefinition::boolean("private"),
            AttributeDefinition::string("country"),
            AttributeDefinition::string_list("deals"),
            AttributeDefinition::integer("exchange_id"),
            AttributeDefinition::float("bidfloor"),
            AttributeDefinition::integer("country"),
            AttributeDefinition::integer_list("segment_ids"),
        ];

        let result = ATree::new(&definitions);

        assert!(result.is_err());
    }

    #[test]
    fn return_an_error_on_invalid_boolean_expression() {
        let definitions = [
            AttributeDefinition::boolean("private"),
            AttributeDefinition::string("country"),
            AttributeDefinition::string_list("deals"),
            AttributeDefinition::integer("exchange_id"),
            AttributeDefinition::integer_list("segment_ids"),
        ];
        let mut atree = ATree::new(&definitions).unwrap();

        let result = atree.insert(AN_INVALID_BOOLEAN_EXPRESSION);

        assert!(result.is_err());
    }
}
