use crate::{
    ast::{self, *},
    error::ATreeError,
    evaluation::EvaluationResult,
    events::{
        AttributeDefinition, AttributeId, AttributeKind, AttributeTable, Event, EventBuilder,
        EventError,
    },
    parser,
    predicates::Predicate,
    strings::StringTable,
};
use bimap::BiMap;
use slab::Slab;
use std::borrow::BorrowMut;

type NodeId = usize;
type ExpressionId = u64;

/// The A-Tree data structure as described by the paper.
pub struct ATree {
    nodes: Slab<ATreeNode>,
    strings: StringTable,
    attributes: AttributeTable,
    expression_to_node: BiMap<ExpressionId, NodeId>,
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
            nodes: Slab::new(),
            expression_to_node: BiMap::new(),
        })
    }

    /// Insert an arbitrary boolean expression inside the [`ATree`].
    pub fn insert<'a, 'tree: 'a>(&'tree mut self, abe: &'a str) -> Result<NodeId, ATreeError<'a>> {
        let ast = parser::parse(abe, &self.attributes, &mut self.strings)
            .map_err(ATreeError::ParseError)?;
        self.insert_root(ast)
    }

    fn insert_root(&mut self, root: Node) -> Result<NodeId, ATreeError> {
        let rnode = match &root {
            Node::And(left, right) | Node::Or(left, right) => {
                let nodes = &mut self.nodes;
                let left_id = insert_node(nodes, &mut self.expression_to_node, &*left)?;
                let right_id = insert_node(nodes, &mut self.expression_to_node, &*right)?;
                let mut left_node = nodes[left_id].borrow_mut();
                let mut right_node = nodes[right_id].borrow_mut();
                let rnode = RNode {
                    level: 1 + std::cmp::max(left_node.level(), right_node.level()),
                    operator: if matches!(root, Node::And(_, _)) {
                        Operator::And
                    } else {
                        Operator::Or
                    },
                    children: vec![left_id, right_id],
                };
                let expression_id = rnode.id(&self.expression_to_node);
                rnode
            }
            Node::Not(child) => {
                unimplemented!();
            }
            Node::Value(value) => RNode {
                operator: Operator::Value(value.clone()),
                level: 1,
                children: vec![],
            },
        };
        let rnode = ATreeNode::RNode(rnode);
        Ok(1)
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
        Ok(Report::new(self.nodes.len()))
    }
}

fn insert_node<'atree, 'a>(
    nodes: &'atree mut Slab<ATreeNode>,
    expression_to_node: &'atree mut BiMap<ExpressionId, NodeId>,
    node: &'a Node,
) -> Result<NodeId, ATreeError<'a>> {
    match node {
        Node::And(left, right) | Node::Or(left, right) => {
            unimplemented!();
        }
        Node::Value(node) => {
            let expression_id = node.id();
            let node_id = if let Some(node_id) = expression_to_node.get_by_left(&expression_id) {
                *node_id
            } else {
                let lnode = ATreeNode::LNode(LNode {
                    level: 1,
                    parents: vec![],
                    predicate: node.clone(),
                });
                let arena_id = nodes.insert(lnode);
                // TODO: Revisit this arena_id
                expression_to_node.insert_no_overwrite(expression_id, arena_id);
                arena_id
            };
            Ok(node_id)
        }
        Node::Not(node) => {
            let child_id = insert_node(nodes, expression_to_node, node)?;
            let node = nodes[child_id].borrow_mut();
            let inode = INode {
                parents: vec![],
                level: 1 + node.level(),
                operator: Operator::Not,
                children: vec![child_id],
            };
            let id = 1;
            Ok(id)
        }
    }
}

enum ATreeNode {
    LNode(LNode),
    INode(INode),
    RNode(RNode),
}

impl ATreeNode {
    fn id(&self, expression_to_node: &BiMap<ExpressionId, NodeId>) -> u64 {
        match self {
            ATreeNode::LNode(node) => node.id(),
            ATreeNode::INode(node) => node.id(expression_to_node),
            ATreeNode::RNode(node) => node.id(expression_to_node),
        }
    }

    #[inline]
    const fn level(&self) -> usize {
        match self {
            ATreeNode::RNode(node) => node.level,
            ATreeNode::LNode(node) => node.level,
            ATreeNode::INode(node) => node.level,
        }
    }
}

struct LNode {
    parents: Vec<NodeId>,
    level: usize,
    predicate: Predicate,
}

impl LNode {
    fn id(&self) -> u64 {
        self.predicate.id()
    }
}

struct INode {
    parents: Vec<NodeId>,
    children: Vec<NodeId>,
    level: usize,
    operator: Operator,
}

impl INode {
    fn id(&self, expression_to_node: &BiMap<ExpressionId, NodeId>) -> u64 {
        match &self.operator {
            Operator::And | Operator::Or => {
                let children_ids = self.children.iter().map(|child| {
                    expression_to_node.get_by_right(child).expect(&format!(
                        "no expression ID found for child {child}; this is a bug"
                    ))
                });
                if matches!(self.operator, Operator::And) {
                    children_ids.product()
                } else {
                    children_ids.sum()
                }
            }
            Operator::Not => {
                let child_id = self
                    .children
                    .first()
                    .expect(&format!("no child ID for 'not' expression; this is a bug"));
                let child_id = expression_to_node
                    .get_by_right(child_id)
                    .expect(&format!("no expression ID for child {child_id}"));
                !child_id
            }
            Operator::Value(_) => {
                unreachable!("i-node should not be a leaf; this is a bug");
            }
        }
    }
}

struct RNode {
    children: Vec<NodeId>,
    level: usize,
    operator: Operator,
}

impl RNode {
    fn id(&self, expression_to_node: &BiMap<ExpressionId, NodeId>) -> u64 {
        match &self.operator {
            Operator::And | Operator::Or => {
                let children_ids = self.children.iter().map(|child| {
                    expression_to_node.get_by_right(child).expect(&format!(
                        "no expression ID found for child {child} in r-node; this is a bug"
                    ))
                });
                if matches!(self.operator, Operator::And) {
                    children_ids.product()
                } else {
                    children_ids.sum()
                }
            }
            Operator::Not => {
                let child_id = self.children.first().expect(&format!(
                    "no child ID in r-node for 'not' expression; this is a bug"
                ));
                let child_id = expression_to_node
                    .get_by_right(child_id)
                    .expect(&format!("no expression ID for child {child_id} in r-node"));
                !child_id
            }
            Operator::Value(predicate) => predicate.id(),
        }
    }
}

pub struct Report {
    evaluation: EvaluationResult,
}

impl Report {
    fn new(nodes: usize) -> Self {
        Self {
            evaluation: EvaluationResult::new(nodes),
        }
    }
}

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
