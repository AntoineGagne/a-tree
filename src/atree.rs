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
    roots: Vec<NodeId>,
    predicates: Vec<NodeId>,
    expression_to_node: BiMap<ExpressionId, NodeId>,
}

impl ATree {
    const DEFAULT_PREDICATES: usize = 200;
    const DEFAULT_ROOTS: usize = 50;

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
            roots: Vec::with_capacity(Self::DEFAULT_ROOTS),
            predicates: Vec::with_capacity(Self::DEFAULT_PREDICATES),
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
        let is_and = matches!(&root, Node::And(_, _));
        let node_id = match root {
            Node::And(left, right) | Node::Or(left, right) => {
                let left_id = self.insert_node(*left)?;
                let right_id = self.insert_node(*right)?;
                let (left_node, right_node) =
                    unsafe { self.nodes.get2_unchecked_mut(left_id, right_id) };
                let rnode = ATreeNode::RNode(RNode {
                    level: 1 + std::cmp::max(left_node.level(), right_node.level()),
                    operator: if is_and { Operator::And } else { Operator::Or },
                    children: vec![left_id, right_id],
                });
                let expression_id = rnode.id(&self.expression_to_node);
                // FIXME: Need to update parents
                // let node_id = self.get_or_update(&expression_id, rnode);
                // left_node.set_parents(node_id);
                // right_node.set_parents(node_id);
                // node_id
                self.get_or_update(&expression_id, rnode)
            }
            Node::Not(child) => {
                let child_id = self.insert_node(*child)?;
                let node = unsafe { self.nodes.get_unchecked_mut(child_id) };
                let rnode = ATreeNode::RNode(RNode {
                    level: 1 + node.level(),
                    operator: Operator::Not,
                    children: vec![child_id],
                });
                let expression_id = rnode.id(&self.expression_to_node);
                // FIXME: Need to update parents
                // let node_id = self.get_or_update(&expression_id, rnode);
                // node.set_parents(node_id);
                // node_id
                self.get_or_update(&expression_id, rnode)
            }
            Node::Value(value) => {
                let rnode = ATreeNode::RNode(RNode::value(&value));
                let expression_id = rnode.id(&self.expression_to_node);
                self.get_or_update(&expression_id, rnode)
            }
        };
        Ok(node_id)
    }

    fn insert_node<'a>(&mut self, node: Node) -> Result<NodeId, ATreeError<'a>> {
        let operator = if matches!(node, Node::And(_, _)) {
            Operator::And
        } else {
            Operator::Or
        };
        match node {
            Node::And(left, right) | Node::Or(left, right) => {
                let left_id = self.insert_node(*left)?;
                let right_id = self.insert_node(*right)?;
                let (left_node, right_node) =
                    unsafe { self.nodes.get2_unchecked_mut(left_id, right_id) };
                let inode = INode {
                    parents: vec![],
                    level: 1 + std::cmp::max(left_node.level(), right_node.level()),
                    operator,
                    children: vec![left_id, right_id],
                };
                let expression_id = inode.id(&self.expression_to_node);
                let inode = ATreeNode::INode(inode);
                let node_id = self.get_or_update(&expression_id, inode);
                // FIXME: Need to update parents
                Ok(node_id)
            }
            Node::Value(node) => {
                let expression_id = node.id();
                let lnode = ATreeNode::lnode(&node);
                let node_id = self.get_or_update(&expression_id, lnode);
                Ok(node_id)
            }
            Node::Not(node) => {
                let child_id = self.insert_node(*node)?;
                let node = self.nodes[child_id].borrow_mut();
                let inode = ATreeNode::INode(INode {
                    parents: vec![],
                    level: 1 + node.level(),
                    operator: Operator::Not,
                    children: vec![child_id],
                });
                let expression_id = inode.id(&self.expression_to_node);
                let node_id = self.get_or_update(&expression_id, inode);
                // FIXME: Need to update parents
                Ok(node_id)
            }
        }
    }

    fn get_or_update(&mut self, expression_id: &ExpressionId, node: ATreeNode) -> NodeId {
        self.expression_to_node
            .get_by_left(expression_id)
            .cloned()
            .unwrap_or_else(|| {
                let node_id = self.nodes.insert(node);
                self.expression_to_node
                    .insert_no_overwrite(*expression_id, node_id)
                    .unwrap_or_else(|_| {
                        panic!("{expression_id} is already present; this is a bug")
                    });
                node_id
            })
    }

    /// Create a new [`EventBuilder`] to be able to generate an [`Event`] that will be usable for
    /// finding the matching arbitrary boolean expressions inside the [`ATree`] via the [`ATree::search()`] function.
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

#[derive(Debug)]
enum ATreeNode {
    LNode(LNode),
    INode(INode),
    RNode(RNode),
}

impl ATreeNode {
    #[inline]
    fn id(&self, expression_to_node: &BiMap<ExpressionId, NodeId>) -> u64 {
        match self {
            ATreeNode::LNode(node) => node.id(),
            ATreeNode::INode(node) => node.id(expression_to_node),
            ATreeNode::RNode(node) => node.id(expression_to_node),
        }
    }

    fn lnode(predicate: &Predicate) -> Self {
        ATreeNode::LNode(LNode {
            level: 1,
            parents: vec![],
            predicate: predicate.clone(),
        })
    }

    #[inline]
    const fn level(&self) -> usize {
        match self {
            ATreeNode::RNode(node) => node.level,
            ATreeNode::LNode(node) => node.level,
            ATreeNode::INode(node) => node.level,
        }
    }

    #[inline]
    fn set_parents(&mut self, parent_id: NodeId) {
        match self.borrow_mut() {
            ATreeNode::INode(node) => {
                node.parents.push(parent_id);
            }
            ATreeNode::RNode(node) => {
                unreachable!("trying to insert parents to r-node {node:?} which cannot have any parents; this is a bug");
            }
            ATreeNode::LNode(node) => {
                node.parents.push(parent_id);
            }
        }
    }
}

#[derive(Debug)]
struct LNode {
    parents: Vec<NodeId>,
    level: usize,
    predicate: Predicate,
}

impl LNode {
    #[inline]
    fn id(&self) -> u64 {
        self.predicate.id()
    }
}

#[derive(Debug)]
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

#[derive(Debug)]
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

    fn value(predicate: &Predicate) -> Self {
        RNode {
            operator: Operator::Value(predicate.clone()),
            level: 1,
            children: vec![],
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
