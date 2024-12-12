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

/// The A-Tree data structure as described by the paper
///
/// See the [module documentation] for more details.
///
/// [module documentation]: index.html
#[derive(Debug)]
pub struct ATree {
    nodes: Slab<Entry>,
    strings: StringTable,
    attributes: AttributeTable,
    roots: Vec<NodeId>,
    predicates: Vec<NodeId>,
    expression_to_node: BiMap<ExpressionId, NodeId>,
}

impl ATree {
    const DEFAULT_PREDICATES: usize = 1000;
    const DEFAULT_NODES: usize = 2000;
    const DEFAULT_ROOTS: usize = 50;

    /// Create a new [`ATree`] with the attributes that can be used by the inserted arbitrary
    /// boolean expressions along with their types.
    ///
    /// # Examples
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
    ///
    /// Duplicate attributes are not allowed and the [`ATree::new()`] function will return an error if there are some:
    ///
    /// ```rust
    /// use a_tree::{ATree, AttributeDefinition};
    ///
    /// let definitions = [
    ///     AttributeDefinition::boolean("private"),
    ///     AttributeDefinition::boolean("private"),
    /// ];
    /// let result = ATree::new(&definitions);
    /// assert!(result.is_err());
    /// ```
    pub fn new(definitions: &[AttributeDefinition]) -> Result<Self, ATreeError> {
        let attributes = AttributeTable::new(definitions).map_err(ATreeError::Event)?;
        let strings = StringTable::new();
        Ok(Self {
            attributes,
            strings,
            roots: Vec::with_capacity(Self::DEFAULT_ROOTS),
            predicates: Vec::with_capacity(Self::DEFAULT_PREDICATES),
            nodes: Slab::with_capacity(Self::DEFAULT_NODES),
            expression_to_node: BiMap::new(),
        })
    }

    /// Insert an arbitrary boolean expression inside the [`ATree`].
    ///
    /// # Examples
    ///
    /// ```rust
    /// use a_tree::{ATree, AttributeDefinition};
    ///
    /// let definitions = [
    ///     AttributeDefinition::boolean("private"),
    ///     AttributeDefinition::integer("exchange_id")
    /// ];
    /// let mut atree = ATree::new(&definitions).unwrap();
    /// assert!(atree.insert("exchange_id = 5").is_ok());
    /// assert!(atree.insert("private").is_ok());
    /// ```
    pub fn insert<'a>(&'a mut self, abe: &'a str) -> Result<NodeId, ATreeError<'a>> {
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
                let (left_entry, right_entry) =
                    unsafe { self.nodes.get2_unchecked_mut(left_id, right_id) };
                let rnode = ATreeNode::RNode(RNode {
                    level: 1 + std::cmp::max(left_entry.node.level(), right_entry.node.level()),
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
                let entry = unsafe { self.nodes.get_unchecked_mut(child_id) };
                let rnode = ATreeNode::RNode(RNode {
                    level: 1 + entry.node.level(),
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
        self.roots.push(node_id);
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
                let (left_entry, right_entry) =
                    unsafe { self.nodes.get2_unchecked_mut(left_id, right_id) };
                let inode = INode {
                    parents: vec![],
                    level: 1 + std::cmp::max(left_entry.node.level(), right_entry.node.level()),
                    operator,
                    children: vec![left_id, right_id],
                };
                let expression_id = inode.id(&self.expression_to_node);
                let inode = ATreeNode::INode(inode);
                let node_id = self.get_or_update(&expression_id, inode);
                // FIXME: Need to update parents
                Ok(node_id)
            }
            Node::Not(node) => {
                let child_id = self.insert_node(*node)?;
                let entry = self.nodes[child_id].borrow_mut();
                let inode = ATreeNode::INode(INode {
                    parents: vec![],
                    level: 1 + entry.node.level(),
                    operator: Operator::Not,
                    children: vec![child_id],
                });
                let expression_id = inode.id(&self.expression_to_node);
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
        }
    }

    fn get_or_update(&mut self, expression_id: &ExpressionId, node: ATreeNode) -> NodeId {
        if let Some(node_id) = self.expression_to_node.get_by_left(expression_id).cloned() {
            let entry = self.nodes[node_id].borrow_mut();
            entry.use_count += 1;
            node_id
        } else {
            let entry = Entry::new(*expression_id, node);
            let node_id = self.nodes.insert(entry);
            self.expression_to_node
                .insert_no_overwrite(*expression_id, node_id)
                .unwrap_or_else(|_| panic!("{expression_id} is already present; this is a bug"));
            node_id
        }
    }

    /// Create a new [`EventBuilder`] to be able to generate an [`Event`] that will be usable for
    /// finding the matching arbitrary boolean expressions inside the [`ATree`] via the
    /// [`ATree::search()`] function.
    pub fn make_event(&self) -> EventBuilder {
        EventBuilder::new(&self.attributes, &self.strings)
    }

    /// Search the [`ATree`] for arbitrary boolean expressions that match the [`Event`].
    pub fn search(&self, _event: Event) -> Result<Report, ATreeError> {
        Ok(Report::new(self.nodes.len()))
    }
}

#[derive(Debug)]
struct Entry {
    id: ExpressionId,
    node: ATreeNode,
    use_count: usize,
}

impl Entry {
    fn new(id: ExpressionId, node: ATreeNode) -> Self {
        Self {
            id,
            node,
            use_count: 1,
        }
    }
}

#[derive(Debug)]
#[allow(clippy::enum_variant_names)]
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
    fn add_parent(&mut self, parent_id: NodeId) {
        match self.borrow_mut() {
            ATreeNode::INode(node) => {
                node.parents.push(parent_id);
            }
            ATreeNode::LNode(node) => {
                node.parents.push(parent_id);
            }
            ATreeNode::RNode(node) => {
                unreachable!("trying to insert parents to r-node {node:?} which cannot have any parents; this is a bug");
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
                    // FIXME: Even though this is what the paper says, this might easily overflows
                    // if the numbers start to become high leading to a crash. Furthermore, there
                    // is no collision guarantees for this. A better way might be to use a hashing
                    // mechanism that is prefix independent
                    //
                    // For example, 5 * 3 is equal to 2 * 6 + 3 which means that an expression
                    // such as (A ∧ B) and (C ∧ D ∨ E) might yield the same ID. This might be
                    // problematic since the A-Tree is all about sharing the nodes but this might
                    // not correct because of the collision (i.e. (A ∧ B) ≢ (C ∧ D ∨ E)).
                    children_ids.fold(1, |acc, x| acc.wrapping_mul(*x))
                } else {
                    children_ids.fold(0, |acc, x| acc.wrapping_add(*x))
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
                    children_ids.fold(1, |acc, x| acc.wrapping_mul(*x))
                } else {
                    children_ids.fold(0, |acc, x| acc.wrapping_add(*x))
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

#[derive(Debug)]
pub struct Report {
    evaluation: EvaluationResult,
}

impl Report {
    #[inline]
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
