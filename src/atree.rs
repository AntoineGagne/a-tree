use crate::{
    ast::*,
    error::ATreeError,
    evaluation::EvaluationResult,
    events::{AttributeDefinition, AttributeTable, Event, EventBuilder},
    parser,
    predicates::Predicate,
    strings::StringTable,
};
use bimap::BiMap;
use slab::Slab;

type NodeId = usize;
type ExpressionId = u64;

/// The A-Tree data structure as described by the paper
///
/// See the [module documentation] for more details.
///
/// [module documentation]: index.html
#[derive(Clone, Debug)]
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
        let expression_id = root.id();
        if let Some(node_id) = self.expression_to_node.get_by_left(&expression_id) {
            update_use_count(*node_id, &mut self.nodes);
            return Ok(*node_id);
        }

        let is_and = matches!(&root, Node::And(_, _));
        let node_id = match root {
            Node::And(left, right) | Node::Or(left, right) => {
                let left_id = self.insert_node(*left)?;
                let right_id = self.insert_node(*right)?;
                let left_entry = &self.nodes[left_id];
                let right_entry = &self.nodes[right_id];
                let rnode = ATreeNode::RNode(RNode {
                    level: 1 + std::cmp::max(left_entry.node.level(), right_entry.node.level()),
                    operator: if is_and { Operator::And } else { Operator::Or },
                    children: vec![left_id, right_id],
                });
                let node_id = get_or_update(
                    &mut self.expression_to_node,
                    &mut self.nodes,
                    &expression_id,
                    rnode,
                );
                add_parent(&mut self.nodes[left_id], node_id);
                add_parent(&mut self.nodes[right_id], node_id);
                node_id
            }
            Node::Not(child) => {
                let child_id = self.insert_node(*child)?;
                let entry = &self.nodes[child_id];
                let rnode = ATreeNode::RNode(RNode {
                    level: 1 + entry.node.level(),
                    operator: Operator::Not,
                    children: vec![child_id],
                });
                let node_id = get_or_update(
                    &mut self.expression_to_node,
                    &mut self.nodes,
                    &expression_id,
                    rnode,
                );
                add_parent(&mut self.nodes[child_id], node_id);
                node_id
            }
            Node::Value(value) => {
                let rnode = ATreeNode::RNode(RNode::value(&value));
                let node_id = get_or_update(
                    &mut self.expression_to_node,
                    &mut self.nodes,
                    &expression_id,
                    rnode,
                );
                self.predicates.push(node_id);
                node_id
            }
        };
        self.roots.push(node_id);
        Ok(node_id)
    }

    fn insert_node<'a>(&mut self, node: Node) -> Result<NodeId, ATreeError<'a>> {
        let expression_id = node.id();
        if let Some(node_id) = self.expression_to_node.get_by_left(&expression_id) {
            update_use_count(*node_id, &mut self.nodes);
            return Ok(*node_id);
        }

        let is_and = matches!(node, Node::And(_, _));
        match node {
            Node::And(left, right) | Node::Or(left, right) => {
                let left_id = self.insert_node(*left)?;
                let right_id = self.insert_node(*right)?;
                let left_entry = &self.nodes[left_id];
                let right_entry = &self.nodes[right_id];
                let inode = INode {
                    parents: vec![],
                    level: 1 + std::cmp::max(left_entry.node.level(), right_entry.node.level()),
                    operator: if is_and { Operator::And } else { Operator::Or },
                    children: vec![left_id, right_id],
                };
                let inode = ATreeNode::INode(inode);
                let node_id = get_or_update(
                    &mut self.expression_to_node,
                    &mut self.nodes,
                    &expression_id,
                    inode,
                );
                add_parent(&mut self.nodes[left_id], node_id);
                add_parent(&mut self.nodes[right_id], node_id);
                Ok(node_id)
            }
            Node::Not(node) => {
                let child_id = self.insert_node(*node)?;
                let entry = &self.nodes[child_id];
                let inode = ATreeNode::INode(INode {
                    parents: vec![],
                    level: 1 + entry.node.level(),
                    operator: Operator::Not,
                    children: vec![child_id],
                });
                let node_id = get_or_update(
                    &mut self.expression_to_node,
                    &mut self.nodes,
                    &expression_id,
                    inode,
                );
                add_parent(&mut self.nodes[child_id], node_id);
                Ok(node_id)
            }
            Node::Value(node) => {
                let lnode = ATreeNode::lnode(&node);
                let node_id = get_or_update(
                    &mut self.expression_to_node,
                    &mut self.nodes,
                    &expression_id,
                    lnode,
                );
                self.predicates.push(node_id);
                Ok(node_id)
            }
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

#[inline]
fn get_or_update(
    expression_to_node: &mut BiMap<ExpressionId, NodeId>,
    nodes: &mut Slab<Entry>,
    expression_id: &ExpressionId,
    node: ATreeNode,
) -> NodeId {
    if let Some(node_id) = expression_to_node.get_by_left(expression_id).cloned() {
        let entry = &mut nodes[node_id];
        entry.use_count += 1;
        node_id
    } else {
        let entry = Entry::new(*expression_id, node);
        let node_id = nodes.insert(entry);
        expression_to_node
            .insert_no_overwrite(*expression_id, node_id)
            .unwrap_or_else(|_| panic!("{expression_id} is already present; this is a bug"));
        node_id
    }
}

#[inline]
fn add_parent(entry: &mut Entry, node_id: NodeId) {
    entry.node.add_parent(node_id);
}

#[inline]
fn update_use_count(node_id: NodeId, nodes: &mut Slab<Entry>) {
    nodes[node_id].use_count += 1;
}

#[derive(Clone, Debug)]
struct Entry {
    id: ExpressionId,
    node: ATreeNode,
    use_count: usize,
}

impl Entry {
    #[inline]
    fn new(id: ExpressionId, node: ATreeNode) -> Self {
        Self {
            id,
            node,
            use_count: 1,
        }
    }
}

#[derive(Clone, Debug)]
#[allow(clippy::enum_variant_names)]
enum ATreeNode {
    LNode(LNode),
    INode(INode),
    RNode(RNode),
}

impl ATreeNode {
    #[inline]
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
        match self {
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

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
struct INode {
    parents: Vec<NodeId>,
    children: Vec<NodeId>,
    level: usize,
    operator: Operator,
}

#[derive(Clone, Debug)]
struct RNode {
    children: Vec<NodeId>,
    level: usize,
    operator: Operator,
}

impl RNode {
    #[inline]
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
    const AN_EXPRESSION: &str = "exchange_id = 1";
    const A_NOT_EXPRESSION: &str = "not private";
    const AN_EXPRESSION_WITH_AND_OPERATORS: &str =
        r#"exchange_id = 1 and deals one of ["deal-1", "deal-2"]"#;
    const AN_EXPRESSION_WITH_OR_OPERATORS: &str =
        r#"exchange_id = 1 and deals one of ["deal-1", "deal-2"]"#;
    const A_COMPLEX_EXPRESSION: &str = r#"exchange_id = 1 and not private and deal_ids one of ["deal-1", "deal-2"] and segment_ids one of [1, 2, 3] and country = 'CA' and city in ['QC'] or country = 'US' and city in ['AZ']"#;
    const ANOTHER_COMPLEX_EXPRESSION: &str = r#"exchange_id = 1 and not private and deal_ids one of ["deal-1", "deal-2"] and segment_ids one of [1, 2, 3] and country in ['FR', 'GB']"#;

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

    #[test]
    fn return_an_error_on_empty_boolean_expression() {
        let definitions = [
            AttributeDefinition::boolean("private"),
            AttributeDefinition::string("country"),
            AttributeDefinition::string_list("deals"),
            AttributeDefinition::integer("exchange_id"),
            AttributeDefinition::integer_list("segment_ids"),
        ];
        let mut atree = ATree::new(&definitions).unwrap();

        let result = atree.insert("");

        assert!(result.is_err());
    }

    #[test]
    fn can_insert_a_simple_expression() {
        let definitions = [
            AttributeDefinition::boolean("private"),
            AttributeDefinition::string("country"),
            AttributeDefinition::string_list("deals"),
            AttributeDefinition::integer("exchange_id"),
            AttributeDefinition::integer_list("segment_ids"),
        ];
        let mut atree = ATree::new(&definitions).unwrap();

        let result = atree.insert(AN_EXPRESSION);

        assert!(result.is_ok());
    }

    #[test]
    fn can_insert_a_negative_expression() {
        let definitions = [
            AttributeDefinition::boolean("private"),
            AttributeDefinition::string("country"),
            AttributeDefinition::string_list("deals"),
            AttributeDefinition::integer("exchange_id"),
            AttributeDefinition::integer_list("segment_ids"),
        ];
        let mut atree = ATree::new(&definitions).unwrap();

        let result = atree.insert(A_NOT_EXPRESSION);

        assert!(result.is_ok());
    }

    #[test]
    fn can_insert_an_expression_with_and_operators() {
        let definitions = [
            AttributeDefinition::boolean("private"),
            AttributeDefinition::string("country"),
            AttributeDefinition::string_list("deals"),
            AttributeDefinition::integer("exchange_id"),
            AttributeDefinition::integer_list("segment_ids"),
        ];
        let mut atree = ATree::new(&definitions).unwrap();

        let result = atree.insert(AN_EXPRESSION_WITH_AND_OPERATORS);

        assert!(result.is_ok());
    }

    #[test]
    fn can_insert_an_expression_with_or_operators() {
        let definitions = [
            AttributeDefinition::boolean("private"),
            AttributeDefinition::string("country"),
            AttributeDefinition::string_list("deals"),
            AttributeDefinition::integer("exchange_id"),
            AttributeDefinition::integer_list("segment_ids"),
        ];
        let mut atree = ATree::new(&definitions).unwrap();

        let result = atree.insert(AN_EXPRESSION_WITH_OR_OPERATORS);

        assert!(result.is_ok());
    }

    #[test]
    fn can_insert_an_expression_with_mixed_operators() {
        let definitions = [
            AttributeDefinition::boolean("private"),
            AttributeDefinition::integer("exchange_id"),
            AttributeDefinition::string_list("deal_ids"),
            AttributeDefinition::integer_list("segment_ids"),
            AttributeDefinition::string("country"),
            AttributeDefinition::string("city"),
        ];
        let mut atree = ATree::new(&definitions).unwrap();

        let result = atree.insert(A_COMPLEX_EXPRESSION);

        assert!(result.is_ok());
    }

    #[test]
    fn can_insert_multiple_expressions_with_mixed_operators() {
        let definitions = [
            AttributeDefinition::boolean("private"),
            AttributeDefinition::integer("exchange_id"),
            AttributeDefinition::string_list("deal_ids"),
            AttributeDefinition::integer_list("segment_ids"),
            AttributeDefinition::string("country"),
            AttributeDefinition::string("city"),
        ];
        let mut atree = ATree::new(&definitions).unwrap();

        assert!(atree.insert(A_COMPLEX_EXPRESSION).is_ok());
        assert!(atree.insert(ANOTHER_COMPLEX_EXPRESSION).is_ok());
    }
}
