use crate::predicates::Predicate;
use std::hash::{DefaultHasher, Hash, Hasher};

pub type TreeNode = Box<Node>;

#[derive(PartialEq, Clone, Debug)]
pub enum Node {
    And(TreeNode, TreeNode),
    Or(TreeNode, TreeNode),
    Not(TreeNode),
    Value(Predicate),
}

#[derive(PartialEq, Clone, Debug)]
pub enum OptimizedNode {
    And(Vec<OptimizedNode>),
    Or(Vec<OptimizedNode>),
    Value(Predicate),
}

#[derive(Debug, Hash, Clone, Eq, PartialEq)]
pub enum Operator {
    And,
    Or,
}

impl OptimizedNode {
    #[inline]
    pub fn id(&self) -> u64 {
        match self {
            Self::And(children) | Self::Or(children) => {
                let mut hasher = DefaultHasher::new();
                let op = if matches!(self, Self::And(_)) {
                    Operator::And
                } else {
                    Operator::Or
                };
                op.hash(&mut hasher);
                let mut ids: Vec<u64> = children.iter().map(|c| c.id()).collect();
                ids.sort_unstable();
                ids.hash(&mut hasher);
                hasher.finish()
            }
            Self::Value(node) => node.id(),
        }
    }

    #[inline]
    pub fn cost(&self) -> u64 {
        match self {
            Self::And(c) => c.iter().map(|n| n.cost()).sum::<u64>() + 50 * (c.len() as u64 - 1),
            Self::Or(c) => c.iter().map(|n| n.cost()).sum::<u64>() + 60 * (c.len() as u64 - 1),
            Self::Value(node) => node.cost(),
        }
    }
}

impl Node {
    #[inline]
    pub fn optimize(self) -> OptimizedNode {
        self.zero_suppression_filter(false)
    }

    fn zero_suppression_filter(self, negate: bool) -> OptimizedNode {
        match self {
            Node::Not(inner) => inner.zero_suppression_filter(!negate),
            Node::Value(p) => OptimizedNode::Value(if negate { !p } else { p }),
            node => {
                let operator = node
                    .operator(negate)
                    .expect("And|Or nodes should always have an operator. This is a bug");
                let (left, right) = match node {
                    Node::And(left, right) | Node::Or(left, right) => (left, right),
                    _ => unreachable!("got unexpected node {:?}, this is a bug", node),
                };
                let mut children = Vec::new();
                left.collect(negate, &operator, &mut children);
                right.collect(negate, &operator, &mut children);
                match operator {
                    Operator::And => OptimizedNode::And(children),
                    Operator::Or => OptimizedNode::Or(children),
                }
            }
        }
    }

    fn collect(self, negate: bool, parent: &Operator, out: &mut Vec<OptimizedNode>) {
        match self {
            Node::Not(inner) => inner.collect(!negate, parent, out),
            node if node.operator(negate).as_ref() == Some(parent) => {
                if let Node::And(l, r) | Node::Or(l, r) = node {
                    l.collect(negate, parent, out);
                    r.collect(negate, parent, out);
                }
            }
            other => out.push(other.zero_suppression_filter(negate)),
        }
    }

    fn operator(&self, negate: bool) -> Option<Operator> {
        match (self, negate) {
            (Node::And(..), false) | (Node::Or(..), true) => Some(Operator::And),
            (Node::Or(..), false) | (Node::And(..), true) => Some(Operator::Or),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        events::{AttributeDefinition, AttributeTable},
        predicates::PredicateKind,
        test_utils::{
            ast::{and, not, or, value},
            optimized_node,
        },
    };

    #[test]
    fn can_optimize_a_negated_or_expression() {
        let attributes = define_attributes();
        let a_predicate = Predicate::new(&attributes, "private", PredicateKind::Variable).unwrap();
        let expression = not!(or!(
            value!(a_predicate.clone()),
            value!(!a_predicate.clone())
        ));
        assert_eq!(
            optimized_node::and!(
                optimized_node::value!(!a_predicate.clone()),
                optimized_node::value!(a_predicate)
            ),
            expression.optimize()
        );
    }

    #[test]
    fn can_optimize_a_negated_and_expression() {
        let attributes = define_attributes();
        let a_predicate =
            Predicate::new(&attributes, "private", PredicateKind::NegatedVariable).unwrap();
        let expression = not!(and!(
            value!(a_predicate.clone()),
            value!(!a_predicate.clone())
        ));

        assert_eq!(
            optimized_node::or!(
                optimized_node::value!(!a_predicate.clone()),
                optimized_node::value!(a_predicate)
            ),
            expression.optimize()
        );
    }

    #[test]
    fn can_optimize_a_negated_expression() {
        let attributes = define_attributes();
        let a_predicate = Predicate::new(&attributes, "private", PredicateKind::Variable).unwrap();
        let expression = not!(value!(a_predicate.clone()));

        assert_eq!(optimized_node::value!(!a_predicate), expression.optimize());
    }

    #[test]
    fn can_optimize_a_negated_negated_expression() {
        let attributes = define_attributes();
        let a_predicate = Predicate::new(&attributes, "private", PredicateKind::Variable).unwrap();
        let expression = not!(not!(value!(a_predicate.clone())));

        assert_eq!(optimized_node::value!(a_predicate), expression.optimize());
    }

    #[test]
    fn can_recursively_apply_the_optimizations() {
        let attributes = define_attributes();
        let a_predicate = Predicate::new(&attributes, "private", PredicateKind::Variable).unwrap();
        let expression = not!(and!(
            not!(or!(
                value!(a_predicate.clone()),
                value!(a_predicate.clone())
            )),
            and!(
                or!(value!(a_predicate.clone()), value!(a_predicate.clone())),
                or!(value!(a_predicate.clone()), value!(a_predicate.clone()))
            )
        ));

        assert_eq!(
            expression.optimize(),
            optimized_node::or!(
                optimized_node::value!(a_predicate.clone()),
                optimized_node::value!(a_predicate.clone()),
                optimized_node::and!(
                    optimized_node::value!(!a_predicate.clone()),
                    optimized_node::value!(!a_predicate.clone())
                ),
                optimized_node::and!(
                    optimized_node::value!(!a_predicate.clone()),
                    optimized_node::value!(!a_predicate.clone())
                )
            ),
        );
    }

    #[test]
    fn leave_unnegated_value_as_is() {
        let attributes = define_attributes();
        let a_predicate = Predicate::new(&attributes, "private", PredicateKind::Variable).unwrap();

        assert_eq!(
            optimized_node::value!(a_predicate.clone()),
            value!(a_predicate).optimize()
        );
    }

    #[test]
    fn leave_unnegated_and_as_is() {
        let attributes = define_attributes();
        let a_predicate = Predicate::new(&attributes, "private", PredicateKind::Variable).unwrap();
        let expression = and!(value!(a_predicate.clone()), value!(a_predicate.clone()));

        assert_eq!(
            optimized_node::and!(
                optimized_node::value!(a_predicate.clone()),
                optimized_node::value!(a_predicate.clone())
            ),
            expression.optimize()
        );
    }

    #[test]
    fn leave_unnegated_or_as_is() {
        let attributes = define_attributes();
        let a_predicate = Predicate::new(&attributes, "private", PredicateKind::Variable).unwrap();
        let expression = or!(value!(a_predicate.clone()), value!(a_predicate.clone()));

        assert_eq!(
            optimized_node::or!(
                optimized_node::value!(a_predicate.clone()),
                optimized_node::value!(a_predicate.clone())
            ),
            expression.optimize()
        );
    }

    #[test]
    fn can_optimize_a_negated_and_expression_not_at_the_top_level() {
        let attributes = define_attributes();
        let a_predicate = Predicate::new(&attributes, "private", PredicateKind::Variable).unwrap();
        let expression = and!(
            not!(and!(
                value!(a_predicate.clone()),
                value!(a_predicate.clone())
            )),
            value!(a_predicate.clone())
        );

        assert_eq!(
            optimized_node::and!(
                optimized_node::or!(
                    optimized_node::value!(!a_predicate.clone()),
                    optimized_node::value!(!a_predicate.clone())
                ),
                optimized_node::value!(a_predicate)
            ),
            expression.optimize()
        );
    }

    #[test]
    fn can_optimize_a_negated_or_expression_not_at_the_top_level() {
        let attributes = define_attributes();
        let a_predicate = Predicate::new(&attributes, "private", PredicateKind::Variable).unwrap();
        let expression = or!(
            not!(or!(
                value!(a_predicate.clone()),
                value!(a_predicate.clone())
            )),
            value!(a_predicate.clone())
        );

        assert_eq!(
            optimized_node::or!(
                optimized_node::and!(
                    optimized_node::value!(!a_predicate.clone()),
                    optimized_node::value!(!a_predicate.clone())
                ),
                optimized_node::value!(a_predicate)
            ),
            expression.optimize()
        );
    }

    #[test]
    fn return_different_ids_expressions_that_only_differ_by_their_operators() {
        let attributes = define_attributes();
        let a_predicate = Predicate::new(&attributes, "private", PredicateKind::Variable).unwrap();
        let another_predicate =
            Predicate::new(&attributes, "test", PredicateKind::Variable).unwrap();
        let expression = optimized_node::or!(
            optimized_node::value!(a_predicate.clone()),
            optimized_node::value!(another_predicate.clone())
        );
        let another_expression = optimized_node::and!(
            optimized_node::value!(a_predicate),
            optimized_node::value!(another_predicate)
        );

        assert_ne!(expression.id(), another_expression.id());
    }

    #[test]
    fn return_the_same_id_on_the_same_expressions() {
        let attributes = define_attributes();
        let a_predicate = Predicate::new(&attributes, "private", PredicateKind::Variable).unwrap();
        let another_predicate =
            Predicate::new(&attributes, "test", PredicateKind::Variable).unwrap();
        let expression = optimized_node::or!(
            optimized_node::value!(a_predicate.clone()),
            optimized_node::value!(another_predicate.clone())
        );
        let another_expression = optimized_node::or!(
            optimized_node::value!(a_predicate),
            optimized_node::value!(another_predicate)
        );

        assert_eq!(expression.id(), another_expression.id());
    }

    #[test]
    fn return_the_same_id_on_expressions_that_only_differ_by_their_predicate_ordering() {
        let attributes = define_attributes();
        let a_predicate = Predicate::new(&attributes, "private", PredicateKind::Variable).unwrap();
        let another_predicate =
            Predicate::new(&attributes, "test", PredicateKind::Variable).unwrap();
        let expression = optimized_node::or!(
            optimized_node::value!(a_predicate.clone()),
            optimized_node::value!(another_predicate.clone())
        );
        let another_expression = optimized_node::or!(
            optimized_node::value!(another_predicate),
            optimized_node::value!(a_predicate)
        );

        assert_eq!(expression.id(), another_expression.id());
    }

    fn define_attributes() -> AttributeTable {
        let definitions = vec![
            AttributeDefinition::string_list("deals"),
            AttributeDefinition::string("deal"),
            AttributeDefinition::integer("price"),
            AttributeDefinition::integer("exchange_id"),
            AttributeDefinition::boolean("private"),
            AttributeDefinition::boolean("test"),
            AttributeDefinition::string_list("deal_ids"),
            AttributeDefinition::integer_list("ids"),
            AttributeDefinition::integer_list("segment_ids"),
            AttributeDefinition::string("continent"),
            AttributeDefinition::string("country"),
            AttributeDefinition::string("city"),
        ];
        AttributeTable::new(&definitions).unwrap()
    }
}
