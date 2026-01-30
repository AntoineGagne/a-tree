use crate::predicates::Predicate;
use std::cmp::{max, min};
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
    And(Box<OptimizedNode>, Box<OptimizedNode>),
    Or(Box<OptimizedNode>, Box<OptimizedNode>),
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
            Self::And(left, right) => {
                let mut hasher = DefaultHasher::new();
                Operator::And.hash(&mut hasher);

                let left_id = left.id();
                let right_id = right.id();
                min(left_id, right_id).hash(&mut hasher);
                max(left_id, right_id).hash(&mut hasher);
                hasher.finish()
            }
            Self::Or(left, right) => {
                let mut hasher = DefaultHasher::new();
                Operator::Or.hash(&mut hasher);

                let left_id = left.id();
                let right_id = right.id();
                min(left_id, right_id).hash(&mut hasher);
                max(left_id, right_id).hash(&mut hasher);
                hasher.finish()
            }
            Self::Value(node) => node.id(),
        }
    }

    #[inline]
    pub fn cost(&self) -> u64 {
        match self {
            // There is more chance that the evaluation leads to a `false` result which means that
            // `AND` nodes are usually less expansive since they might be skipped entirely because
            // of the propagation on demand.
            Self::And(left, right) => left.cost() + right.cost() + 50,
            Self::Or(left, right) => left.cost() + right.cost() + 60,
            Self::Value(node) => node.cost(),
        }
    }
}

impl Node {
    #[inline]
    pub fn optimize(self) -> OptimizedNode {
        self.zero_suppression_filter(false)
    }

    pub fn zero_suppression_filter(self, negate: bool) -> OptimizedNode {
        match (self, negate) {
            (Self::And(left, right), true) => OptimizedNode::Or(
                Box::new(left.zero_suppression_filter(true)),
                Box::new(right.zero_suppression_filter(true)),
            ),
            (Self::Or(left, right), true) => OptimizedNode::And(
                Box::new(left.zero_suppression_filter(true)),
                Box::new(right.zero_suppression_filter(true)),
            ),
            (Self::Not(value), true) => value.zero_suppression_filter(false),
            (Self::Not(value), false) => value.zero_suppression_filter(true),
            (Self::Value(predicate), true) => OptimizedNode::Value(!predicate),
            (Self::And(left, right), false) => OptimizedNode::And(
                Box::new(left.zero_suppression_filter(false)),
                Box::new(right.zero_suppression_filter(false)),
            ),
            (Self::Or(left, right), false) => OptimizedNode::Or(
                Box::new(left.zero_suppression_filter(false)),
                Box::new(right.zero_suppression_filter(false)),
            ),
            (Self::Value(predicate), _) => OptimizedNode::Value(predicate),
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
            optimized_node::or!(
                optimized_node::or!(
                    optimized_node::value!(a_predicate.clone()),
                    optimized_node::value!(a_predicate.clone())
                ),
                optimized_node::or!(
                    optimized_node::and!(
                        optimized_node::value!(!a_predicate.clone()),
                        optimized_node::value!(!a_predicate.clone())
                    ),
                    optimized_node::and!(
                        optimized_node::value!(!a_predicate.clone()),
                        optimized_node::value!(!a_predicate.clone())
                    )
                )
            ),
            expression.optimize()
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
