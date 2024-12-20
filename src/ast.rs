use crate::predicates::Predicate;
use std::hash::Hash;

pub type TreeNode = Box<Node>;

#[derive(PartialEq, Clone, Debug)]
pub enum Node {
    And(TreeNode, TreeNode),
    Or(TreeNode, TreeNode),
    Not(TreeNode),
    Value(Predicate),
}

#[derive(Debug, Hash, Clone, Eq, PartialEq)]
pub enum Operator {
    And,
    Or,
    Not,
}

impl Node {
    #[inline]
    pub fn id(&self) -> u64 {
        // TODO: Even though the paper specifies that way of computing the ID, I feel as though
        // this might yield collisions. For example, if there are some expressions such as
        // (where A = 3, B = 5, C = 2 and D = 6):
        //
        // A ∧ B
        // (C ∧ D) ∨ A
        //
        // Then, given the above expressions, there could be a conflict in the expression IDs.
        // If this is possible, should this implementation be switched for a commutative hashing
        // strategy?
        match self {
            Self::And(left, right) => u64::wrapping_mul(left.id(), right.id()),
            Self::Or(left, right) => u64::wrapping_add(left.id(), right.id()),
            Self::Not(node) => !node.id(),
            Self::Value(node) => node.id(),
        }
    }

    #[inline]
    pub fn cost(&self) -> u64 {
        match self {
            Self::And(left, right) | Self::Or(left, right) => left.cost() + right.cost(),
            Self::Not(node) => node.cost() + 1,
            Self::Value(node) => node.cost(),
        }
    }

    #[inline]
    pub fn optimize(self) -> Self {
        self.zero_suppression_filter(false)
    }

    pub fn zero_suppression_filter(self, negate: bool) -> Self {
        match (self, negate) {
            (Self::And(left, right), true) => Self::Or(
                Box::new(left.zero_suppression_filter(true)),
                Box::new(right.zero_suppression_filter(true)),
            ),
            (Self::Or(left, right), true) => Self::And(
                Box::new(left.zero_suppression_filter(true)),
                Box::new(right.zero_suppression_filter(true)),
            ),
            (Self::Not(value), true) => value.zero_suppression_filter(false),
            (Self::Not(value), false) => value.zero_suppression_filter(true),
            (Self::Value(predicate), true) => Self::Value(!predicate),
            (Self::And(left, right), false) => {
                Self::And(Box::new(left.optimize()), Box::new(right.optimize()))
            }
            (Self::Or(left, right), false) => {
                Self::Or(Box::new(left.optimize()), Box::new(right.optimize()))
            }
            (value @ Self::Value(_), _) => value,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        events::{AttributeDefinition, AttributeTable},
        predicates::PredicateKind,
        test_utils::ast::{and, not, or, value},
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
            and!(value!(!a_predicate.clone()), value!(a_predicate)),
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
            or!(value!(!a_predicate.clone()), value!(a_predicate)),
            expression.optimize()
        );
    }

    #[test]
    fn can_optimize_a_negated_expression() {
        let attributes = define_attributes();
        let a_predicate = Predicate::new(&attributes, "private", PredicateKind::Variable).unwrap();
        let expression = not!(value!(a_predicate.clone()));

        assert_eq!(value!(!a_predicate), expression.optimize());
    }

    #[test]
    fn can_optimize_a_negated_negated_expression() {
        let attributes = define_attributes();
        let a_predicate = Predicate::new(&attributes, "private", PredicateKind::Variable).unwrap();
        let expression = not!(not!(value!(a_predicate.clone())));

        assert_eq!(value!(a_predicate), expression.optimize());
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
            or!(
                or!(value!(a_predicate.clone()), value!(a_predicate.clone())),
                or!(
                    and!(value!(!a_predicate.clone()), value!(!a_predicate.clone())),
                    and!(value!(!a_predicate.clone()), value!(!a_predicate.clone()))
                )
            ),
            expression.optimize()
        );
    }

    #[test]
    fn leave_unnegated_value_as_is() {
        let attributes = define_attributes();
        let a_predicate = Predicate::new(&attributes, "private", PredicateKind::Variable).unwrap();

        assert_eq!(value!(a_predicate.clone()), value!(a_predicate).optimize());
    }

    #[test]
    fn leave_unnegated_and_as_is() {
        let attributes = define_attributes();
        let a_predicate = Predicate::new(&attributes, "private", PredicateKind::Variable).unwrap();
        let expression = and!(value!(a_predicate.clone()), value!(a_predicate));

        assert_eq!(expression.clone(), expression.optimize());
    }

    #[test]
    fn leave_unnegated_or_as_is() {
        let attributes = define_attributes();
        let a_predicate = Predicate::new(&attributes, "private", PredicateKind::Variable).unwrap();
        let expression = or!(value!(a_predicate.clone()), value!(a_predicate));

        assert_eq!(expression.clone(), expression.optimize());
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
            and!(
                or!(value!(!a_predicate.clone()), value!(!a_predicate.clone())),
                value!(a_predicate)
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
            or!(
                and!(value!(!a_predicate.clone()), value!(!a_predicate.clone())),
                value!(a_predicate)
            ),
            expression.optimize()
        );
    }

    fn define_attributes() -> AttributeTable {
        let definitions = vec![
            AttributeDefinition::string_list("deals"),
            AttributeDefinition::string("deal"),
            AttributeDefinition::integer("price"),
            AttributeDefinition::integer("exchange_id"),
            AttributeDefinition::boolean("private"),
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
