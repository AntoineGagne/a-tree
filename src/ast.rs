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
    Value(Predicate),
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
        // If this is possible, should this implementation be switch for a commutative hashing
        // strategy?
        match self {
            Self::And(left, right) => u64::wrapping_mul(left.id(), right.id()),
            Self::Or(left, right) => u64::wrapping_add(left.id(), right.id()),
            Self::Not(node) => !node.id(),
            Self::Value(node) => node.id(),
        }
    }
}
