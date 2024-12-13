use crate::predicates::Predicate;
use std::hash::{Hash, Hasher};

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
        match self {
            Self::And(left, right) => u64::wrapping_mul(left.id(), right.id()),
            Self::Or(left, right) => u64::wrapping_add(left.id(), right.id()),
            Self::Not(node) => !node.id(),
            Self::Value(node) => node.id(),
        }
    }
}
