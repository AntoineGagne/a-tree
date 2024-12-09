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

#[derive(Hash, Clone, Eq, PartialEq)]
pub enum Operator {
    And,
    Or,
    Not,
    Value(Predicate),
}
