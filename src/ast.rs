use crate::predicates::Predicate;

pub type TreeNode = Box<Node>;

#[derive(PartialEq, Clone, Debug)]
pub enum Node {
    And(TreeNode, TreeNode),
    Or(TreeNode, TreeNode),
    Not(TreeNode),
    Value(Predicate),
}
