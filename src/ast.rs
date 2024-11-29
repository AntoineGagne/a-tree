use crate::strings::StringId;
use rust_decimal::Decimal;

pub type TreeNode = Box<Node>;

#[derive(PartialEq, Clone, Debug)]
pub enum Node {
    And(TreeNode, TreeNode),
    Or(TreeNode, TreeNode),
    Not(TreeNode),
    Value(Predicate),
}

#[derive(Hash, PartialEq, Clone, Debug)]
pub struct Predicate {
    pub attribute: String,
    pub kind: PredicateKind,
}

#[derive(Hash, PartialEq, Clone, Debug)]
pub enum PredicateKind {
    Variable,
    Set(SetOperator, ListLiteral),
    Comparison(ComparisonOperator, ComparisonValue),
    Equality(EqualityOperator, PrimitiveLiteral),
    List(ListOperator, ListLiteral),
    Null(NullOperator),
}

#[derive(Hash, PartialEq, Clone, Debug)]
pub enum SetOperator {
    NotIn,
    In,
}

#[derive(Hash, PartialEq, Clone, Debug)]
pub enum ComparisonOperator {
    LessThan,
    LessThanEqual,
    GreaterThanEqual,
    GreaterThan,
}

#[derive(Hash, PartialEq, Clone, Debug)]
pub enum ComparisonValue {
    Integer(i64),
    Float(Decimal),
}

#[derive(Hash, PartialEq, Clone, Debug)]
pub enum EqualityOperator {
    Equal,
    NotEqual,
}

#[derive(Hash, PartialEq, Clone, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum ListOperator {
    OneOf,
    NoneOf,
    AllOf,
}

#[derive(Hash, PartialEq, Clone, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum NullOperator {
    IsNull,
    IsNotNull,
    IsEmpty,
}

#[derive(Hash, PartialEq, Clone, Debug)]
pub enum ListLiteral {
    IntegerList(Vec<i64>),
    StringList(Vec<StringId>),
}

#[derive(Hash, PartialEq, Clone, Debug)]
pub enum PrimitiveLiteral {
    Integer(i64),
    Float(Decimal),
    String(StringId),
}
