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
    Set(set::Operator, ListLiteral),
    Comparison(comparison::Operator, comparison::Value),
    Equality(equality::Operator, PrimitiveLiteral),
    List(list::Operator, ListLiteral),
    Null(null::Operator),
}

pub mod set {
    #[derive(Hash, PartialEq, Clone, Debug)]
    pub enum Operator {
        NotIn,
        In,
    }
}

pub mod comparison {
    use rust_decimal::Decimal;

    #[derive(Hash, PartialEq, Clone, Debug)]
    pub enum Operator {
        LessThan,
        LessThanEqual,
        GreaterThanEqual,
        GreaterThan,
    }

    #[derive(Hash, PartialEq, Clone, Debug)]
    pub enum Value {
        Integer(i64),
        Float(Decimal),
    }
}

pub mod equality {
    #[derive(Hash, PartialEq, Clone, Debug)]
    pub enum Operator {
        Equal,
        NotEqual,
    }
}

pub mod list {
    #[derive(Hash, PartialEq, Clone, Debug)]
    #[allow(clippy::enum_variant_names)]
    pub enum Operator {
        OneOf,
        NoneOf,
        AllOf,
    }
}

pub mod null {
    #[derive(Hash, PartialEq, Clone, Debug)]
    #[allow(clippy::enum_variant_names)]
    pub enum Operator {
        IsNull,
        IsNotNull,
        IsEmpty,
    }
}

#[derive(Hash, PartialEq, Clone, Debug)]
pub enum ListLiteral {
    IntegerList(Vec<i64>),
    StringList(Vec<String>),
}

#[derive(Hash, PartialEq, Clone, Debug)]
pub enum PrimitiveLiteral {
    Integer(i64),
    Float(Decimal),
    String(String),
}
