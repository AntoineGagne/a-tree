use crate::{
    events::{AttributeIndex, AttributeKind, AttributeTable, EventError},
    strings::StringId,
};
use rust_decimal::Decimal;

#[derive(Hash, Debug, Clone, PartialEq)]
pub struct Predicate {
    attribute: AttributeIndex,
    kind: PredicateKind,
}

impl Predicate {
    pub fn new<'a>(
        attributes: &AttributeTable,
        name: &str,
        kind: PredicateKind,
    ) -> Result<Self, EventError> {
        attributes
            .by_name(name)
            .ok_or_else(|| EventError::NonExistingAttribute(name.to_string()))
            .and_then(|id| {
                validate_predicate(name, &kind, &attributes.by_id(id))?;
                Ok(Predicate {
                    attribute: id,
                    kind,
                })
            })
    }
}

fn validate_predicate(
    name: &str,
    kind: &PredicateKind,
    attribute_kind: &AttributeKind,
) -> Result<(), EventError> {
    match (&kind, attribute_kind) {
        (PredicateKind::Set(_, ListLiteral::StringList(_)), AttributeKind::String) => Ok(()),
        (PredicateKind::Set(_, ListLiteral::IntegerList(_)), AttributeKind::Integer) => Ok(()),
        (PredicateKind::Comparison(_, ComparisonValue::Integer(_)), AttributeKind::Integer) => {
            Ok(())
        }
        (PredicateKind::Comparison(_, ComparisonValue::Float(_)), AttributeKind::Float) => Ok(()),
        (PredicateKind::Equality(_, PrimitiveLiteral::Integer(_)), AttributeKind::Integer) => {
            Ok(())
        }
        (PredicateKind::Equality(_, PrimitiveLiteral::Float(_)), AttributeKind::Float) => Ok(()),
        (PredicateKind::Equality(_, PrimitiveLiteral::String(_)), AttributeKind::String) => Ok(()),
        (PredicateKind::List(_, ListLiteral::IntegerList(_)), AttributeKind::IntegerList) => Ok(()),
        (PredicateKind::List(_, ListLiteral::StringList(_)), AttributeKind::StringList) => Ok(()),
        (PredicateKind::Variable, AttributeKind::Boolean) => Ok(()),
        (PredicateKind::Null(NullOperator::IsEmpty), AttributeKind::StringList) => Ok(()),
        (PredicateKind::Null(NullOperator::IsEmpty), AttributeKind::IntegerList) => Ok(()),
        (PredicateKind::Null(NullOperator::IsNull), AttributeKind::Integer) => Ok(()),
        (PredicateKind::Null(NullOperator::IsNull), AttributeKind::Float) => Ok(()),
        (PredicateKind::Null(NullOperator::IsNull), AttributeKind::String) => Ok(()),
        (PredicateKind::Null(NullOperator::IsNotNull), AttributeKind::Integer) => Ok(()),
        (PredicateKind::Null(NullOperator::IsNotNull), AttributeKind::Float) => Ok(()),
        (PredicateKind::Null(NullOperator::IsNotNull), AttributeKind::String) => Ok(()),
        (actual, expected) => Err(EventError::MismatchingTypes {
            name: name.to_string(),
            expected: expected.clone(),
            actual: (*actual).clone(),
        }),
    }
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
