use crate::{
    events::{AttributeIndex, AttributeKind, AttributeTable, AttributeValue, Event, EventError},
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

    pub fn evaluate(&self, event: Event) -> bool {
        let value = event.get(&self.attribute);
        match (&self.kind, value) {
            (PredicateKind::Variable, AttributeValue::Boolean(value)) => *value,
            (PredicateKind::Null(operator), value) => operator.evaluate(value),
            (PredicateKind::Set(operator, haystack), needle) => operator.evaluate(haystack, needle),
            (PredicateKind::Comparison(operator, a), b) => operator.evaluate(a, b),
            (PredicateKind::Equality(operator, a), b) => operator.evaluate(a, b),
            (PredicateKind::List(operator, a), b) => operator.evaluate(a, b),
            (kind, value) => {
                unreachable!("Invalid => got: {kind:?} with {value:?}");
            }
        }
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

impl SetOperator {
    fn evaluate(&self, haystack: &ListLiteral, needle: &AttributeValue) -> bool {
        match (haystack, needle) {
            (ListLiteral::StringList(haystack), AttributeValue::String(needle)) => {
                self.apply(haystack, needle)
            }
            (ListLiteral::IntegerList(haystack), AttributeValue::Integer(needle)) => {
                self.apply(haystack, needle)
            }
            (a, b) => {
                unreachable!("Set operation ({self:?}) in haystack {a:?} for {b:?} should never happen. This is a bug.")
            }
        }
    }

    fn apply<T: PartialOrd>(&self, haystack: &[T], needle: &T) -> bool {
        unimplemented!();
    }
}

#[derive(Hash, PartialEq, Clone, Debug)]
pub enum ComparisonOperator {
    LessThan,
    LessThanEqual,
    GreaterThanEqual,
    GreaterThan,
}

impl ComparisonOperator {
    fn evaluate(&self, a: &ComparisonValue, b: &AttributeValue) -> bool {
        match (a, b) {
            (ComparisonValue::Float(b), AttributeValue::Float(a)) => self.apply(&a, &b),
            (ComparisonValue::Integer(b), AttributeValue::Integer(a)) => self.apply(&a, &b),
            (a, b) => {
                unreachable!("Comparison ({self:?}) between {a:?} and {b:?} should never happen. This is a bug.")
            }
        }
    }

    fn apply<T: PartialOrd>(&self, a: &T, b: &T) -> bool {
        match self {
            Self::LessThan => *a < *b,
            Self::LessThanEqual => *a <= *b,
            Self::GreaterThan => *a > *b,
            Self::GreaterThanEqual => *a >= *b,
        }
    }
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

impl EqualityOperator {
    fn evaluate(&self, a: &PrimitiveLiteral, b: &AttributeValue) -> bool {
        match (a, b) {
            (PrimitiveLiteral::Float(a), AttributeValue::Float(b)) => self.apply(&a, &b),
            (PrimitiveLiteral::Integer(a), AttributeValue::Integer(b)) => self.apply(&a, &b),
            (PrimitiveLiteral::String(a), AttributeValue::String(b)) => self.apply(&a, &b),
            (a, b) => {
                unreachable!("Equality ({self:?}) between {a:?} and {b:?} should never happen. This is a bug.")
            }
        }
    }

    fn apply<T: PartialEq>(&self, a: &T, b: &T) -> bool {
        match self {
            Self::Equal => *a == *b,
            Self::NotEqual => *a != *b,
        }
    }
}

#[derive(Hash, PartialEq, Clone, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum ListOperator {
    OneOf,
    NoneOf,
    AllOf,
}

impl ListOperator {
    fn evaluate(&self, a: &ListLiteral, b: &AttributeValue) -> bool {
        unimplemented!();
    }
}

#[derive(Hash, PartialEq, Clone, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum NullOperator {
    IsNull,
    IsNotNull,
    IsEmpty,
}

impl NullOperator {
    fn evaluate(&self, value: &AttributeValue) -> bool {
        match (self, value) {
            (Self::IsNull, AttributeValue::Undefined) => true,
            (
                Self::IsNull,
                AttributeValue::Integer(_) | AttributeValue::String(_) | AttributeValue::Float(_),
            ) => false,
            (Self::IsNotNull, AttributeValue::Undefined) => false,
            (
                Self::IsNotNull,
                AttributeValue::Integer(_) | AttributeValue::String(_) | AttributeValue::Float(_),
            ) => true,
            (Self::IsEmpty, AttributeValue::StringList(list)) => list.is_empty(),
            (Self::IsEmpty, AttributeValue::IntegerList(list)) => list.is_empty(),
            (_, value) => {
                unreachable!(
                    "Null check ({self:?}) for {value:?} should never happen. This is a bug."
                )
            }
        }
    }
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
