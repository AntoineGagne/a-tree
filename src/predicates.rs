use crate::{
    events::{AttributeId, AttributeKind, AttributeTable, AttributeValue, Event, EventError},
    strings::StringId,
};
use rust_decimal::Decimal;
use std::{
    fmt::{Display, Formatter},
    hash::{Hash, Hasher},
    ops::Not,
};

#[derive(Eq, PartialEq, Hash, Debug, Clone)]
pub struct Predicate {
    attribute: AttributeId,
    kind: PredicateKind,
}

impl Predicate {
    pub fn new(
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

    #[inline]
    pub fn id(&self) -> u64 {
        use std::hash::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }

    #[inline]
    pub fn cost(&self) -> u64 {
        self.kind.cost()
    }

    pub fn evaluate(&self, event: &Event) -> Option<bool> {
        let value = &event[self.attribute];
        match (&self.kind, value) {
            (PredicateKind::Null(operator), value) => Some(operator.evaluate(value)),
            (_, AttributeValue::Undefined) => None,
            (PredicateKind::Variable, AttributeValue::Boolean(value)) => Some(*value),
            (PredicateKind::NegatedVariable, AttributeValue::Boolean(value)) => Some(!*value),
            (PredicateKind::Set(operator, haystack), needle) => {
                Some(operator.evaluate(haystack, needle))
            }
            (PredicateKind::Comparison(operator, a), b) => Some(operator.evaluate(a, b)),
            (PredicateKind::Equality(operator, a), b) => Some(operator.evaluate(a, b)),
            (PredicateKind::List(operator, a), b) => Some(operator.evaluate(a, b)),
            (kind, value) => {
                unreachable!("Invalid => got: {kind:?} with {value:?}");
            }
        }
    }
}

impl Not for Predicate {
    type Output = Self;

    fn not(self) -> Self::Output {
        Self {
            attribute: self.attribute,
            kind: !self.kind,
        }
    }
}

impl Display for Predicate {
    fn fmt(&self, formatter: &mut Formatter) -> std::fmt::Result {
        write!(formatter, "⟨{}, {}⟩", self.attribute, self.kind)
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
        (PredicateKind::NegatedVariable, AttributeKind::Boolean) => Ok(()),

        (PredicateKind::Null(NullOperator::IsEmpty), AttributeKind::StringList) => Ok(()),
        (PredicateKind::Null(NullOperator::IsEmpty), AttributeKind::IntegerList) => Ok(()),
        (PredicateKind::Null(NullOperator::IsNotEmpty), AttributeKind::StringList) => Ok(()),
        (PredicateKind::Null(NullOperator::IsNotEmpty), AttributeKind::IntegerList) => Ok(()),
        (PredicateKind::Null(NullOperator::IsNull), AttributeKind::Integer) => Ok(()),
        (PredicateKind::Null(NullOperator::IsNull), AttributeKind::Float) => Ok(()),
        (PredicateKind::Null(NullOperator::IsNull), AttributeKind::String) => Ok(()),
        (PredicateKind::Null(NullOperator::IsNull), AttributeKind::Boolean) => Ok(()),
        (PredicateKind::Null(NullOperator::IsNotNull), AttributeKind::Integer) => Ok(()),
        (PredicateKind::Null(NullOperator::IsNotNull), AttributeKind::Float) => Ok(()),
        (PredicateKind::Null(NullOperator::IsNotNull), AttributeKind::String) => Ok(()),
        (PredicateKind::Null(NullOperator::IsNotNull), AttributeKind::Boolean) => Ok(()),
        (actual, expected) => Err(EventError::MismatchingTypes {
            name: name.to_string(),
            expected: expected.clone(),
            actual: (*actual).clone(),
        }),
    }
}

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub enum PredicateKind {
    Variable,
    NegatedVariable,
    Set(SetOperator, ListLiteral),
    Comparison(ComparisonOperator, ComparisonValue),
    Equality(EqualityOperator, PrimitiveLiteral),
    List(ListOperator, ListLiteral),
    Null(NullOperator),
}

impl PredicateKind {
    const CONSTANT_COST: u64 = 0;
    const LOGARITHMIC_COST: u64 = 1;
    const LIST_COST: u64 = 2;

    #[inline]
    pub fn cost(&self) -> u64 {
        match self {
            Self::NegatedVariable
            | Self::Variable
            | Self::Null(_)
            | Self::Comparison(_, _)
            | Self::Equality(_, _) => Self::CONSTANT_COST,
            Self::Set(_, ListLiteral::StringList(list)) => {
                Self::LOGARITHMIC_COST * (list.len() as u64)
            }
            Self::Set(_, ListLiteral::IntegerList(list)) => {
                Self::LOGARITHMIC_COST * (list.len() as u64)
            }
            Self::List(_, ListLiteral::StringList(list)) => Self::LIST_COST * (list.len() as u64),
            Self::List(_, ListLiteral::IntegerList(list)) => Self::LIST_COST * (list.len() as u64),
        }
    }
}

impl Not for PredicateKind {
    type Output = Self;

    fn not(self) -> Self {
        match self {
            Self::Set(SetOperator::In, value) => Self::Set(SetOperator::NotIn, value),
            Self::Set(SetOperator::NotIn, value) => Self::Set(SetOperator::In, value),
            Self::Comparison(ComparisonOperator::LessThan, value) => {
                Self::Comparison(ComparisonOperator::GreaterThanEqual, value)
            }
            Self::Comparison(ComparisonOperator::LessThanEqual, value) => {
                Self::Comparison(ComparisonOperator::GreaterThan, value)
            }
            Self::Comparison(ComparisonOperator::GreaterThan, value) => {
                Self::Comparison(ComparisonOperator::LessThanEqual, value)
            }
            Self::Comparison(ComparisonOperator::GreaterThanEqual, value) => {
                Self::Comparison(ComparisonOperator::LessThan, value)
            }
            Self::Null(NullOperator::IsNull) => Self::Null(NullOperator::IsNotNull),
            Self::Null(NullOperator::IsNotNull) => Self::Null(NullOperator::IsNull),
            Self::Null(NullOperator::IsEmpty) => Self::Null(NullOperator::IsNotEmpty),
            Self::Null(NullOperator::IsNotEmpty) => Self::Null(NullOperator::IsEmpty),
            Self::Equality(EqualityOperator::Equal, value) => {
                Self::Equality(EqualityOperator::NotEqual, value)
            }
            Self::Equality(EqualityOperator::NotEqual, value) => {
                Self::Equality(EqualityOperator::Equal, value)
            }
            Self::List(ListOperator::OneOf, value) => Self::List(ListOperator::NoneOf, value),
            Self::List(ListOperator::AllOf, value) => Self::List(ListOperator::NotAllOf, value),
            Self::List(ListOperator::NotAllOf, value) => Self::List(ListOperator::AllOf, value),
            Self::List(ListOperator::NoneOf, value) => Self::List(ListOperator::OneOf, value),
            Self::Variable => Self::NegatedVariable,
            Self::NegatedVariable => Self::Variable,
        }
    }
}

impl Display for PredicateKind {
    fn fmt(&self, formatter: &mut Formatter) -> std::fmt::Result {
        match self {
            Self::Variable => write!(formatter, "id, variable"),
            Self::NegatedVariable => write!(formatter, "not, variable"),
            Self::Set(operator, values) => write!(formatter, "{operator}, {values}"),
            Self::Comparison(operator, values) => write!(formatter, "{operator}, {values}"),
            Self::List(operator, values) => write!(formatter, "{operator}, {values}"),
            Self::Null(operator) => write!(formatter, "{operator}, variable"),
            Self::Equality(operator, values) => write!(formatter, "{operator}, {values}"),
        }
    }
}

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
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

    fn apply<T: Ord>(&self, haystack: &[T], needle: &T) -> bool {
        match self {
            Self::In => haystack.binary_search(needle).is_ok(),
            Self::NotIn => haystack.binary_search(needle).is_err(),
        }
    }
}

impl Display for SetOperator {
    fn fmt(&self, formatter: &mut Formatter) -> std::fmt::Result {
        match self {
            Self::In => write!(formatter, "in"),
            Self::NotIn => write!(formatter, "not in"),
        }
    }
}

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
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

impl Display for ComparisonOperator {
    fn fmt(&self, formatter: &mut Formatter) -> std::fmt::Result {
        match self {
            Self::GreaterThanEqual => write!(formatter, ">="),
            Self::GreaterThan => write!(formatter, ">"),
            Self::LessThan => write!(formatter, "<"),
            Self::LessThanEqual => write!(formatter, "<="),
        }
    }
}

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub enum ComparisonValue {
    Integer(i64),
    Float(Decimal),
}

impl Display for ComparisonValue {
    fn fmt(&self, formatter: &mut Formatter) -> std::fmt::Result {
        match self {
            Self::Integer(value) => write!(formatter, "{value}"),
            Self::Float(value) => write!(formatter, "{value}"),
        }
    }
}

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
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

impl Display for EqualityOperator {
    fn fmt(&self, formatter: &mut Formatter) -> std::fmt::Result {
        match self {
            Self::Equal => write!(formatter, "="),
            Self::NotEqual => write!(formatter, "<>"),
        }
    }
}

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum ListOperator {
    OneOf,
    NoneOf,
    AllOf,
    // This is an internal operator only This is only to achieve symmetry with
    // the `all_of` operator for the zero suppression filter.
    NotAllOf,
}

impl ListOperator {
    fn evaluate(&self, a: &ListLiteral, b: &AttributeValue) -> bool {
        match (a, b) {
            (ListLiteral::StringList(right), AttributeValue::StringList(left)) => {
                self.apply(left, right)
            }
            (ListLiteral::IntegerList(right), AttributeValue::IntegerList(left)) => {
                self.apply(left, right)
            }
            (a, b) => {
                unreachable!("List operations ({self:?}) between {a:?} and {b:?} should never happen. This is a bug.")
            }
        }
    }

    fn apply<T: Ord>(&self, left: &[T], right: &[T]) -> bool {
        match self {
            Self::OneOf => one_of(left, right),
            Self::NoneOf => none_of(left, right),
            Self::AllOf => all_of(left, right),
            Self::NotAllOf => not_all_of(left, right),
        }
    }
}

impl Display for ListOperator {
    fn fmt(&self, formatter: &mut Formatter) -> std::fmt::Result {
        match self {
            Self::AllOf => write!(formatter, "all of"),
            Self::OneOf => write!(formatter, "one of"),
            Self::NoneOf => write!(formatter, "none of"),
            Self::NotAllOf => write!(formatter, "not all of"),
        }
    }
}

#[inline]
fn none_of<T: Ord>(left: &[T], right: &[T]) -> bool {
    !one_of(left, right)
}

fn one_of<T: Ord>(left: &[T], right: &[T]) -> bool {
    use std::cmp::Ordering;

    if left.is_empty() || right.is_empty() {
        return false;
    }

    let mut i = 0usize;
    let mut j = 0usize;
    while j < left.len() && i < right.len() {
        let x = &left[j];
        let y = &right[i];
        match y.cmp(x) {
            Ordering::Less => {
                i += 1;
            }
            Ordering::Equal => {
                return true;
            }
            Ordering::Greater => {
                j += 1;
            }
        }
    }

    false
}

#[inline]
fn not_all_of<T: Ord>(left: &[T], right: &[T]) -> bool {
    !all_of(left, right)
}

fn all_of<T: Ord>(left: &[T], right: &[T]) -> bool {
    use std::cmp::Ordering;

    if left.len() > right.len() {
        return false;
    }

    let mut i = 0usize;
    let mut j = 0usize;
    while j < left.len() && i < right.len() {
        let x = &left[j];
        let y = &right[i];
        match y.cmp(x) {
            Ordering::Less => {
                i += 1;
            }
            Ordering::Equal => {
                i += 1;
                j += 1;
            }
            Ordering::Greater => {
                break;
            }
        }
    }

    j >= left.len()
}

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum NullOperator {
    IsNull,
    IsNotNull,
    IsEmpty,
    IsNotEmpty,
}

impl NullOperator {
    fn evaluate(&self, value: &AttributeValue) -> bool {
        match (self, value) {
            (Self::IsNull, AttributeValue::Undefined) => true,
            (
                Self::IsNull,
                AttributeValue::Integer(_)
                | AttributeValue::String(_)
                | AttributeValue::Float(_)
                | AttributeValue::Boolean(_),
            ) => false,
            (Self::IsNotNull, AttributeValue::Undefined) => false,
            (
                Self::IsNotNull,
                AttributeValue::Integer(_)
                | AttributeValue::String(_)
                | AttributeValue::Float(_)
                | AttributeValue::Boolean(_),
            ) => true,
            (Self::IsEmpty, AttributeValue::StringList(list)) => list.is_empty(),
            (Self::IsEmpty, AttributeValue::IntegerList(list)) => list.is_empty(),
            (Self::IsNotEmpty, AttributeValue::StringList(list)) => !list.is_empty(),
            (Self::IsNotEmpty, AttributeValue::IntegerList(list)) => !list.is_empty(),
            (_, value) => {
                unreachable!(
                    "Null check ({self:?}) for {value:?} should never happen. This is a bug."
                )
            }
        }
    }
}

impl Display for NullOperator {
    fn fmt(&self, formatter: &mut Formatter) -> std::fmt::Result {
        match self {
            Self::IsNull => write!(formatter, "is null"),
            Self::IsNotNull => write!(formatter, "is not null"),
            Self::IsEmpty => write!(formatter, "is empty"),
            Self::IsNotEmpty => write!(formatter, "is not empty"),
        }
    }
}

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub enum ListLiteral {
    IntegerList(Vec<i64>),
    StringList(Vec<StringId>),
}

impl Display for ListLiteral {
    fn fmt(&self, formatter: &mut Formatter) -> std::fmt::Result {
        match self {
            Self::IntegerList(values) => write!(formatter, "{values:?}"),
            Self::StringList(values) => write!(formatter, "{values:?}"),
        }
    }
}

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub enum PrimitiveLiteral {
    Integer(i64),
    Float(Decimal),
    String(StringId),
}

impl Display for PrimitiveLiteral {
    fn fmt(&self, formatter: &mut Formatter) -> std::fmt::Result {
        match self {
            Self::Integer(values) => write!(formatter, "{values}"),
            Self::Float(values) => write!(formatter, "{values}"),
            Self::String(values) => write!(formatter, "{values:?}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        events::{AttributeDefinition, AttributeTable, EventBuilder},
        strings::StringTable,
        test_utils::predicates::{
            all_of, comparison_float, comparison_integer, equal, greater_than, greater_than_equal,
            integer_list, is_empty, is_not_empty, is_not_null, is_null, less_than, less_than_equal,
            negated_variable, none_of, not_equal, one_of, predicate, primitive_string, set_in,
            set_not_in, string_list, variable,
        },
    };
    use itertools::Itertools;
    use proptest::prelude::{proptest, *};

    const AN_EXCHANGE_ID: i64 = 23;
    const A_COUNTRY: &str = "CA";
    const ANOTHER_COUNTRY: &str = "US";

    #[test]
    fn return_true_on_boolean_variable_that_is_true() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_boolean("private", true).unwrap();
        let event = builder.build().unwrap();
        let predicate = variable!(&attributes, "private");

        assert_eq!(Some(true), predicate.evaluate(&event));
    }

    #[test]
    fn return_false_on_boolean_variable_that_is_false() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_boolean("private", false).unwrap();
        let event = builder.build().unwrap();
        let predicate = variable!(&attributes, "private");

        assert_eq!(Some(false), predicate.evaluate(&event));
    }

    #[test]
    fn return_false_on_negated_boolean_variable_that_is_true() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_boolean("private", true).unwrap();
        let event = builder.build().unwrap();
        let predicate = negated_variable!(&attributes, "private");

        assert_eq!(Some(false), predicate.evaluate(&event));
    }

    #[test]
    fn return_true_on_negated_boolean_variable_that_is_false() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_boolean("private", false).unwrap();
        let event = builder.build().unwrap();
        let predicate = negated_variable!(&attributes, "private");

        assert_eq!(Some(true), predicate.evaluate(&event));
    }

    #[test]
    fn return_false_on_null_check_for_defined_variable() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let event = an_event_builder(&attributes, &strings).build().unwrap();
        let predicate = is_null!(&attributes, "country");

        assert_eq!(Some(false), predicate.evaluate(&event));
    }

    #[test]
    fn return_true_on_null_check_for_undefined_variable() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_undefined("country").unwrap();
        let event = builder.build().unwrap();
        let predicate = is_null!(&attributes, "country");

        assert_eq!(Some(true), predicate.evaluate(&event));
    }

    #[test]
    fn return_true_on_not_null_check_for_defined_variable() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let event = an_event_builder(&attributes, &strings).build().unwrap();
        let predicate = is_not_null!(&attributes, "country");

        assert_eq!(Some(true), predicate.evaluate(&event));
    }

    #[test]
    fn return_true_on_not_null_check_for_undefined_variable() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_undefined("country").unwrap();
        let event = builder.build().unwrap();
        let predicate = is_not_null!(&attributes, "country");

        assert_eq!(Some(false), predicate.evaluate(&event));
    }

    #[test]
    fn return_true_on_empty_check_for_empty_list_variable() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_integer_list("segment_ids", &[]).unwrap();
        let event = builder.build().unwrap();
        let predicate = is_empty!(&attributes, "segment_ids");

        assert_eq!(Some(true), predicate.evaluate(&event));
    }

    #[test]
    fn return_false_on_empty_check_for_non_empty_list_variable() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder
            .with_integer_list("segment_ids", &[1, 2, 3])
            .unwrap();
        let event = builder.build().unwrap();
        let predicate = is_empty!(&attributes, "segment_ids");

        assert_eq!(Some(false), predicate.evaluate(&event));
    }

    #[test]
    fn return_false_on_not_empty_check_for_empty_list_variable() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_integer_list("segment_ids", &[]).unwrap();
        let event = builder.build().unwrap();
        let predicate = is_not_empty!(&attributes, "segment_ids");

        assert_eq!(Some(false), predicate.evaluate(&event));
    }

    #[test]
    fn return_true_on_not_empty_check_for_non_empty_list_variable() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder
            .with_integer_list("segment_ids", &[1, 2, 3])
            .unwrap();
        let event = builder.build().unwrap();
        let predicate = is_not_empty!(&attributes, "segment_ids");

        assert_eq!(Some(true), predicate.evaluate(&event));
    }

    #[test]
    fn return_false_when_searching_for_an_element_in_an_empty_set() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_integer("exchange_id", AN_EXCHANGE_ID).unwrap();
        let event = builder.build().unwrap();
        let predicate = set_in!(&attributes, "exchange_id", integer_list!(vec![]));

        assert_eq!(Some(false), predicate.evaluate(&event));
    }

    #[test]
    fn return_false_when_searching_for_an_element_in_a_set_that_does_not_contain_said_element() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_integer("exchange_id", AN_EXCHANGE_ID).unwrap();
        let event = builder.build().unwrap();
        let predicate = set_in!(
            &attributes,
            "exchange_id",
            integer_list!((1..AN_EXCHANGE_ID).collect())
        );

        assert_eq!(Some(false), predicate.evaluate(&event));
    }

    #[test]
    fn return_true_when_searching_for_an_element_in_a_set_that_contains_said_element() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_integer("exchange_id", AN_EXCHANGE_ID).unwrap();
        let event = builder.build().unwrap();
        let predicate = set_in!(
            &attributes,
            "exchange_id",
            integer_list!((1..=50).collect())
        );

        assert_eq!(Some(true), predicate.evaluate(&event));
    }

    #[test]
    fn return_true_when_looking_for_the_absence_of_an_element_in_an_empty_set() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_integer("exchange_id", AN_EXCHANGE_ID).unwrap();
        let event = builder.build().unwrap();
        let predicate = Predicate::new(
            &attributes,
            "exchange_id",
            PredicateKind::Set(SetOperator::NotIn, ListLiteral::IntegerList(vec![])),
        )
        .unwrap();

        assert_eq!(Some(true), predicate.evaluate(&event));
    }

    #[test]
    fn return_true_when_looking_for_the_absence_of_an_element_in_a_set_that_does_not_contain_said_element(
    ) {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_integer("exchange_id", AN_EXCHANGE_ID).unwrap();
        let event = builder.build().unwrap();
        let predicate = set_not_in!(
            &attributes,
            "exchange_id",
            integer_list!((1..AN_EXCHANGE_ID).collect())
        );

        assert_eq!(Some(true), predicate.evaluate(&event));
    }

    #[test]
    fn return_false_when_looking_for_the_absence_of_an_element_in_a_set_that_contains_said_element()
    {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_integer("exchange_id", AN_EXCHANGE_ID).unwrap();
        let event = builder.build().unwrap();
        let predicate = set_not_in!(
            &attributes,
            "exchange_id",
            integer_list!((1..=50).collect())
        );

        assert_eq!(Some(false), predicate.evaluate(&event));
    }

    #[test]
    fn return_true_when_checking_for_equality_for_two_elements_that_are_equal() {
        let attributes = define_attributes();
        let mut strings = StringTable::new();
        let string_id = strings.get_or_update(A_COUNTRY);
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_string("country", A_COUNTRY).unwrap();
        let event = builder.build().unwrap();
        let predicate = equal!(&attributes, "country", primitive_string!(string_id));

        assert_eq!(Some(true), predicate.evaluate(&event));
    }

    #[test]
    fn return_false_when_checking_for_equality_for_two_elements_that_are_not_equal() {
        let attributes = define_attributes();
        let mut strings = StringTable::new();
        let _ = strings.get_or_update(A_COUNTRY);
        let another_string_id = strings.get_or_update(ANOTHER_COUNTRY);
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_string("country", A_COUNTRY).unwrap();
        let event = builder.build().unwrap();
        let predicate = equal!(&attributes, "country", primitive_string!(another_string_id));

        assert_eq!(Some(false), predicate.evaluate(&event));
    }

    #[test]
    fn return_false_when_checking_for_inequality_for_two_elements_that_are_equal() {
        let attributes = define_attributes();
        let mut strings = StringTable::new();
        let string_id = strings.get_or_update(A_COUNTRY);
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_string("country", A_COUNTRY).unwrap();
        let event = builder.build().unwrap();
        let predicate = not_equal!(&attributes, "country", primitive_string!(string_id));

        assert_eq!(Some(false), predicate.evaluate(&event));
    }

    #[test]
    fn return_true_when_checking_for_inequality_for_two_elements_that_are_not_equal() {
        let attributes = define_attributes();
        let mut strings = StringTable::new();
        let _ = strings.get_or_update(A_COUNTRY);
        let another_string_id = strings.get_or_update(ANOTHER_COUNTRY);
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_string("country", A_COUNTRY).unwrap();
        let event = builder.build().unwrap();
        let predicate = not_equal!(&attributes, "country", primitive_string!(another_string_id));

        assert_eq!(Some(true), predicate.evaluate(&event));
    }

    #[test]
    fn can_check_if_value_lesser_than_another_value_is_less_than_the_other_value() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_float("bidfloor", 55, 3).unwrap();
        let event = builder.build().unwrap();

        let predicate = less_than!(
            &attributes,
            "bidfloor",
            comparison_float!(Decimal::new(2, 0))
        );

        assert_eq!(Some(true), predicate.evaluate(&event));
    }

    #[test]
    fn can_check_if_value_lesser_or_equal_than_another_value_is_less_or_equal_than_the_other_value()
    {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_float("bidfloor", 55, 3).unwrap();
        let event = builder.build().unwrap();

        let predicate = less_than_equal!(
            &attributes,
            "bidfloor",
            comparison_float!(Decimal::new(2, 0))
        );

        assert_eq!(Some(true), predicate.evaluate(&event));
    }

    #[test]
    fn can_check_if_value_greater_than_another_value_is_greater_than_the_other_value() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_float("bidfloor", 55, 3).unwrap();
        let event = builder.build().unwrap();

        let predicate = greater_than!(
            &attributes,
            "bidfloor",
            comparison_float!(Decimal::new(55, 4))
        );

        assert_eq!(Some(true), predicate.evaluate(&event));
    }

    #[test]
    fn can_check_if_value_greater_than_equal_another_value_is_greater_than_equal_the_other_value() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_float("bidfloor", 55, 3).unwrap();
        let event = builder.build().unwrap();

        let predicate = greater_than_equal!(
            &attributes,
            "bidfloor",
            comparison_float!(Decimal::new(44, 4))
        );

        assert_eq!(Some(true), predicate.evaluate(&event));
    }

    #[test]
    fn return_false_when_checking_if_subset_of_an_empty_list() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder
            .with_string_list("deals", &["deal-1", "deal-2"])
            .unwrap();
        let event = builder.build().unwrap();

        let predicate = all_of!(&attributes, "deals", string_list!(vec![]));

        assert_eq!(Some(false), predicate.evaluate(&event));
    }

    #[test]
    fn return_true_when_checking_if_empty_list_is_subset_of_a_list() {
        let attributes = define_attributes();
        let mut strings = StringTable::new();
        let id = strings.get_or_update("deal-1");
        let another_id = strings.get_or_update("deal-2");
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_string_list("deals", &[]).unwrap();
        let event = builder.build().unwrap();

        let predicate = all_of!(&attributes, "deals", string_list!(vec![id, another_id]));

        assert_eq!(Some(true), predicate.evaluate(&event));
    }

    #[test]
    fn return_false_when_checking_if_list_that_is_bigger_than_the_other_list_is_a_subset() {
        let attributes = define_attributes();
        let mut strings = StringTable::new();
        let id = strings.get_or_update("deal-1");
        let another_id = strings.get_or_update("deal-2");
        let _ = strings.get_or_update("deal-3");
        let mut builder = an_event_builder(&attributes, &strings);
        builder
            .with_string_list("deals", &["deal-1", "deal-2", "deal-3"])
            .unwrap();
        let event = builder.build().unwrap();

        let predicate = all_of!(&attributes, "deals", string_list!(vec![id, another_id]));

        assert_eq!(Some(false), predicate.evaluate(&event));
    }

    #[test]
    fn return_false_when_checking_if_list_whose_elements_are_not_all_contained_by_the_other_list_is_a_subset(
    ) {
        let attributes = define_attributes();
        let mut strings = StringTable::new();
        let id = strings.get_or_update("deal-1");
        let another_id = strings.get_or_update("deal-2");
        let a_third_id = strings.get_or_update("deal-3");
        let a_fourth_id = strings.get_or_update("deal-4");
        let mut builder = an_event_builder(&attributes, &strings);
        builder
            .with_string_list("deals", &["deal-3", "deal-4"])
            .unwrap();
        let event = builder.build().unwrap();
        let mut builder = an_event_builder(&attributes, &strings);
        builder
            .with_string_list("deals", &["deal-1", "deal-2"])
            .unwrap();
        let event_2 = builder.build().unwrap();

        let predicate = all_of!(&attributes, "deals", string_list!(vec![id, another_id]));
        let predicate_2 = all_of!(
            &attributes,
            "deals",
            string_list!(vec![a_third_id, a_fourth_id])
        );

        assert_eq!(Some(false), predicate.evaluate(&event));
        assert_eq!(Some(false), predicate_2.evaluate(&event_2));
    }

    #[test]
    fn return_true_when_checking_if_list_whose_elements_are_all_contained_by_the_other_list_is_a_subset(
    ) {
        let attributes = define_attributes();
        let mut strings = StringTable::new();
        let id = strings.get_or_update("deal-1");
        let another_id = strings.get_or_update("deal-2");
        let a_third_id = strings.get_or_update("deal-3");
        let a_fourth_id = strings.get_or_update("deal-4");
        let mut builder = an_event_builder(&attributes, &strings);
        builder
            .with_string_list("deals", &["deal-3", "deal-4"])
            .unwrap();
        let event = builder.build().unwrap();

        let predicate = all_of!(
            &attributes,
            "deals",
            string_list!(vec![id, another_id, a_third_id, a_fourth_id])
        );

        assert_eq!(Some(true), predicate.evaluate(&event));
    }

    #[test]
    fn return_false_when_checking_for_one_of_and_list_attribute_is_empty() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_integer_list("segment_ids", &[]).unwrap();
        let event = builder.build().unwrap();

        let predicate = one_of!(&attributes, "segment_ids", integer_list!(vec![1, 2, 3, 4]));

        assert_eq!(Some(false), predicate.evaluate(&event));
    }

    #[test]
    fn return_false_when_checking_for_one_of_and_predicate_list_is_empty() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder
            .with_integer_list("segment_ids", &[1, 2, 3])
            .unwrap();
        let event = builder.build().unwrap();

        let predicate = one_of!(&attributes, "segment_ids", integer_list!(vec![]));

        assert_eq!(Some(false), predicate.evaluate(&event));
    }

    #[test]
    fn return_true_when_one_of_the_value_of_the_first_is_contained_in_the_other_list() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder
            .with_integer_list("segment_ids", &[2, 4, 6])
            .unwrap();
        let event = builder.build().unwrap();

        let predicate = one_of!(&attributes, "segment_ids", integer_list!(vec![1, 3, 6]));

        assert_eq!(Some(true), predicate.evaluate(&event));
    }

    #[test]
    fn return_false_when_none_of_the_value_of_the_first_is_contained_in_the_other_list() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder
            .with_integer_list("segment_ids", &[2, 4, 6])
            .unwrap();
        let event = builder.build().unwrap();

        let predicate = one_of!(&attributes, "segment_ids", integer_list!(vec![1, 3, 5]));

        assert_eq!(Some(false), predicate.evaluate(&event));
    }

    #[test]
    fn return_true_when_none_of_the_value_of_the_first_is_contained_in_the_other_list() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder
            .with_integer_list("segment_ids", &[2, 4, 6])
            .unwrap();
        let event = builder.build().unwrap();

        let predicate = none_of!(&attributes, "segment_ids", integer_list!(vec![1, 3, 5]));

        assert_eq!(Some(true), predicate.evaluate(&event));
    }

    #[test]
    fn return_false_when_one_of_the_value_of_the_first_is_contained_in_the_other_list() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder
            .with_integer_list("segment_ids", &[2, 3, 6])
            .unwrap();
        let event = builder.build().unwrap();

        let predicate = none_of!(&attributes, "segment_ids", integer_list!(vec![1, 3, 5]));

        assert_eq!(Some(false), predicate.evaluate(&event));
    }

    #[test]
    fn return_true_when_checking_if_not_subset_of_the_other_list_and_the_first_list_is_empty() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_integer_list("segment_ids", &[]).unwrap();
        let event = builder.build().unwrap();

        let predicate = none_of!(&attributes, "segment_ids", integer_list!(vec![1, 3, 5]));

        assert_eq!(Some(true), predicate.evaluate(&event));
    }

    #[test]
    fn return_true_when_checking_if_not_subset_of_the_other_list_and_the_other_list_is_empty() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder
            .with_integer_list("segment_ids", &[1, 2, 3])
            .unwrap();
        let event = builder.build().unwrap();

        let predicate = none_of!(&attributes, "segment_ids", integer_list!(vec![]));

        assert_eq!(Some(true), predicate.evaluate(&event));
    }

    #[test]
    fn return_none_when_the_attribute_is_undefined() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_undefined("segment_ids").unwrap();
        let event = builder.build().unwrap();

        let predicate = none_of!(&attributes, "segment_ids", integer_list!(vec![]));

        assert_eq!(None, predicate.evaluate(&event));
    }

    #[test]
    fn can_negate_a_variable() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_boolean("private", true).unwrap();
        let event = builder.build().unwrap();
        let predicate = variable!(&attributes, "private");

        assert_eq!(
            predicate.evaluate(&event).map(std::ops::Not::not),
            (!predicate).evaluate(&event)
        )
    }

    #[test]
    fn can_negate_a_negated_variable() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_boolean("private", true).unwrap();
        let event = builder.build().unwrap();
        let predicate = negated_variable!(&attributes, "private");

        assert_eq!(
            predicate.evaluate(&event).map(std::ops::Not::not),
            (!predicate).evaluate(&event)
        )
    }

    #[test]
    fn can_negate_a_null_check() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_boolean("private", true).unwrap();
        let event = builder.build().unwrap();
        let predicate = is_null!(&attributes, "private");

        assert_eq!(
            predicate.evaluate(&event).map(std::ops::Not::not),
            (!predicate).evaluate(&event)
        )
    }

    #[test]
    fn can_negate_a_not_null_check() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_boolean("private", true).unwrap();
        let event = builder.build().unwrap();
        let predicate = is_not_null!(&attributes, "private");

        assert_eq!(
            predicate.evaluate(&event).map(std::ops::Not::not),
            (!predicate).evaluate(&event)
        )
    }

    #[test]
    fn can_negate_an_empty_check() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder
            .with_integer_list("segment_ids", &[1, 2, 3])
            .unwrap();
        let event = builder.build().unwrap();
        let predicate = is_empty!(&attributes, "segment_ids");

        assert_eq!(
            predicate.evaluate(&event).map(std::ops::Not::not),
            (!predicate).evaluate(&event)
        )
    }

    #[test]
    fn can_negate_a_not_empty_check() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder
            .with_integer_list("segment_ids", &[1, 2, 3])
            .unwrap();
        let event = builder.build().unwrap();
        let predicate = is_not_empty!(&attributes, "segment_ids");

        assert_eq!(
            predicate.evaluate(&event).map(std::ops::Not::not),
            (!predicate).evaluate(&event)
        )
    }

    #[test]
    fn can_negate_a_set_in_predicate() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_integer("exchange_id", AN_EXCHANGE_ID).unwrap();
        let event = builder.build().unwrap();
        let predicate = set_in!(&attributes, "exchange_id", integer_list!(vec![]));

        assert_eq!(
            predicate.evaluate(&event).map(std::ops::Not::not),
            (!predicate).evaluate(&event)
        )
    }

    #[test]
    fn can_negate_a_set_not_in_predicate() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_integer("exchange_id", AN_EXCHANGE_ID).unwrap();
        let event = builder.build().unwrap();
        let predicate = set_not_in!(&attributes, "exchange_id", integer_list!(vec![]));

        assert_eq!(
            predicate.evaluate(&event).map(std::ops::Not::not),
            (!predicate).evaluate(&event)
        )
    }

    #[test]
    fn can_negate_an_equal_predicate() {
        let attributes = define_attributes();
        let mut strings = StringTable::new();
        let string_id = strings.get_or_update(A_COUNTRY);
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_string("country", A_COUNTRY).unwrap();
        let event = builder.build().unwrap();
        let predicate = equal!(&attributes, "country", primitive_string!(string_id));

        assert_eq!(
            predicate.evaluate(&event).map(std::ops::Not::not),
            (!predicate).evaluate(&event)
        )
    }

    #[test]
    fn can_negate_a_not_equal_predicate() {
        let attributes = define_attributes();
        let mut strings = StringTable::new();
        let string_id = strings.get_or_update(A_COUNTRY);
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_string("country", A_COUNTRY).unwrap();
        let event = builder.build().unwrap();
        let predicate = not_equal!(&attributes, "country", primitive_string!(string_id));

        assert_eq!(
            predicate.evaluate(&event).map(std::ops::Not::not),
            (!predicate).evaluate(&event)
        )
    }

    #[test]
    fn can_negate_a_less_than_predicate() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_integer("exchange_id", AN_EXCHANGE_ID).unwrap();
        let event = builder.build().unwrap();
        let predicate = less_than!(&attributes, "exchange_id", comparison_integer!(0));

        assert_eq!(
            predicate.evaluate(&event).map(std::ops::Not::not),
            (!predicate).evaluate(&event)
        )
    }

    #[test]
    fn can_negate_a_less_than_equal_predicate() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_integer("exchange_id", AN_EXCHANGE_ID).unwrap();
        let event = builder.build().unwrap();
        let predicate = less_than_equal!(&attributes, "exchange_id", comparison_integer!(0));

        assert_eq!(
            predicate.evaluate(&event).map(std::ops::Not::not),
            (!predicate).evaluate(&event)
        )
    }

    #[test]
    fn can_negate_a_greater_than_predicate() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_integer("exchange_id", AN_EXCHANGE_ID).unwrap();
        let event = builder.build().unwrap();
        let predicate = greater_than!(&attributes, "exchange_id", comparison_integer!(0));

        assert_eq!(
            predicate.evaluate(&event).map(std::ops::Not::not),
            (!predicate).evaluate(&event)
        )
    }

    #[test]
    fn can_negate_a_greater_than_equal_predicate() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_integer("exchange_id", AN_EXCHANGE_ID).unwrap();
        let event = builder.build().unwrap();
        let predicate = greater_than_equal!(&attributes, "exchange_id", comparison_integer!(0));

        assert_eq!(
            predicate.evaluate(&event).map(std::ops::Not::not),
            (!predicate).evaluate(&event)
        )
    }

    #[test]
    fn can_negate_a_one_of_predicate() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_integer_list("segment_ids", &[]).unwrap();
        let event = builder.build().unwrap();
        let predicate = one_of!(&attributes, "segment_ids", integer_list!(vec![1, 2, 3, 4]));

        assert_eq!(
            predicate.evaluate(&event).map(std::ops::Not::not),
            (!predicate).evaluate(&event)
        )
    }

    #[test]
    fn can_negate_a_none_of_predicate() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_integer_list("segment_ids", &[]).unwrap();
        let event = builder.build().unwrap();
        let predicate = none_of!(&attributes, "segment_ids", integer_list!(vec![1, 2, 3, 4]));

        assert_eq!(
            predicate.evaluate(&event).map(std::ops::Not::not),
            (!predicate).evaluate(&event)
        )
    }

    #[test]
    fn can_negate_an_all_of_predicate() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_integer_list("segment_ids", &[]).unwrap();
        let event = builder.build().unwrap();
        let predicate = all_of!(&attributes, "segment_ids", integer_list!(vec![1, 2, 3, 4]));

        assert_eq!(
            predicate.evaluate(&event).map(std::ops::Not::not),
            (!predicate).evaluate(&event)
        )
    }

    proptest! {
        #[test]
        #[cfg_attr(miri, ignore)]
        fn can_find_an_element_if_it_is_present_in_the_input((value, index, _) in vec_and_index()) {
            let attributes = define_attributes();
            let strings = StringTable::new();
            let mut builder = an_event_builder(&attributes, &strings);
            builder
                .with_integer("exchange_id", value[index])
                .unwrap();
            let event = builder.build().unwrap();

            let predicate = set_in!(&attributes, "exchange_id", integer_list!(value));

            assert_eq!(Some(true), predicate.evaluate(&event));
        }

        #[test]
        #[cfg_attr(miri, ignore)]
        fn can_find_an_element_common_from_both_lists((value, index, _) in vec_and_index(), (mut variable, variable_index, _) in vec_and_index()) {
            variable[variable_index] = value[index];
            let variable = variable.into_iter().sorted().unique().collect_vec();

            let attributes = define_attributes();
            let strings = StringTable::new();
            let mut builder = an_event_builder(&attributes, &strings);
            builder
                .with_integer_list("segment_ids", &variable)
                .unwrap();
            let event = builder.build().unwrap();

            let predicate = one_of!(&attributes, "segment_ids", integer_list!(value));

            assert_eq!(Some(true), predicate.evaluate(&event));
        }

        #[test]
        #[cfg_attr(miri, ignore)]
        fn can_find_a_subset_if_it_is_present_in_the_input((value, index, index_2) in vec_and_index()) {
            let attributes = define_attributes();
            let strings = StringTable::new();
            let mut builder = an_event_builder(&attributes, &strings);
            let start = std::cmp::min(index, index_2);
            let end = std::cmp::max(index, index_2);
            builder
                .with_integer_list("segment_ids", &value[start..end])
                .unwrap();
            let event = builder.build().unwrap();

            let predicate = all_of!(&attributes, "segment_ids", integer_list!(value));

            assert_eq!(Some(true), predicate.evaluate(&event));
        }
    }

    fn define_attributes() -> AttributeTable {
        let definitions = vec![
            AttributeDefinition::string_list("deals"),
            AttributeDefinition::string("deal"),
            AttributeDefinition::float("bidfloor"),
            AttributeDefinition::integer("exchange_id"),
            AttributeDefinition::boolean("private"),
            AttributeDefinition::integer_list("segment_ids"),
            AttributeDefinition::string("country"),
        ];
        AttributeTable::new(&definitions).unwrap()
    }

    fn an_event_builder<'a>(
        attributes: &'a AttributeTable,
        strings: &'a StringTable,
    ) -> EventBuilder<'a> {
        let mut builder = EventBuilder::new(attributes, strings);
        assert!(builder
            .with_string_list("deals", &["deal-1", "deal-2"])
            .is_ok());
        assert!(builder.with_float("bidfloor", 1, 0).is_ok());
        assert!(builder.with_integer("exchange_id", AN_EXCHANGE_ID).is_ok());
        assert!(builder.with_boolean("private", true).is_ok());
        assert!(builder.with_integer_list("segment_ids", &[1, 2, 3]).is_ok());
        assert!(builder.with_string("country", A_COUNTRY).is_ok());
        builder
    }

    fn vec_and_index() -> impl Strategy<Value = (Vec<i64>, usize, usize)> {
        prop::collection::vec(any::<i64>(), 1..100).prop_flat_map(|vec| {
            let vec = vec.into_iter().sorted().unique().collect_vec();
            let length = vec.len();
            (Just(vec), 0..length, 0..length)
        })
    }
}
