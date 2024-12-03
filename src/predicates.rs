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

    pub fn evaluate(&self, event: &Event) -> Option<bool> {
        let value = event.get(&self.attribute);
        match (&self.kind, value) {
            (PredicateKind::Null(operator), value) => Some(operator.evaluate(value)),
            (_, AttributeValue::Undefined) => None,
            (PredicateKind::Variable, AttributeValue::Boolean(value)) => Some(*value),
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

    fn apply<T: Ord>(&self, haystack: &[T], needle: &T) -> bool {
        match self {
            Self::In => haystack.binary_search(needle).is_ok(),
            Self::NotIn => haystack.binary_search(needle).is_err(),
        }
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
        }
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        events::{AttributeDefinition, AttributeTable, EventBuilder},
        strings::StringTable,
    };

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
        let predicate = Predicate::new(&attributes, "private", PredicateKind::Variable).unwrap();

        assert_eq!(Some(true), predicate.evaluate(&event));
    }

    #[test]
    fn return_false_on_boolean_variable_that_is_false() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_boolean("private", false).unwrap();
        let event = builder.build().unwrap();
        let predicate = Predicate::new(&attributes, "private", PredicateKind::Variable).unwrap();

        assert_eq!(Some(false), predicate.evaluate(&event));
    }

    #[test]
    fn return_false_on_null_check_for_defined_variable() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let event = an_event_builder(&attributes, &strings).build().unwrap();
        let predicate = Predicate::new(
            &attributes,
            "country",
            PredicateKind::Null(NullOperator::IsNull),
        )
        .unwrap();

        assert_eq!(Some(false), predicate.evaluate(&event));
    }

    #[test]
    fn return_true_on_null_check_for_undefined_variable() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_undefined("country").unwrap();
        let event = builder.build().unwrap();
        let predicate = Predicate::new(
            &attributes,
            "country",
            PredicateKind::Null(NullOperator::IsNull),
        )
        .unwrap();

        assert_eq!(Some(true), predicate.evaluate(&event));
    }

    #[test]
    fn return_true_on_not_null_check_for_defined_variable() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let event = an_event_builder(&attributes, &strings).build().unwrap();
        let predicate = Predicate::new(
            &attributes,
            "country",
            PredicateKind::Null(NullOperator::IsNotNull),
        )
        .unwrap();

        assert_eq!(Some(true), predicate.evaluate(&event));
    }

    #[test]
    fn return_true_on_not_null_check_for_undefined_variable() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_undefined("country").unwrap();
        let event = builder.build().unwrap();
        let predicate = Predicate::new(
            &attributes,
            "country",
            PredicateKind::Null(NullOperator::IsNotNull),
        )
        .unwrap();

        assert_eq!(Some(false), predicate.evaluate(&event));
    }

    #[test]
    fn return_true_on_empty_check_for_empty_list_variable() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_integer_list("segment_ids", &[]).unwrap();
        let event = builder.build().unwrap();
        let predicate = Predicate::new(
            &attributes,
            "segment_ids",
            PredicateKind::Null(NullOperator::IsEmpty),
        )
        .unwrap();

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
        let predicate = Predicate::new(
            &attributes,
            "segment_ids",
            PredicateKind::Null(NullOperator::IsEmpty),
        )
        .unwrap();

        assert_eq!(Some(false), predicate.evaluate(&event));
    }

    #[test]
    fn return_false_when_searching_for_an_element_in_an_empty_set() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_integer("exchange_id", AN_EXCHANGE_ID).unwrap();
        let event = builder.build().unwrap();
        let predicate = Predicate::new(
            &attributes,
            "exchange_id",
            PredicateKind::Set(SetOperator::In, ListLiteral::IntegerList(vec![])),
        )
        .unwrap();

        assert_eq!(Some(false), predicate.evaluate(&event));
    }

    #[test]
    fn return_false_when_searching_for_an_element_in_a_set_that_does_not_contain_said_element() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_integer("exchange_id", AN_EXCHANGE_ID).unwrap();
        let event = builder.build().unwrap();
        let predicate = Predicate::new(
            &attributes,
            "exchange_id",
            PredicateKind::Set(
                SetOperator::In,
                ListLiteral::IntegerList((1..AN_EXCHANGE_ID).collect()),
            ),
        )
        .unwrap();

        assert_eq!(Some(false), predicate.evaluate(&event));
    }

    #[test]
    fn return_true_when_searching_for_an_element_in_a_set_that_contains_said_element() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_integer("exchange_id", AN_EXCHANGE_ID).unwrap();
        let event = builder.build().unwrap();
        let predicate = Predicate::new(
            &attributes,
            "exchange_id",
            PredicateKind::Set(
                SetOperator::In,
                ListLiteral::IntegerList((1..=50).collect()),
            ),
        )
        .unwrap();

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
        let predicate = Predicate::new(
            &attributes,
            "exchange_id",
            PredicateKind::Set(
                SetOperator::NotIn,
                ListLiteral::IntegerList((1..AN_EXCHANGE_ID).collect()),
            ),
        )
        .unwrap();

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
        let predicate = Predicate::new(
            &attributes,
            "exchange_id",
            PredicateKind::Set(
                SetOperator::NotIn,
                ListLiteral::IntegerList((1..=50).collect()),
            ),
        )
        .unwrap();

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
        let predicate = Predicate::new(
            &attributes,
            "country",
            PredicateKind::Equality(EqualityOperator::Equal, PrimitiveLiteral::String(string_id)),
        )
        .unwrap();

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
        let predicate = Predicate::new(
            &attributes,
            "country",
            PredicateKind::Equality(
                EqualityOperator::Equal,
                PrimitiveLiteral::String(another_string_id),
            ),
        )
        .unwrap();

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
        let predicate = Predicate::new(
            &attributes,
            "country",
            PredicateKind::Equality(
                EqualityOperator::NotEqual,
                PrimitiveLiteral::String(string_id),
            ),
        )
        .unwrap();

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
        let predicate = Predicate::new(
            &attributes,
            "country",
            PredicateKind::Equality(
                EqualityOperator::NotEqual,
                PrimitiveLiteral::String(another_string_id),
            ),
        )
        .unwrap();

        assert_eq!(Some(true), predicate.evaluate(&event));
    }

    #[test]
    fn can_check_if_value_lesser_than_another_value_is_less_than_the_other_value() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_float("bidfloor", Decimal::new(55, 3)).unwrap();
        let event = builder.build().unwrap();

        let predicate = Predicate::new(
            &attributes,
            "bidfloor",
            PredicateKind::Comparison(
                ComparisonOperator::LessThan,
                ComparisonValue::Float(Decimal::new(2, 0)),
            ),
        )
        .unwrap();

        assert_eq!(Some(true), predicate.evaluate(&event));
    }

    #[test]
    fn can_check_if_value_lesser_or_equal_than_another_value_is_less_or_equal_than_the_other_value()
    {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_float("bidfloor", Decimal::new(55, 3)).unwrap();
        let event = builder.build().unwrap();

        let predicate = Predicate::new(
            &attributes,
            "bidfloor",
            PredicateKind::Comparison(
                ComparisonOperator::LessThanEqual,
                ComparisonValue::Float(Decimal::new(2, 0)),
            ),
        )
        .unwrap();

        assert_eq!(Some(true), predicate.evaluate(&event));
    }

    #[test]
    fn can_check_if_value_greater_than_another_value_is_greater_than_the_other_value() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_float("bidfloor", Decimal::new(55, 3)).unwrap();
        let event = builder.build().unwrap();

        let predicate = Predicate::new(
            &attributes,
            "bidfloor",
            PredicateKind::Comparison(
                ComparisonOperator::GreaterThan,
                ComparisonValue::Float(Decimal::new(55, 4)),
            ),
        )
        .unwrap();

        assert_eq!(Some(true), predicate.evaluate(&event));
    }

    #[test]
    fn can_check_if_value_greater_than_equal_another_value_is_greater_than_equal_the_other_value() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_float("bidfloor", Decimal::new(55, 3)).unwrap();
        let event = builder.build().unwrap();

        let predicate = Predicate::new(
            &attributes,
            "bidfloor",
            PredicateKind::Comparison(
                ComparisonOperator::GreaterThanEqual,
                ComparisonValue::Float(Decimal::new(44, 4)),
            ),
        )
        .unwrap();

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

        let predicate = Predicate::new(
            &attributes,
            "deals",
            PredicateKind::List(ListOperator::AllOf, ListLiteral::StringList(vec![])),
        )
        .unwrap();

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

        let predicate = Predicate::new(
            &attributes,
            "deals",
            PredicateKind::List(
                ListOperator::AllOf,
                ListLiteral::StringList(vec![id, another_id]),
            ),
        )
        .unwrap();

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

        let predicate = Predicate::new(
            &attributes,
            "deals",
            PredicateKind::List(
                ListOperator::AllOf,
                ListLiteral::StringList(vec![id, another_id]),
            ),
        )
        .unwrap();

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

        let predicate = Predicate::new(
            &attributes,
            "deals",
            PredicateKind::List(
                ListOperator::AllOf,
                ListLiteral::StringList(vec![id, another_id]),
            ),
        )
        .unwrap();
        let predicate_2 = Predicate::new(
            &attributes,
            "deals",
            PredicateKind::List(
                ListOperator::AllOf,
                ListLiteral::StringList(vec![a_third_id, a_fourth_id]),
            ),
        )
        .unwrap();

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

        let predicate = Predicate::new(
            &attributes,
            "deals",
            PredicateKind::List(
                ListOperator::AllOf,
                ListLiteral::StringList(vec![id, another_id, a_third_id, a_fourth_id]),
            ),
        )
        .unwrap();

        assert_eq!(Some(true), predicate.evaluate(&event));
    }

    #[test]
    fn return_false_when_checking_for_one_of_and_list_attribute_is_empty() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_integer_list("segment_ids", &[]).unwrap();
        let event = builder.build().unwrap();

        let predicate = Predicate::new(
            &attributes,
            "segment_ids",
            PredicateKind::List(
                ListOperator::OneOf,
                ListLiteral::IntegerList(vec![1, 2, 3, 4]),
            ),
        )
        .unwrap();

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

        let predicate = Predicate::new(
            &attributes,
            "segment_ids",
            PredicateKind::List(ListOperator::OneOf, ListLiteral::IntegerList(vec![])),
        )
        .unwrap();

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

        let predicate = Predicate::new(
            &attributes,
            "segment_ids",
            PredicateKind::List(ListOperator::OneOf, ListLiteral::IntegerList(vec![1, 3, 6])),
        )
        .unwrap();

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

        let predicate = Predicate::new(
            &attributes,
            "segment_ids",
            PredicateKind::List(ListOperator::OneOf, ListLiteral::IntegerList(vec![1, 3, 5])),
        )
        .unwrap();

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

        let predicate = Predicate::new(
            &attributes,
            "segment_ids",
            PredicateKind::List(
                ListOperator::NoneOf,
                ListLiteral::IntegerList(vec![1, 3, 5]),
            ),
        )
        .unwrap();

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

        let predicate = Predicate::new(
            &attributes,
            "segment_ids",
            PredicateKind::List(
                ListOperator::NoneOf,
                ListLiteral::IntegerList(vec![1, 3, 5]),
            ),
        )
        .unwrap();

        assert_eq!(Some(false), predicate.evaluate(&event));
    }

    #[test]
    fn return_true_when_checking_if_not_subset_of_the_other_list_and_the_first_list_is_empty() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_integer_list("segment_ids", &[]).unwrap();
        let event = builder.build().unwrap();

        let predicate = Predicate::new(
            &attributes,
            "segment_ids",
            PredicateKind::List(
                ListOperator::NoneOf,
                ListLiteral::IntegerList(vec![1, 3, 5]),
            ),
        )
        .unwrap();

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

        let predicate = Predicate::new(
            &attributes,
            "segment_ids",
            PredicateKind::List(ListOperator::NoneOf, ListLiteral::IntegerList(vec![])),
        )
        .unwrap();

        assert_eq!(Some(true), predicate.evaluate(&event));
    }

    #[test]
    fn return_none_when_the_attribute_is_undefined() {
        let attributes = define_attributes();
        let strings = StringTable::new();
        let mut builder = an_event_builder(&attributes, &strings);
        builder.with_undefined("segment_ids").unwrap();
        let event = builder.build().unwrap();

        let predicate = Predicate::new(
            &attributes,
            "segment_ids",
            PredicateKind::List(ListOperator::NoneOf, ListLiteral::IntegerList(vec![])),
        )
        .unwrap();

        assert_eq!(None, predicate.evaluate(&event));
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
        assert!(builder.with_float("bidfloor", Decimal::new(1, 0)).is_ok());
        assert!(builder.with_integer("exchange_id", AN_EXCHANGE_ID).is_ok());
        assert!(builder.with_boolean("private", true).is_ok());
        assert!(builder.with_integer_list("segment_ids", &[1, 2, 3]).is_ok());
        assert!(builder.with_string("country", A_COUNTRY).is_ok());
        builder
    }
}
