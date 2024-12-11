use crate::{
    predicates::PredicateKind,
    strings::{StringId, StringTable},
};
use itertools::Itertools;
use rust_decimal::Decimal;
use std::{collections::HashMap, ops::Index};
use thiserror::Error;

#[derive(Error, PartialEq, Debug)]
pub enum EventError {
    #[error("attribute {0} has already been defined")]
    AlreadyPresent(String),
    #[error("event is missing some attributes")]
    MissingAttributes,
    #[error("ABE refers to non-existing attribute '{0:?}'")]
    NonExistingAttribute(String),
    #[error("{name:?}: wrong types => expected: {expected:?}, found: {actual:?}")]
    WrongType {
        name: String,
        expected: AttributeKind,
        actual: AttributeKind,
    },
    #[error("{name:?}: mismatching types => expected: {expected:?}, found: {actual:?}")]
    MismatchingTypes {
        name: String,
        expected: AttributeKind,
        actual: PredicateKind,
    },
}

/// An [`Event`] builder
///
/// During the builder creation, it will set all the attributes to `undefined`. If some attributes
/// are not assigned, they will be left `undefined`.
pub struct EventBuilder<'atree> {
    by_ids: Vec<AttributeValue>,
    attributes: &'atree AttributeTable,
    strings: &'atree StringTable,
}

impl<'atree> EventBuilder<'atree> {
    pub(crate) fn new(attributes: &'atree AttributeTable, strings: &'atree StringTable) -> Self {
        Self {
            attributes,
            strings,
            by_ids: vec![AttributeValue::Undefined; attributes.len()],
        }
    }

    /// Build the corresponding [`Event`].
    ///
    /// By default, the non-assigned attributes will be undefined.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use a_tree::{ATree, AttributeDefinition};
    ///
    /// let definitions = [
    ///     AttributeDefinition::boolean("private"),
    ///     AttributeDefinition::integer("exchange_id"),
    ///     AttributeDefinition::string_list("deal_ids"),
    /// ];
    /// let atree = ATree::new(&definitions).unwrap();
    ///
    /// let mut builder = atree.make_event();
    /// builder.with_integer("exchange_id", 1).unwrap();
    /// builder.with_boolean("private", false).unwrap();
    ///
    /// // The returned `Event` will have its `deal_ids` attribute `undefined` since it was not set
    /// // by the builder
    /// let event = builder.build().unwrap();
    /// ```
    pub fn build(self) -> Result<Event, EventError> {
        Ok(Event(self.by_ids))
    }

    /// Set the specified boolean attribute.
    pub fn with_boolean(&mut self, name: &str, value: bool) -> Result<(), EventError> {
        self.add_value(name, AttributeKind::Boolean, || {
            AttributeValue::Boolean(value)
        })
    }

    /// Set the specified integer attribute.
    pub fn with_integer(&mut self, name: &str, value: i64) -> Result<(), EventError> {
        self.add_value(name, AttributeKind::Integer, || {
            AttributeValue::Integer(value)
        })
    }

    /// Set the specified float attribute.
    pub fn with_float(&mut self, name: &str, value: Decimal) -> Result<(), EventError> {
        self.add_value(name, AttributeKind::Float, || AttributeValue::Float(value))
    }

    /// Set the specified string attribute.
    pub fn with_string(&mut self, name: &str, value: &str) -> Result<(), EventError> {
        self.add_value(name, AttributeKind::String, || {
            let string_index = self.strings.get(value);
            AttributeValue::String(string_index)
        })
    }

    /// Set the specified list of integers attribute.
    pub fn with_integer_list(&mut self, name: &str, value: &[i64]) -> Result<(), EventError> {
        self.add_value(name, AttributeKind::IntegerList, || {
            let values = value.iter().sorted().unique().cloned().collect_vec();
            AttributeValue::IntegerList(values)
        })
    }

    /// Set the specified attribute to `undefined`.
    pub fn with_undefined(&mut self, name: &str) -> Result<(), EventError> {
        let index = self
            .attributes
            .by_name(name)
            .ok_or_else(|| EventError::NonExistingAttribute(name.to_string()))?;
        self.by_ids[index.0] = AttributeValue::Undefined;
        Ok(())
    }

    /// Set the specified string list attribute.
    pub fn with_string_list(&mut self, name: &str, values: &[&str]) -> Result<(), EventError> {
        self.add_value(name, AttributeKind::StringList, || {
            let values: Vec<_> = values
                .iter()
                .map(|v| self.strings.get(v))
                .sorted()
                .unique()
                .collect();
            AttributeValue::StringList(values)
        })
    }

    fn add_value<F>(&mut self, name: &str, actual: AttributeKind, f: F) -> Result<(), EventError>
    where
        F: FnOnce() -> AttributeValue,
    {
        let index = self
            .attributes
            .by_name(name)
            .ok_or_else(|| EventError::NonExistingAttribute(name.to_string()))?;
        let expected = self.attributes.by_id(index);
        if expected != actual {
            return Err(EventError::WrongType {
                name: name.to_owned(),
                expected,
                actual,
            });
        }
        self.by_ids[index.0] = f();
        Ok(())
    }
}

/// An event that can be used by the [`crate::atree::ATree`] structure to match arbitrary boolean
/// expressions
pub struct Event(Vec<AttributeValue>);

impl Index<AttributeId> for Event {
    type Output = AttributeValue;

    #[inline]
    fn index(&self, index: AttributeId) -> &Self::Output {
        &self.0[index.0]
    }
}

#[derive(Clone, Debug)]
pub enum AttributeValue {
    Boolean(bool),
    Integer(i64),
    Float(Decimal),
    String(StringId),
    IntegerList(Vec<i64>),
    StringList(Vec<StringId>),
    Undefined,
}

#[derive(Debug)]
pub struct AttributeTable {
    by_names: HashMap<String, AttributeId>,
    by_ids: Vec<AttributeKind>,
}

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd, Debug, Hash)]
pub struct AttributeId(usize);

impl AttributeTable {
    pub fn new(definitions: &[AttributeDefinition]) -> Result<Self, EventError> {
        let size = definitions.len();
        let mut by_names = HashMap::with_capacity(size);
        let mut by_ids = Vec::with_capacity(size);
        for (i, definition) in definitions.iter().enumerate() {
            let name = definition.name.to_owned();
            if by_names.contains_key(&name) {
                return Err(EventError::AlreadyPresent(name));
            }

            by_names.insert(name, AttributeId(i));
            by_ids.push(definition.kind.clone());
        }

        Ok(Self { by_names, by_ids })
    }

    #[inline]
    pub fn by_name(&self, name: &str) -> Option<AttributeId> {
        self.by_names.get(name).cloned()
    }

    #[inline]
    pub fn by_id(&self, id: AttributeId) -> AttributeKind {
        self.by_ids[id.0].clone()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.by_ids.len()
    }
}

/// The definition of an attribute that is usable by the [`crate::atree::ATree`]
#[derive(Clone)]
pub struct AttributeDefinition {
    name: String,
    kind: AttributeKind,
}

#[derive(Clone, PartialEq, Debug)]
pub enum AttributeKind {
    Boolean,
    Integer,
    Float,
    String,
    IntegerList,
    StringList,
}

impl AttributeDefinition {
    /// Create a boolean attribute definition.
    pub fn boolean(name: &str) -> Self {
        let kind = AttributeKind::Boolean;
        Self {
            name: name.to_owned(),
            kind,
        }
    }

    /// Create an integer attribute definition.
    pub fn integer(name: &str) -> Self {
        let kind = AttributeKind::Integer;
        Self {
            name: name.to_owned(),
            kind,
        }
    }

    /// Create a float attribute definition.
    pub fn float(name: &str) -> Self {
        let kind = AttributeKind::Float;
        Self {
            name: name.to_owned(),
            kind,
        }
    }

    /// Create a string attribute definition.
    pub fn string(name: &str) -> Self {
        let kind = AttributeKind::String;
        Self {
            name: name.to_owned(),
            kind,
        }
    }

    /// Create a list of integers attribute definition.
    pub fn integer_list(name: &str) -> Self {
        let kind = AttributeKind::IntegerList;
        Self {
            name: name.to_owned(),
            kind,
        }
    }

    /// Create a list of strings attribute definition.
    pub fn string_list(name: &str) -> Self {
        let kind = AttributeKind::StringList;
        Self {
            name: name.to_owned(),
            kind,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_create_an_attribute_table_with_no_attributes() {
        assert!(AttributeTable::new(&[]).is_ok())
    }

    #[test]
    fn can_create_an_attribute_table_with_some_attributes() {
        let definitions = [
            AttributeDefinition::boolean("private"),
            AttributeDefinition::string_list("deals"),
            AttributeDefinition::integer("exchange_id"),
            AttributeDefinition::float("bidfloor"),
            AttributeDefinition::string("country"),
            AttributeDefinition::integer_list("segment_ids"),
        ];

        assert!(AttributeTable::new(&definitions).is_ok());
    }

    #[test]
    fn return_an_error_on_duplicate_definitions() {
        let definitions = [
            AttributeDefinition::boolean("private"),
            AttributeDefinition::string("country"),
            AttributeDefinition::string_list("deals"),
            AttributeDefinition::integer("exchange_id"),
            AttributeDefinition::float("bidfloor"),
            AttributeDefinition::integer("country"),
            AttributeDefinition::integer_list("segment_ids"),
        ];

        assert!(AttributeTable::new(&definitions).is_err());
    }

    #[test]
    fn can_add_a_boolean_attribute_value() {
        let attributes = AttributeTable::new(&[AttributeDefinition::boolean("private")]).unwrap();
        let strings = StringTable::new();
        let mut event_builder = EventBuilder::new(&attributes, &strings);

        let result = event_builder.with_boolean("private", true);

        assert!(result.is_ok());
    }

    #[test]
    fn can_add_an_integer_attribute_value() {
        let attributes =
            AttributeTable::new(&[AttributeDefinition::integer("exchange_id")]).unwrap();
        let strings = StringTable::new();
        let mut event_builder = EventBuilder::new(&attributes, &strings);

        let result = event_builder.with_integer("exchange_id", 1);

        assert!(result.is_ok());
    }

    #[test]
    fn can_add_a_float_attribute_value() {
        let attributes = AttributeTable::new(&[AttributeDefinition::float("bidfloor")]).unwrap();
        let strings = StringTable::new();
        let mut event_builder = EventBuilder::new(&attributes, &strings);

        let result = event_builder.with_float("bidfloor", Decimal::new(1, 0));

        assert!(result.is_ok());
    }

    #[test]
    fn can_add_a_string_attribute_value() {
        let attributes = AttributeTable::new(&[AttributeDefinition::string("country")]).unwrap();
        let strings = StringTable::new();
        let mut event_builder = EventBuilder::new(&attributes, &strings);

        let result = event_builder.with_string("country", "US");

        assert!(result.is_ok());
    }

    #[test]
    fn can_add_an_integer_list_attribute_value() {
        let attributes =
            AttributeTable::new(&[AttributeDefinition::integer_list("segment_ids")]).unwrap();
        let strings = StringTable::new();
        let mut event_builder = EventBuilder::new(&attributes, &strings);

        let result = event_builder.with_integer_list("segment_ids", &[1, 2, 3]);

        assert!(result.is_ok());
    }

    #[test]
    fn can_add_an_string_list_attribute_value() {
        let attributes =
            AttributeTable::new(&[AttributeDefinition::string_list("deal_ids")]).unwrap();
        let strings = StringTable::new();
        let mut event_builder = EventBuilder::new(&attributes, &strings);

        let result = event_builder.with_string_list("deal_ids", &["deal-1", "deal-2"]);

        assert!(result.is_ok());
    }

    #[test]
    fn return_an_error_when_adding_a_non_existing_attribute() {
        let attributes =
            AttributeTable::new(&[AttributeDefinition::string_list("deal_ids")]).unwrap();
        let strings = StringTable::new();
        let mut event_builder = EventBuilder::new(&attributes, &strings);

        let result = event_builder.with_boolean("non_existing", true);

        assert!(matches!(result, Err(EventError::NonExistingAttribute(_))));
    }

    #[test]
    fn can_create_an_event_with_no_attributes() {
        let attributes = AttributeTable::new(&[]).unwrap();
        let strings = StringTable::new();
        let event_builder = EventBuilder::new(&attributes, &strings);

        assert!(event_builder.build().is_ok());
    }

    #[test]
    fn can_create_an_event_with_attributes() {
        let attributes = AttributeTable::new(&[
            AttributeDefinition::boolean("private"),
            AttributeDefinition::string_list("deals"),
            AttributeDefinition::integer("exchange_id"),
            AttributeDefinition::float("bidfloor"),
            AttributeDefinition::string("country"),
            AttributeDefinition::integer_list("segment_ids"),
        ])
        .unwrap();
        let strings = StringTable::new();
        let mut builder = EventBuilder::new(&attributes, &strings);

        assert!(builder.with_boolean("private", true).is_ok());
        assert!(builder
            .with_string_list("deals", &["deal-1", "deal-2"])
            .is_ok());
        assert!(builder.with_integer("exchange_id", 1).is_ok());
        assert!(builder.with_float("bidfloor", Decimal::new(1, 0)).is_ok());
        assert!(builder.with_string("country", "US").is_ok());
        assert!(builder.with_integer_list("segment_ids", &[1, 2, 3]).is_ok());

        assert!(builder.build().is_ok());
    }

    #[test]
    fn can_create_an_event_with_a_missing_attribute() {
        let attributes = AttributeTable::new(&[AttributeDefinition::boolean("private")]).unwrap();
        let strings = StringTable::new();
        let event_builder = EventBuilder::new(&attributes, &strings);

        assert!(event_builder.build().is_ok());
    }

    #[test]
    fn return_an_error_when_trying_to_add_an_attribute_with_mismatched_type() {
        let attributes = AttributeTable::new(&[AttributeDefinition::boolean("private")]).unwrap();
        let strings = StringTable::new();
        let mut event_builder = EventBuilder::new(&attributes, &strings);

        let result = event_builder.with_integer("private", 1);

        assert!(result.is_err());
    }
}
