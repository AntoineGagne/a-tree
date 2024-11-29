use crate::{
    predicates::PredicateKind,
    strings::{StringId, StringTable},
};
use rust_decimal::Decimal;
use std::collections::HashMap;
use thiserror::Error;

#[derive(Error, PartialEq, Debug)]
pub enum EventError {
    #[error("attribute {0} has already been defined")]
    AlreadyPresent(String),
    #[error("attribute {0} does not exist")]
    NonExisting(String),
    #[error("event is missing some attributes")]
    MissingAttributes,
    #[error("ABE refers to non-existing attribute '{0:?}'")]
    NonExistingAttribute(String),
    #[error("{name:?}: mismatching types => expected: {expected:?}, found: {actual:?}")]
    MismatchingTypes {
        name: String,
        expected: AttributeKind,
        actual: PredicateKind,
    },
}

pub struct EventBuilder<'a> {
    by_ids: Vec<(AttributeIndex, AttributeValue)>,
    attributes: &'a AttributeTable,
    strings: &'a StringTable,
}

impl<'a> EventBuilder<'a> {
    pub(crate) fn new(attributes: &'a AttributeTable, strings: &'a StringTable) -> Self {
        Self {
            attributes,
            strings,
            by_ids: Vec::with_capacity(attributes.len()),
        }
    }

    pub fn build(mut self) -> Result<Event, EventError> {
        if self.by_ids.len() != self.attributes.len() {
            return Err(EventError::MissingAttributes);
        }
        self.by_ids.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
        Ok(Event(self.by_ids.into_iter().map(|(_, v)| v).collect()))
    }

    pub fn with_boolean(&mut self, name: &str, value: bool) -> Result<(), EventError> {
        self.add_value(name, || AttributeValue::Boolean(value))
    }

    pub fn with_integer(&mut self, name: &str, value: i64) -> Result<(), EventError> {
        self.add_value(name, || AttributeValue::Integer(value))
    }

    pub fn with_float(&mut self, name: &str, value: Decimal) -> Result<(), EventError> {
        self.add_value(name, || AttributeValue::Float(value))
    }

    pub fn with_string(&mut self, name: &str, value: &str) -> Result<(), EventError> {
        self.add_value(name, || {
            let string_index = self.strings.get(value);
            AttributeValue::String(string_index)
        })
    }

    pub fn with_integer_list(&mut self, name: &str, value: &[i64]) -> Result<(), EventError> {
        self.add_value(name, || {
            let mut values = value.to_vec();
            values.sort();
            AttributeValue::IntegerList(values)
        })
    }

    pub fn with_undefined(&mut self, name: &str) -> Result<(), EventError> {
        self.add_value(name, || AttributeValue::Undefined)
    }

    pub fn with_string_list(&mut self, name: &str, values: &[&str]) -> Result<(), EventError> {
        self.add_value(name, || {
            let mut values: Vec<_> = values.iter().map(|v| self.strings.get(v)).collect();
            values.sort();
            AttributeValue::StringList(values)
        })
    }

    fn add_value<F>(&mut self, name: &str, f: F) -> Result<(), EventError>
    where
        F: FnOnce() -> AttributeValue,
    {
        if let Some(index) = self.attributes.by_name(name) {
            self.by_ids.push((index, f()));
            Ok(())
        } else {
            Err(EventError::NonExisting(name.to_string()))
        }
    }
}

/// An event that can be used by the [`crate::atree::ATree`] structure to match ABE.
pub struct Event(Vec<AttributeValue>);

impl Event {
    pub fn get(&self, index: &AttributeIndex) -> &AttributeValue {
        &self.0[index.0]
    }
}

#[derive(Debug)]
pub enum AttributeValue {
    Boolean(bool),
    Integer(i64),
    Float(Decimal),
    String(StringId),
    IntegerList(Vec<i64>),
    StringList(Vec<StringId>),
    Undefined,
}

pub struct AttributeTable {
    by_names: HashMap<String, AttributeIndex>,
    by_ids: Vec<AttributeKind>,
}

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd, Debug, Hash)]
pub struct AttributeIndex(usize);

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

            by_names.insert(name, AttributeIndex(i));
            by_ids.push(definition.kind.clone());
        }

        Ok(Self { by_names, by_ids })
    }

    pub fn by_name(&self, name: &str) -> Option<AttributeIndex> {
        self.by_names.get(name).cloned()
    }

    pub fn by_id(&self, id: AttributeIndex) -> AttributeKind {
        self.by_ids[id.0].clone()
    }

    pub fn len(&self) -> usize {
        self.by_ids.len()
    }
}

/// The definition of an attribute that is usable by the [`crate::atree::ATree`].
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
