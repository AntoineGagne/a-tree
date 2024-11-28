use crate::ast::{self, PredicateKind, *};
use crate::parser::{self, ATreeParseError};
use rust_decimal::Decimal;
use std::{collections::HashMap, ptr::NonNull};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ATreeError<'a> {
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
    #[error("failed to parse the expression with {0:?}")]
    ParseError(ATreeParseError<'a>),
}

pub struct ATree {
    nodes: Vec<usize>,
    strings: StringTable,
    attributes: AttributeTable,
}

pub enum ATreeNode {
    LNode {
        id: usize,
        parents: Vec<usize>,
        level: usize,
        predicate: Predicate,
    },
    INode {
        id: usize,
        parents: Vec<usize>,
        children: Vec<usize>,
        level: usize,
        operator: Operator,
    },
    RNode {
        id: usize,
        children: Vec<usize>,
        level: usize,
        operator: Operator,
    },
}

impl ATreeNode {
    fn from_abe(a_tree: &mut ATree, abe: Node) -> Result<usize, ATreeError> {
        unimplemented!();
    }
}

pub struct Predicate {
    pub attribute: AttributeIndex,
    pub kind: PredicateKind,
}

impl Predicate {
    fn try_from<'a>(
        attributes: &AttributeTable,
        ast: &ast::Predicate,
    ) -> Result<Self, ATreeError<'a>> {
        attributes
            .by_name(&ast.attribute)
            .ok_or_else(|| ATreeError::NonExistingAttribute(ast.attribute.clone()))
            .and_then(|id| {
                let id = match (&ast.kind, attributes.by_id(id)) {
                    (PredicateKind::Set(_, ListLiteral::StringList(_)), AttributeKind::String) => {
                        Ok(id)
                    }
                    (
                        PredicateKind::Set(_, ListLiteral::IntegerList(_)),
                        AttributeKind::Integer,
                    ) => Ok(id),
                    (
                        PredicateKind::Comparison(_, comparison::Value::Integer(_)),
                        AttributeKind::Integer,
                    ) => Ok(id),
                    (
                        PredicateKind::Comparison(_, comparison::Value::Float(_)),
                        AttributeKind::Float,
                    ) => Ok(id),
                    (
                        PredicateKind::Equality(_, PrimitiveLiteral::Integer(_)),
                        AttributeKind::Integer,
                    ) => Ok(id),
                    (
                        PredicateKind::Equality(_, PrimitiveLiteral::Float(_)),
                        AttributeKind::Float,
                    ) => Ok(id),
                    (
                        PredicateKind::Equality(_, PrimitiveLiteral::String(_)),
                        AttributeKind::String,
                    ) => Ok(id),
                    (
                        PredicateKind::List(_, ListLiteral::IntegerList(_)),
                        AttributeKind::IntegerList,
                    ) => Ok(id),
                    (
                        PredicateKind::List(_, ListLiteral::StringList(_)),
                        AttributeKind::StringList,
                    ) => Ok(id),
                    (actual, expected) => Err(ATreeError::MismatchingTypes {
                        name: ast.attribute.clone(),
                        expected,
                        actual: actual.clone(),
                    }),
                }?;
                Ok(Predicate {
                    attribute: id,
                    kind: ast.kind.clone(),
                })
            })
    }
}

pub enum Operator {
    And,
    Or,
    Not,
}

impl ATree {
    pub fn new(definitions: &[AttributeDefinition]) -> Result<Self, ATreeError> {
        let attributes = AttributeTable::new(definitions)?;
        let strings = StringTable::new();
        Ok(Self {
            attributes,
            strings,
            nodes: vec![],
        })
    }

    pub fn insert<'a, 'tree: 'a>(&'tree mut self, abe: &'a str) -> Result<usize, ATreeError<'a>> {
        let ast = parser::parse(abe).map_err(ATreeError::ParseError)?;
        ATreeNode::from_abe(self, ast)
    }

    pub fn make_event(&self) -> EventBuilder {
        EventBuilder::new(&self.attributes, &self.strings)
    }

    pub fn search(&self, _event: Event) -> Result<Report, ATreeError> {
        Ok(Report)
    }
}

pub struct Report;

pub struct EventBuilder<'a> {
    by_ids: Vec<(AttributeIndex, AttributeValue)>,
    attributes: &'a AttributeTable,
    strings: &'a StringTable,
}

impl<'a> EventBuilder<'a> {
    fn new(attributes: &'a AttributeTable, strings: &'a StringTable) -> Self {
        Self {
            attributes,
            strings,
            by_ids: Vec::with_capacity(attributes.len()),
        }
    }

    fn build(mut self) -> Result<Event, ATreeError<'a>> {
        if self.by_ids.len() != self.attributes.len() {
            return Err(ATreeError::MissingAttributes);
        }
        self.by_ids.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
        Ok(Event(self.by_ids.into_iter().map(|(_, v)| v).collect()))
    }

    pub fn with_boolean(&mut self, name: &str, value: bool) -> Result<(), ATreeError> {
        self.add_value(name, || AttributeValue::Boolean(value))
    }

    pub fn with_integer(&mut self, name: &str, value: i64) -> Result<(), ATreeError> {
        self.add_value(name, || AttributeValue::Integer(value))
    }

    pub fn with_float(&mut self, name: &str, value: Decimal) -> Result<(), ATreeError> {
        self.add_value(name, || AttributeValue::Float(value))
    }

    pub fn with_string(&mut self, name: &str, value: &str) -> Result<(), ATreeError> {
        self.add_value(name, || {
            let string_index = self.strings.get(value);
            AttributeValue::String(string_index)
        })
    }

    pub fn with_integer_list(&mut self, name: &str, value: &[i64]) -> Result<(), ATreeError> {
        self.add_value(name, || AttributeValue::IntegerList(value.to_vec()))
    }

    pub fn with_string_list(&mut self, name: &str, values: &[&str]) -> Result<(), ATreeError> {
        // TODO: Investigate this as this might be problematic with `all of` expression
        self.add_value(name, || {
            AttributeValue::StringList(values.iter().filter_map(|v| self.strings.get(v)).collect())
        })
    }

    fn add_value<F>(&mut self, name: &str, f: F) -> Result<(), ATreeError>
    where
        F: FnOnce() -> AttributeValue,
    {
        if let Some(index) = self.attributes.by_name(name) {
            self.by_ids.push((index, f()));
            Ok(())
        } else {
            Err(ATreeError::NonExisting(name.to_string()))
        }
    }
}

pub struct Event(Vec<AttributeValue>);

enum AttributeValue {
    Boolean(bool),
    Integer(i64),
    Float(Decimal),
    String(Option<StringId>),
    IntegerList(Vec<i64>),
    StringList(Vec<StringId>),
}

struct StringTable {
    by_values: HashMap<String, usize>,
    counter: usize,
}

impl StringTable {
    fn new() -> Self {
        Self {
            by_values: HashMap::new(),
            counter: 0,
        }
    }

    fn get(&self, value: &str) -> Option<StringId> {
        let index = self.by_values.get(value)?;
        Some(StringId(*index))
    }

    fn get_or_update(&mut self, value: &str) -> StringId {
        let counter = self.by_values.entry(value.to_string()).or_insert_with(|| {
            let counter = self.counter;
            self.counter += 1;
            counter
        });

        StringId(*counter)
    }
}

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd, Debug)]
struct StringId(usize);

struct AttributeTable {
    by_names: HashMap<String, AttributeIndex>,
    by_ids: Vec<AttributeKind>,
}

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd, Debug)]
struct AttributeIndex(usize);

impl AttributeTable {
    fn new(definitions: &[AttributeDefinition]) -> Result<Self, ATreeError> {
        let size = definitions.len();
        let mut by_names = HashMap::with_capacity(size);
        let mut by_ids = Vec::with_capacity(size);
        for (i, definition) in definitions.iter().enumerate() {
            let name = definition.name.to_owned();
            if by_names.contains_key(&name) {
                return Err(ATreeError::AlreadyPresent(name));
            }

            by_names.insert(name, AttributeIndex(i));
            by_ids.push(definition.kind.clone());
        }

        Ok(Self { by_names, by_ids })
    }

    fn by_name(&self, name: &str) -> Option<AttributeIndex> {
        self.by_names.get(name).cloned()
    }

    fn by_id(&self, id: AttributeIndex) -> AttributeKind {
        self.by_ids[id.0].clone()
    }

    fn len(&self) -> usize {
        self.by_ids.len()
    }
}

#[derive(Clone)]
pub struct AttributeDefinition {
    name: String,
    kind: AttributeKind,
}

#[derive(Clone, Debug)]
enum AttributeKind {
    Boolean,
    Integer,
    Float,
    String,
    IntegerList,
    StringList,
}

impl AttributeDefinition {
    pub fn boolean(name: &str) -> Self {
        let kind = AttributeKind::Boolean;
        Self {
            name: name.to_owned(),
            kind,
        }
    }

    pub fn integer(name: &str) -> Self {
        let kind = AttributeKind::Integer;
        Self {
            name: name.to_owned(),
            kind,
        }
    }

    pub fn float(name: &str) -> Self {
        let kind = AttributeKind::Float;
        Self {
            name: name.to_owned(),
            kind,
        }
    }

    pub fn string(name: &str) -> Self {
        let kind = AttributeKind::String;
        Self {
            name: name.to_owned(),
            kind,
        }
    }

    pub fn integer_list(name: &str) -> Self {
        let kind = AttributeKind::IntegerList;
        Self {
            name: name.to_owned(),
            kind,
        }
    }

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
    fn can_build_an_atree() {
        let definitions = [
            AttributeDefinition::boolean("private"),
            AttributeDefinition::string_list("deals"),
            AttributeDefinition::integer("exchange_id"),
            AttributeDefinition::float("bidfloor"),
            AttributeDefinition::string("country"),
            AttributeDefinition::integer_list("segment_ids"),
        ];

        let result = ATree::new(&definitions);

        assert!(result.is_ok());
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

        let result = ATree::new(&definitions);

        assert!(result.is_err());
    }

    #[test]
    fn can_add_a_boolean_attribute_value() {
        let definitions = [AttributeDefinition::boolean("private")];
        let atree = ATree::new(&definitions).unwrap();
        let mut event_builder = atree.make_event();

        let result = event_builder.with_boolean("private", true);

        assert!(result.is_ok());
    }

    #[test]
    fn can_add_an_integer_attribute_value() {
        let definitions = [AttributeDefinition::integer("exchange_id")];
        let atree = ATree::new(&definitions).unwrap();
        let mut event_builder = atree.make_event();

        let result = event_builder.with_integer("exchange_id", 1);

        assert!(result.is_ok());
    }

    #[test]
    fn can_add_a_float_attribute_value() {
        let definitions = [AttributeDefinition::integer("bidfloor")];
        let atree = ATree::new(&definitions).unwrap();
        let mut event_builder = atree.make_event();

        let result = event_builder.with_float("bidfloor", Decimal::new(1, 0));

        assert!(result.is_ok());
    }

    #[test]
    fn can_add_a_string_attribute_value() {
        let definitions = [AttributeDefinition::integer("country")];
        let atree = ATree::new(&definitions).unwrap();
        let mut event_builder = atree.make_event();

        let result = event_builder.with_string("country", "US");

        assert!(result.is_ok());
    }

    #[test]
    fn can_add_an_integer_list_attribute_value() {
        let definitions = [AttributeDefinition::integer_list("segment_ids")];
        let atree = ATree::new(&definitions).unwrap();
        let mut event_builder = atree.make_event();

        let result = event_builder.with_integer_list("segment_ids", &[1, 2, 3]);

        assert!(result.is_ok());
    }

    #[test]
    fn can_add_an_string_list_attribute_value() {
        let definitions = [AttributeDefinition::string_list("deal_ids")];
        let atree = ATree::new(&definitions).unwrap();
        let mut event_builder = atree.make_event();

        let result = event_builder.with_string_list("deal_ids", &["deal-1", "deal-2"]);

        assert!(result.is_ok());
    }

    #[test]
    fn return_an_error_when_adding_a_non_existing_attribute() {
        let definitions = [AttributeDefinition::string_list("deal_ids")];
        let atree = ATree::new(&definitions).unwrap();
        let mut event_builder = atree.make_event();

        let result = event_builder.with_boolean("non_existing", true);

        assert!(matches!(result, Err(ATreeError::NonExisting(_))));
    }

    #[test]
    fn can_create_an_event_with_no_attributes() {
        let atree = ATree::new(&[]).unwrap();

        let event_builder = atree.make_event();

        assert!(event_builder.build().is_ok());
    }

    #[test]
    fn can_create_an_event_with_attributes() {
        let definitions = [
            AttributeDefinition::boolean("private"),
            AttributeDefinition::string_list("deals"),
            AttributeDefinition::integer("exchange_id"),
            AttributeDefinition::float("bidfloor"),
            AttributeDefinition::string("country"),
            AttributeDefinition::integer_list("segment_ids"),
        ];
        let atree = ATree::new(&definitions).unwrap();
        let mut builder = atree.make_event();

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
    fn return_an_error_when_creating_an_event_with_missing_attribute() {
        let definitions = [AttributeDefinition::boolean("private")];
        let atree = ATree::new(&definitions).unwrap();

        let event_builder = atree.make_event();

        assert!(matches!(
            event_builder.build(),
            Err(ATreeError::MissingAttributes)
        ));
    }
}
