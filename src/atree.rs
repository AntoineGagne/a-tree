use crate::{
    ast::{self, PredicateKind, *},
    events::{
        AttributeDefinition, AttributeIndex, AttributeKind, AttributeTable, Event, EventBuilder,
        EventError,
    },
    parser::{self, ATreeParseError},
    strings::StringTable,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ATreeError<'a> {
    #[error("failed to parse the expression with {0:?}")]
    ParseError(ATreeParseError<'a>),
    #[error("failed with {0:?}")]
    Event(EventError),
}

pub struct ATree {
    nodes: Vec<usize>,
    strings: StringTable,
    attributes: AttributeTable,
}

enum ATreeNode {
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
    attribute: AttributeIndex,
    kind: PredicateKind,
}

impl Predicate {
    fn try_from<'a>(
        attributes: &AttributeTable,
        ast: &ast::Predicate,
    ) -> Result<Self, ATreeError<'a>> {
        attributes
            .by_name(&ast.attribute)
            .ok_or_else(|| {
                ATreeError::Event(EventError::NonExistingAttribute(ast.attribute.clone()))
            })
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
                        PredicateKind::Comparison(_, ComparisonValue::Integer(_)),
                        AttributeKind::Integer,
                    ) => Ok(id),
                    (
                        PredicateKind::Comparison(_, ComparisonValue::Float(_)),
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
                    (PredicateKind::Variable, AttributeKind::Boolean) => Ok(id),
                    (actual, expected) => Err(ATreeError::Event(EventError::MismatchingTypes {
                        name: ast.attribute.clone(),
                        expected,
                        actual: actual.clone(),
                    })),
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
        let attributes = AttributeTable::new(definitions).map_err(ATreeError::Event)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

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

        assert!(matches!(result, Err(EventError::NonExisting(_))));
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
            Err(EventError::MissingAttributes)
        ));
    }
}
