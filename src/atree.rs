use crate::{
    ast::{self, *},
    error::ATreeError,
    events::{
        AttributeDefinition, AttributeIndex, AttributeKind, AttributeTable, Event, EventBuilder,
        EventError,
    },
    parser,
    predicates::Predicate,
    strings::StringTable,
};

/// The A-Tree data structure as described by the paper.
pub struct ATree {
    nodes: Vec<usize>,
    strings: StringTable,
    attributes: AttributeTable,
}

impl ATree {
    /// Create a new [`ATree`] with the specified attribute definitions.
    ///
    /// ```rust
    /// use a_tree::{ATree, AttributeDefinition};
    ///
    /// let definitions = [
    ///     AttributeDefinition::boolean("private"),
    ///     AttributeDefinition::integer("exchange_id")
    /// ];
    /// let result = ATree::new(&definitions);
    /// assert!(result.is_ok());
    /// ```
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
        let ast = parser::parse(abe, &self.attributes, &mut self.strings)
            .map_err(ATreeError::ParseError)?;
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

pub enum Operator {
    And,
    Or,
    Not,
}

#[cfg(test)]
mod tests {
    use super::*;

    const AN_INVALID_BOOLEAN_EXPRESSION: &str = "invalid in (1, 2, 3 and";

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
    fn return_an_error_on_invalid_boolean_expression() {
        let definitions = [
            AttributeDefinition::boolean("private"),
            AttributeDefinition::string("country"),
            AttributeDefinition::string_list("deals"),
            AttributeDefinition::integer("exchange_id"),
            AttributeDefinition::integer_list("segment_ids"),
        ];
        let mut atree = ATree::new(&definitions).unwrap();

        let result = atree.insert(AN_INVALID_BOOLEAN_EXPRESSION);

        assert!(result.is_err());
    }
}
