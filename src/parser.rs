use crate::{
    ast::Node,
    error::ParserError,
    events::AttributeTable,
    lexer::{Lexer, Token},
    strings::StringTable,
};
use lalrpop_util::{lalrpop_mod, ParseError};

lalrpop_mod!(grammar);

use self::grammar::TreeParser;

pub type ATreeParseError<'a> = ParseError<usize, Token<'a>, ParserError>;

#[inline]
pub fn parse<'a>(
    input: &'a str,
    attributes: &AttributeTable,
    strings: &mut StringTable,
) -> Result<Node, ATreeParseError<'a>> {
    let lexer = Lexer::new(input);
    TreeParser::new().parse(attributes, strings, lexer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ast::*,
        events::AttributeDefinition,
        predicates::{
            ComparisonOperator, ComparisonValue, EqualityOperator, ListLiteral, ListOperator,
            NullOperator, Predicate, PredicateKind, PrimitiveLiteral, SetOperator,
        },
    };

    #[test]
    fn return_an_error_on_empty_input() {
        let attributes = define_attributes();
        let mut strings = StringTable::new();

        let parsed = parse("", &attributes, &mut strings);

        assert!(parsed.is_err());
    }

    #[test]
    fn return_an_error_on_invalid_input() {
        let mut strings = StringTable::new();
        let attributes = define_attributes();

        let parsed = parse(")(invalid-", &attributes, &mut strings);

        assert!(parsed.is_err());
    }

    #[test]
    fn can_parse_less_than_expression_with_left_identifier() {
        let mut strings = StringTable::new();
        let attributes = define_attributes();

        let parsed = parse("price < 15", &attributes, &mut strings);

        assert_eq!(
            Ok(Node::Value(
                Predicate::new(
                    &attributes,
                    "price",
                    PredicateKind::Comparison(
                        ComparisonOperator::LessThan,
                        ComparisonValue::Integer(15)
                    ),
                )
                .unwrap()
            )),
            parsed
        );
    }

    #[test]
    fn can_parse_less_than_expression_with_right_identifier() {
        let mut strings = StringTable::new();
        let attributes = define_attributes();

        let parsed = parse("15 < price", &attributes, &mut strings);

        assert_eq!(
            Ok(Node::Value(
                Predicate::new(
                    &attributes,
                    "price",
                    PredicateKind::Comparison(
                        ComparisonOperator::GreaterThan,
                        ComparisonValue::Integer(15)
                    )
                )
                .unwrap()
            )),
            parsed
        );
    }

    #[test]
    fn can_parse_less_than_equal_expression_with_left_identifier() {
        let mut strings = StringTable::new();
        let attributes = define_attributes();

        let parsed = parse("price <= 15", &attributes, &mut strings);

        assert_eq!(
            Ok(Node::Value(
                Predicate::new(
                    &attributes,
                    "price",
                    PredicateKind::Comparison(
                        ComparisonOperator::LessThanEqual,
                        ComparisonValue::Integer(15)
                    )
                )
                .unwrap()
            )),
            parsed
        );
    }

    #[test]
    fn can_parse_less_than_equal_expression_with_right_identifier() {
        let mut strings = StringTable::new();
        let attributes = define_attributes();

        let parsed = parse("15 <= price", &attributes, &mut strings);

        assert_eq!(
            Ok(Node::Value(
                Predicate::new(
                    &attributes,
                    "price",
                    PredicateKind::Comparison(
                        ComparisonOperator::GreaterThanEqual,
                        ComparisonValue::Integer(15)
                    )
                )
                .unwrap()
            )),
            parsed
        );
    }

    #[test]
    fn can_parse_greater_than_expression_with_left_identifier() {
        let mut strings = StringTable::new();
        let attributes = define_attributes();

        let parsed = parse("price > 15", &attributes, &mut strings);

        assert_eq!(
            Ok(Node::Value(
                Predicate::new(
                    &attributes,
                    "price",
                    PredicateKind::Comparison(
                        ComparisonOperator::GreaterThan,
                        ComparisonValue::Integer(15)
                    )
                )
                .unwrap()
            )),
            parsed
        );
    }

    #[test]
    fn can_parse_greater_than_equal_expression_with_left_identifier() {
        let mut strings = StringTable::new();
        let attributes = define_attributes();

        let parsed = parse("price >= 15", &attributes, &mut strings);

        assert_eq!(
            Ok(Node::Value(
                Predicate::new(
                    &attributes,
                    "price",
                    PredicateKind::Comparison(
                        ComparisonOperator::GreaterThanEqual,
                        ComparisonValue::Integer(15)
                    )
                )
                .unwrap()
            )),
            parsed
        );
    }

    #[test]
    fn can_parse_greater_expression_with_right_identifier() {
        let mut strings = StringTable::new();
        let attributes = define_attributes();

        let parsed = parse("15 > price", &attributes, &mut strings);

        assert_eq!(
            Ok(Node::Value(
                Predicate::new(
                    &attributes,
                    "price",
                    PredicateKind::Comparison(
                        ComparisonOperator::LessThan,
                        ComparisonValue::Integer(15)
                    )
                )
                .unwrap()
            )),
            parsed
        );
    }

    #[test]
    fn can_parse_greater_than_equal_expression_with_right_identifier() {
        let mut strings = StringTable::new();
        let attributes = define_attributes();

        let parsed = parse("15 >= price", &attributes, &mut strings);

        assert_eq!(
            Ok(Node::Value(
                Predicate::new(
                    &attributes,
                    "price",
                    PredicateKind::Comparison(
                        ComparisonOperator::LessThanEqual,
                        ComparisonValue::Integer(15)
                    )
                )
                .unwrap()
            )),
            parsed
        );
    }

    #[test]
    fn can_parse_equal_expression_with_left_identifier() {
        let mut strings = StringTable::new();
        let attributes = define_attributes();

        let parsed = parse("exchange_id = 1", &attributes, &mut strings);

        assert_eq!(
            Ok(Node::Value(
                Predicate::new(
                    &attributes,
                    "exchange_id",
                    PredicateKind::Equality(EqualityOperator::Equal, PrimitiveLiteral::Integer(1))
                )
                .unwrap()
            )),
            parsed
        );
    }

    #[test]
    fn can_parse_equal_expression_with_right_identifier() {
        let mut strings = StringTable::new();
        let attributes = define_attributes();

        let parsed = parse("1 = exchange_id", &attributes, &mut strings);

        assert_eq!(
            Ok(Node::Value(
                Predicate::new(
                    &attributes,
                    "exchange_id",
                    PredicateKind::Equality(EqualityOperator::Equal, PrimitiveLiteral::Integer(1))
                )
                .unwrap()
            )),
            parsed
        );
    }

    #[test]
    fn can_parse_not_equal_expression_with_left_identifier() {
        let mut strings = StringTable::new();
        let attributes = define_attributes();

        let parsed = parse("exchange_id <> 1", &attributes, &mut strings);

        assert_eq!(
            Ok(Node::Value(
                Predicate::new(
                    &attributes,
                    "exchange_id",
                    PredicateKind::Equality(
                        EqualityOperator::NotEqual,
                        PrimitiveLiteral::Integer(1)
                    )
                )
                .unwrap()
            )),
            parsed
        );
    }

    #[test]
    fn can_parse_not_equal_expression_with_right_identifier() {
        let mut strings = StringTable::new();
        let attributes = define_attributes();

        let parsed = parse("1 <> exchange_id", &attributes, &mut strings);

        assert_eq!(
            Ok(Node::Value(
                Predicate::new(
                    &attributes,
                    "exchange_id",
                    PredicateKind::Equality(
                        EqualityOperator::NotEqual,
                        PrimitiveLiteral::Integer(1)
                    )
                )
                .unwrap()
            )),
            parsed
        );
    }

    #[test]
    fn can_parse_is_null_expression() {
        let mut strings = StringTable::new();
        let attributes = define_attributes();

        let parsed = parse("exchange_id is null", &attributes, &mut strings);

        assert_eq!(
            Ok(Node::Value(
                Predicate::new(
                    &attributes,
                    "exchange_id",
                    PredicateKind::Null(NullOperator::IsNull)
                )
                .unwrap()
            )),
            parsed
        );
    }

    #[test]
    fn can_parse_is_not_null_expression() {
        let mut strings = StringTable::new();
        let attributes = define_attributes();

        let parsed = parse("exchange_id is not null", &attributes, &mut strings);

        assert_eq!(
            Ok(Node::Value(
                Predicate::new(
                    &attributes,
                    "exchange_id",
                    PredicateKind::Null(NullOperator::IsNotNull)
                )
                .unwrap()
            )),
            parsed
        );
    }

    #[test]
    fn can_parse_is_empty_expression() {
        let mut strings = StringTable::new();
        let attributes = define_attributes();

        let parsed = parse("deals is empty", &attributes, &mut strings);

        assert_eq!(
            Ok(Node::Value(
                Predicate::new(
                    &attributes,
                    "deals",
                    PredicateKind::Null(NullOperator::IsEmpty)
                )
                .unwrap()
            )),
            parsed
        );
    }

    #[test]
    fn return_an_error_on_an_empty_list() {
        let mut strings = StringTable::new();
        let attributes = define_attributes();

        let parsed = parse("deals one of ()", &attributes, &mut strings);

        assert!(parsed.is_err());
    }

    #[test]
    fn can_parse_one_of_list_expression_with_single_element_integer_list() {
        let mut strings = StringTable::new();
        let attributes = define_attributes();

        let parsed = parse("ids one of (1)", &attributes, &mut strings);

        assert_eq!(
            Ok(Node::Value(
                Predicate::new(
                    &attributes,
                    "ids",
                    PredicateKind::List(ListOperator::OneOf, ListLiteral::IntegerList(vec![1]))
                )
                .unwrap()
            )),
            parsed
        );
    }

    #[test]
    fn can_parse_one_of_list_expression_with_integer_list() {
        let mut strings = StringTable::new();
        let attributes = define_attributes();

        let parsed = parse("ids one of (1, 2, 3)", &attributes, &mut strings);

        assert_eq!(
            Ok(Node::Value(
                Predicate::new(
                    &attributes,
                    "ids",
                    PredicateKind::List(
                        ListOperator::OneOf,
                        ListLiteral::IntegerList(vec![1, 2, 3])
                    )
                )
                .unwrap()
            )),
            parsed
        );
    }

    #[test]
    fn can_parse_one_of_list_expression_with_single_element_string_list() {
        let mut strings = StringTable::new();
        let attributes = define_attributes();

        let parsed = parse(r##"deals one of ("deal-1")"##, &attributes, &mut strings);

        assert_eq!(
            Ok(Node::Value(
                Predicate::new(
                    &attributes,
                    "deals",
                    PredicateKind::List(
                        ListOperator::OneOf,
                        ListLiteral::StringList(vec![strings.get("deal-1")])
                    )
                )
                .unwrap()
            )),
            parsed
        );
    }

    #[test]
    fn can_parse_one_of_list_expression_with_string_list() {
        let mut strings = StringTable::new();
        let attributes = define_attributes();

        let parsed = parse(
            r##"deals one of ("deal-1", "deal-2", "deal-3")"##,
            &attributes,
            &mut strings,
        );

        assert_eq!(
            Ok(Node::Value(
                Predicate::new(
                    &attributes,
                    "deals",
                    PredicateKind::List(
                        ListOperator::OneOf,
                        ListLiteral::StringList(vec![
                            strings.get("deal-1"),
                            strings.get("deal-2"),
                            strings.get("deal-3")
                        ])
                    )
                )
                .unwrap()
            )),
            parsed
        );
    }

    #[test]
    fn can_parse_all_of_list_expression_with_integer_list() {
        let mut strings = StringTable::new();
        let attributes = define_attributes();

        let parsed = parse("ids all of (1, 2, 3)", &attributes, &mut strings);

        assert_eq!(
            Ok(Node::Value(
                Predicate::new(
                    &attributes,
                    "ids",
                    PredicateKind::List(
                        ListOperator::AllOf,
                        ListLiteral::IntegerList(vec![1, 2, 3])
                    )
                )
                .unwrap()
            )),
            parsed
        );
    }

    #[test]
    fn sort_lists_when_parsing_an_expression_that_contains_a_list() {
        let mut strings = StringTable::new();
        let attributes = define_attributes();

        let parsed = parse(
            "ids all of (12, 8, 10, 11, 9, 4, 3, 4, 5, 1, 0, 6, 7, 3, 4, 1, 2, 3)",
            &attributes,
            &mut strings,
        );

        assert_eq!(
            Ok(Node::Value(
                Predicate::new(
                    &attributes,
                    "ids",
                    PredicateKind::List(
                        ListOperator::AllOf,
                        ListLiteral::IntegerList(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])
                    )
                )
                .unwrap()
            )),
            parsed
        );
    }

    #[test]
    fn can_parse_all_of_list_expression_with_string_list() {
        let mut strings = StringTable::new();
        let attributes = define_attributes();

        let parsed = parse(
            r##"deals all of ("deal-1", "deal-2", "deal-3")"##,
            &attributes,
            &mut strings,
        );

        assert_eq!(
            Ok(Node::Value(
                Predicate::new(
                    &attributes,
                    "deals",
                    PredicateKind::List(
                        ListOperator::AllOf,
                        ListLiteral::StringList(vec![
                            strings.get("deal-1"),
                            strings.get("deal-2"),
                            strings.get("deal-3")
                        ])
                    )
                )
                .unwrap()
            )),
            parsed
        );
    }

    #[test]
    fn can_parse_none_of_list_expression_with_integer_list() {
        let mut strings = StringTable::new();
        let attributes = define_attributes();

        let parsed = parse("ids none of (1, 2, 3)", &attributes, &mut strings);

        assert_eq!(
            Ok(Node::Value(
                Predicate::new(
                    &attributes,
                    "ids",
                    PredicateKind::List(
                        ListOperator::NoneOf,
                        ListLiteral::IntegerList(vec![1, 2, 3])
                    )
                )
                .unwrap()
            )),
            parsed
        );
    }

    #[test]
    fn can_parse_none_of_list_expression_with_string_list() {
        let mut strings = StringTable::new();
        let attributes = define_attributes();

        let parsed = parse(
            r##"deals none of ("deal-1", "deal-2", "deal-3")"##,
            &attributes,
            &mut strings,
        );

        assert_eq!(
            Ok(Node::Value(
                Predicate::new(
                    &attributes,
                    "deals",
                    PredicateKind::List(
                        ListOperator::NoneOf,
                        ListLiteral::StringList(vec![
                            strings.get("deal-1"),
                            strings.get("deal-2"),
                            strings.get("deal-3")
                        ])
                    )
                )
                .unwrap()
            )),
            parsed
        );
    }

    #[test]
    fn can_parse_an_expression_enclosed_in_parenthesis() {
        let mut strings = StringTable::new();
        let attributes = define_attributes();

        let parsed = parse(
            r##"(deals none of ("deal-1", "deal-2", "deal-3"))"##,
            &attributes,
            &mut strings,
        );

        assert_eq!(
            Ok(Node::Value(
                Predicate::new(
                    &attributes,
                    "deals",
                    PredicateKind::List(
                        ListOperator::NoneOf,
                        ListLiteral::StringList(vec![
                            strings.get("deal-1"),
                            strings.get("deal-2"),
                            strings.get("deal-3")
                        ])
                    )
                )
                .unwrap()
            )),
            parsed
        );
    }

    #[test]
    fn return_an_error_on_empty_parenthesis() {
        let mut strings = StringTable::new();
        let attributes = define_attributes();

        let parsed = parse(r##"()"##, &attributes, &mut strings);

        assert!(parsed.is_err());
    }

    #[test]
    fn can_parse_in_expression() {
        let mut strings = StringTable::new();
        let attributes = define_attributes();

        let parsed = parse(
            r##"deal in ("deal-1", "deal-2", "deal-3")"##,
            &attributes,
            &mut strings,
        );

        assert_eq!(
            Ok(Node::Value(
                Predicate::new(
                    &attributes,
                    "deal",
                    PredicateKind::Set(
                        SetOperator::In,
                        ListLiteral::StringList(vec![
                            strings.get("deal-1"),
                            strings.get("deal-2"),
                            strings.get("deal-3")
                        ])
                    )
                )
                .unwrap()
            )),
            parsed
        );
    }

    #[test]
    fn can_parse_not_in_expression() {
        let mut strings = StringTable::new();
        let attributes = define_attributes();

        let parsed = parse(
            r##"exchange_id not in (1, 2, 3)"##,
            &attributes,
            &mut strings,
        );

        assert_eq!(
            Ok(Node::Value(
                Predicate::new(
                    &attributes,
                    "exchange_id",
                    PredicateKind::Set(SetOperator::NotIn, ListLiteral::IntegerList(vec![1, 2, 3]))
                )
                .unwrap()
            )),
            parsed
        );
    }

    #[test]
    fn return_an_error_on_set_expression_with_empty_set() {
        let mut strings = StringTable::new();
        let attributes = define_attributes();

        let parsed = parse(r##"exchange_id not in ()"##, &attributes, &mut strings);

        assert!(parsed.is_err());
    }

    #[test]
    fn can_parse_binary_and_expression() {
        let mut strings = StringTable::new();
        let attributes = define_attributes();

        let parsed = parse(
            r##"deal_ids none of ("deal-2", "deal-4") and deal_ids one of ("deal-1", "deal-3")"##,
            &attributes,
            &mut strings,
        );

        assert_eq!(
            Ok(Node::And(
                Box::new(Node::Value(
                    Predicate::new(
                        &attributes,
                        "deal_ids",
                        PredicateKind::List(
                            ListOperator::NoneOf,
                            ListLiteral::StringList(vec![
                                strings.get("deal-2"),
                                strings.get("deal-4")
                            ])
                        )
                    )
                    .unwrap()
                )),
                Box::new(Node::Value(
                    Predicate::new(
                        &attributes,
                        "deal_ids",
                        PredicateKind::List(
                            ListOperator::OneOf,
                            ListLiteral::StringList(vec![
                                strings.get("deal-1"),
                                strings.get("deal-3")
                            ])
                        )
                    )
                    .unwrap()
                ))
            )),
            parsed
        );
    }

    #[test]
    fn can_parse_even_number_of_binary_and_expression() {
        let mut strings = StringTable::new();
        let attributes = define_attributes();

        let parsed = parse(
            r##"exchange_id = 1 and private and deal_ids none of ("deal-2", "deal-4")"##,
            &attributes,
            &mut strings,
        );

        assert_eq!(
            Ok(Node::And(
                Box::new(Node::And(
                    Box::new(Node::Value(
                        Predicate::new(
                            &attributes,
                            "exchange_id",
                            PredicateKind::Equality(
                                EqualityOperator::Equal,
                                PrimitiveLiteral::Integer(1)
                            )
                        )
                        .unwrap()
                    )),
                    Box::new(Node::Value(
                        Predicate::new(&attributes, "private", PredicateKind::Variable).unwrap()
                    ))
                )),
                Box::new(Node::Value(
                    Predicate::new(
                        &attributes,
                        "deal_ids",
                        PredicateKind::List(
                            ListOperator::NoneOf,
                            ListLiteral::StringList(vec![
                                strings.get("deal-2"),
                                strings.get("deal-4")
                            ])
                        )
                    )
                    .unwrap()
                ))
            ),),
            parsed
        );
    }

    #[test]
    fn can_parse_odd_number_of_binary_and_expression() {
        let mut strings = StringTable::new();
        let attributes = define_attributes();

        let parsed = parse(
            r##"exchange_id = 1 and private and deal_ids none of ("deal-2", "deal-4") and deal_ids one of ("deal-1", "deal-3")"##,
            &attributes,
            &mut strings,
        );

        assert_eq!(
            Ok(Node::And(
                Box::new(Node::And(
                    Box::new(Node::And(
                        Box::new(Node::Value(
                            Predicate::new(
                                &attributes,
                                "exchange_id",
                                PredicateKind::Equality(
                                    EqualityOperator::Equal,
                                    PrimitiveLiteral::Integer(1)
                                )
                            )
                            .unwrap()
                        )),
                        Box::new(Node::Value(
                            Predicate::new(&attributes, "private", PredicateKind::Variable)
                                .unwrap()
                        ))
                    )),
                    Box::new(Node::Value(
                        Predicate::new(
                            &attributes,
                            "deal_ids",
                            PredicateKind::List(
                                ListOperator::NoneOf,
                                ListLiteral::StringList(vec![
                                    strings.get("deal-2"),
                                    strings.get("deal-4")
                                ])
                            )
                        )
                        .unwrap()
                    ))
                )),
                Box::new(Node::Value(
                    Predicate::new(
                        &attributes,
                        "deal_ids",
                        PredicateKind::List(
                            ListOperator::OneOf,
                            ListLiteral::StringList(vec![
                                strings.get("deal-1"),
                                strings.get("deal-3")
                            ])
                        )
                    )
                    .unwrap()
                ))
            )),
            parsed
        );
    }

    #[test]
    fn can_parse_binary_or_expression() {
        let mut strings = StringTable::new();
        let attributes = define_attributes();

        let parsed = parse(
            r##"deal_ids none of ("deal-2", "deal-4") or deal_ids one of ("deal-1", "deal-3")"##,
            &attributes,
            &mut strings,
        );

        assert_eq!(
            Ok(Node::Or(
                Box::new(Node::Value(
                    Predicate::new(
                        &attributes,
                        "deal_ids",
                        PredicateKind::List(
                            ListOperator::NoneOf,
                            ListLiteral::StringList(vec![
                                strings.get("deal-2"),
                                strings.get("deal-4")
                            ])
                        )
                    )
                    .unwrap()
                )),
                Box::new(Node::Value(
                    Predicate::new(
                        &attributes,
                        "deal_ids",
                        PredicateKind::List(
                            ListOperator::OneOf,
                            ListLiteral::StringList(vec![
                                strings.get("deal-1"),
                                strings.get("deal-3")
                            ])
                        )
                    )
                    .unwrap()
                ))
            )),
            parsed
        );
    }

    #[test]
    fn can_parse_even_number_of_binary_or_expression() {
        let mut strings = StringTable::new();
        let attributes = define_attributes();

        let parsed = parse(
            r##"exchange_id = 1 or private or deal_ids none of ("deal-2", "deal-4")"##,
            &attributes,
            &mut strings,
        );

        assert_eq!(
            Ok(Node::Or(
                Box::new(Node::Or(
                    Box::new(Node::Value(
                        Predicate::new(
                            &attributes,
                            "exchange_id",
                            PredicateKind::Equality(
                                EqualityOperator::Equal,
                                PrimitiveLiteral::Integer(1)
                            )
                        )
                        .unwrap()
                    )),
                    Box::new(Node::Value(
                        Predicate::new(&attributes, "private", PredicateKind::Variable).unwrap()
                    ))
                )),
                Box::new(Node::Value(
                    Predicate::new(
                        &attributes,
                        "deal_ids",
                        PredicateKind::List(
                            ListOperator::NoneOf,
                            ListLiteral::StringList(vec![
                                strings.get("deal-2"),
                                strings.get("deal-4")
                            ])
                        )
                    )
                    .unwrap()
                ))
            ),),
            parsed
        );
    }

    #[test]
    fn can_parse_odd_number_of_binary_or_expression() {
        let mut strings = StringTable::new();
        let attributes = define_attributes();

        let parsed = parse(
            r##"exchange_id = 1 or private or deal_ids none of ("deal-2", "deal-4") or deal_ids one of ("deal-1", "deal-3")"##,
            &attributes,
            &mut strings,
        );

        assert_eq!(
            Ok(Node::Or(
                Box::new(Node::Or(
                    Box::new(Node::Or(
                        Box::new(Node::Value(
                            Predicate::new(
                                &attributes,
                                "exchange_id",
                                PredicateKind::Equality(
                                    EqualityOperator::Equal,
                                    PrimitiveLiteral::Integer(1)
                                )
                            )
                            .unwrap()
                        )),
                        Box::new(Node::Value(
                            Predicate::new(&attributes, "private", PredicateKind::Variable)
                                .unwrap()
                        ))
                    )),
                    Box::new(Node::Value(
                        Predicate::new(
                            &attributes,
                            "deal_ids",
                            PredicateKind::List(
                                ListOperator::NoneOf,
                                ListLiteral::StringList(vec![
                                    strings.get("deal-2"),
                                    strings.get("deal-4")
                                ])
                            )
                        )
                        .unwrap()
                    ))
                )),
                Box::new(Node::Value(
                    Predicate::new(
                        &attributes,
                        "deal_ids",
                        PredicateKind::List(
                            ListOperator::OneOf,
                            ListLiteral::StringList(vec![
                                strings.get("deal-1"),
                                strings.get("deal-3")
                            ])
                        )
                    )
                    .unwrap()
                ))
            )),
            parsed
        );
    }

    #[test]
    fn can_parse_negated_expression() {
        let mut strings = StringTable::new();
        let attributes = define_attributes();

        let parsed = parse(r##"not exchange_id > 2"##, &attributes, &mut strings);

        assert_eq!(
            Ok(Node::Not(Box::new(Node::Value(
                Predicate::new(
                    &attributes,
                    "exchange_id",
                    PredicateKind::Comparison(
                        ComparisonOperator::GreaterThan,
                        ComparisonValue::Integer(2)
                    )
                )
                .unwrap()
            )),)),
            parsed
        );
    }

    #[test]
    fn can_parse_a_variable() {
        let mut strings = StringTable::new();
        let attributes = define_attributes();

        let parsed = parse(r##"private"##, &attributes, &mut strings);

        assert_eq!(
            Ok(Node::Value(
                Predicate::new(&attributes, "private", PredicateKind::Variable).unwrap()
            )),
            parsed
        );
    }

    #[test]
    fn can_parse_an_expression_with_mixed_binary_operator() {
        let mut strings = StringTable::new();
        let attributes = define_attributes();

        let parsed = parse(
            r##"(exchange_id = 1) and private and (deal_ids one of ("deal-1", "deal-2")) or (exchange_id = 2) and private and (deal_ids one of ("deal-3", "deal-4")) and (segment_ids one of (1, 2, 3, 4, 5, 6)) and (continent in ('NA')) and (country in ("US", "CA")) and (city in ("QC", "TN"))"##,
            &attributes,
            &mut strings,
        );

        assert_eq!(
            Ok(Node::And(
                Box::new(Node::And(
                    Box::new(Node::And(
                        Box::new(Node::And(
                            Box::new(Node::And(
                                Box::new(Node::And(
                                    Box::new(Node::Or(
                                        Box::new(Node::And(
                                            Box::new(Node::And(
                                                Box::new(Node::Value(
                                                    Predicate::new(
                                                        &attributes,
                                                        "exchange_id",
                                                        PredicateKind::Equality(
                                                            EqualityOperator::Equal,
                                                            PrimitiveLiteral::Integer(1)
                                                        )
                                                    )
                                                    .unwrap()
                                                )),
                                                Box::new(Node::Value(
                                                    Predicate::new(
                                                        &attributes,
                                                        "private",
                                                        PredicateKind::Variable
                                                    )
                                                    .unwrap()
                                                ))
                                            )),
                                            Box::new(Node::Value(
                                                Predicate::new(
                                                    &attributes,
                                                    "deal_ids",
                                                    PredicateKind::List(
                                                        ListOperator::OneOf,
                                                        ListLiteral::StringList(vec![
                                                            strings.get("deal-1"),
                                                            strings.get("deal-2")
                                                        ])
                                                    )
                                                )
                                                .unwrap()
                                            ))
                                        )),
                                        Box::new(Node::Value(
                                            Predicate::new(
                                                &attributes,
                                                "exchange_id",
                                                PredicateKind::Equality(
                                                    EqualityOperator::Equal,
                                                    PrimitiveLiteral::Integer(2)
                                                )
                                            )
                                            .unwrap()
                                        ))
                                    )),
                                    Box::new(Node::Value(
                                        Predicate::new(
                                            &attributes,
                                            "private",
                                            PredicateKind::Variable
                                        )
                                        .unwrap()
                                    ))
                                )),
                                Box::new(Node::Value(
                                    Predicate::new(
                                        &attributes,
                                        "deal_ids",
                                        PredicateKind::List(
                                            ListOperator::OneOf,
                                            ListLiteral::StringList(vec![
                                                strings.get("deal-3"),
                                                strings.get("deal-4")
                                            ])
                                        )
                                    )
                                    .unwrap()
                                ))
                            )),
                            Box::new(Node::Value(
                                Predicate::new(
                                    &attributes,
                                    "segment_ids",
                                    PredicateKind::List(
                                        ListOperator::OneOf,
                                        ListLiteral::IntegerList(vec![1, 2, 3, 4, 5, 6])
                                    )
                                )
                                .unwrap()
                            ))
                        )),
                        Box::new(Node::Value(
                            Predicate::new(
                                &attributes,
                                "continent",
                                PredicateKind::Set(
                                    SetOperator::In,
                                    ListLiteral::StringList(vec![strings.get("NA")])
                                )
                            )
                            .unwrap()
                        ))
                    )),
                    Box::new(Node::Value(
                        Predicate::new(
                            &attributes,
                            "country",
                            PredicateKind::Set(
                                SetOperator::In,
                                ListLiteral::StringList(vec![strings.get("CA"), strings.get("US")])
                            )
                        )
                        .unwrap()
                    ))
                )),
                Box::new(Node::Value(
                    Predicate::new(
                        &attributes,
                        "city",
                        PredicateKind::Set(
                            SetOperator::In,
                            ListLiteral::StringList(vec![strings.get("QC"), strings.get("TN")])
                        )
                    )
                    .unwrap()
                ))
            )),
            parsed
        );
    }

    fn define_attributes() -> AttributeTable {
        let definitions = vec![
            AttributeDefinition::string_list("deals"),
            AttributeDefinition::string("deal"),
            AttributeDefinition::integer("price"),
            AttributeDefinition::integer("exchange_id"),
            AttributeDefinition::boolean("private"),
            AttributeDefinition::string_list("deal_ids"),
            AttributeDefinition::integer_list("ids"),
            AttributeDefinition::integer_list("segment_ids"),
            AttributeDefinition::string("continent"),
            AttributeDefinition::string("country"),
            AttributeDefinition::string("city"),
        ];
        AttributeTable::new(&definitions).unwrap()
    }
}
