use crate::{
    ast::Node,
    error::ParserError,
    lexer::{Lexer, Token},
    strings::StringTable,
};
use lalrpop_util::{lalrpop_mod, ParseError};

lalrpop_mod!(grammar);

use self::grammar::TreeParser;

pub type ATreeParseError<'a> = ParseError<usize, Token<'a>, ParserError>;

pub fn parse<'a>(input: &'a str, strings: &mut StringTable) -> Result<Node, ATreeParseError<'a>> {
    let lexer = Lexer::new(input);
    TreeParser::new().parse(strings, lexer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::*;

    #[test]
    fn return_an_error_on_empty_input() {
        let mut strings = StringTable::new();

        let parsed = parse("", &mut strings);

        assert!(parsed.is_err());
    }

    #[test]
    fn return_an_error_on_invalid_input() {
        let mut strings = StringTable::new();

        let parsed = parse(")(invalid-", &mut strings);

        assert!(parsed.is_err());
    }

    #[test]
    fn can_parse_less_than_expression_with_left_identifier() {
        let mut strings = StringTable::new();

        let parsed = parse("price < 15", &mut strings);

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "price".to_string(),
                kind: PredicateKind::Comparison(
                    ComparisonOperator::LessThan,
                    ComparisonValue::Integer(15)
                )
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_less_than_expression_with_right_identifier() {
        let mut strings = StringTable::new();

        let parsed = parse("15 < price", &mut strings);

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "price".to_string(),
                kind: PredicateKind::Comparison(
                    ComparisonOperator::GreaterThan,
                    ComparisonValue::Integer(15)
                )
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_less_than_equal_expression_with_left_identifier() {
        let mut strings = StringTable::new();

        let parsed = parse("price <= 15", &mut strings);

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "price".to_string(),
                kind: PredicateKind::Comparison(
                    ComparisonOperator::LessThanEqual,
                    ComparisonValue::Integer(15)
                )
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_less_than_equal_expression_with_right_identifier() {
        let mut strings = StringTable::new();

        let parsed = parse("15 <= price", &mut strings);

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "price".to_string(),
                kind: PredicateKind::Comparison(
                    ComparisonOperator::GreaterThanEqual,
                    ComparisonValue::Integer(15)
                )
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_greater_than_expression_with_left_identifier() {
        let mut strings = StringTable::new();

        let parsed = parse("price > 15", &mut strings);

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "price".to_string(),
                kind: PredicateKind::Comparison(
                    ComparisonOperator::GreaterThan,
                    ComparisonValue::Integer(15)
                )
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_greater_than_equal_expression_with_left_identifier() {
        let mut strings = StringTable::new();

        let parsed = parse("price >= 15", &mut strings);

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "price".to_string(),
                kind: PredicateKind::Comparison(
                    ComparisonOperator::GreaterThanEqual,
                    ComparisonValue::Integer(15)
                )
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_greater_expression_with_right_identifier() {
        let mut strings = StringTable::new();

        let parsed = parse("15 > price", &mut strings);

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "price".to_string(),
                kind: PredicateKind::Comparison(
                    ComparisonOperator::LessThan,
                    ComparisonValue::Integer(15)
                )
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_greater_than_equal_expression_with_right_identifier() {
        let mut strings = StringTable::new();

        let parsed = parse("15 >= price", &mut strings);

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "price".to_string(),
                kind: PredicateKind::Comparison(
                    ComparisonOperator::LessThanEqual,
                    ComparisonValue::Integer(15)
                )
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_equal_expression_with_left_identifier() {
        let mut strings = StringTable::new();

        let parsed = parse("exchange_id = 1", &mut strings);

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "exchange_id".to_string(),
                kind: PredicateKind::Equality(
                    EqualityOperator::Equal,
                    PrimitiveLiteral::Integer(1)
                )
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_equal_expression_with_right_identifier() {
        let mut strings = StringTable::new();

        let parsed = parse("1 = exchange_id", &mut strings);

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "exchange_id".to_string(),
                kind: PredicateKind::Equality(
                    EqualityOperator::Equal,
                    PrimitiveLiteral::Integer(1)
                )
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_not_equal_expression_with_left_identifier() {
        let mut strings = StringTable::new();

        let parsed = parse("exchange_id <> 1", &mut strings);

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "exchange_id".to_string(),
                kind: PredicateKind::Equality(
                    EqualityOperator::NotEqual,
                    PrimitiveLiteral::Integer(1)
                )
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_not_equal_expression_with_right_identifier() {
        let mut strings = StringTable::new();

        let parsed = parse("1 <> exchange_id", &mut strings);

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "exchange_id".to_string(),
                kind: PredicateKind::Equality(
                    EqualityOperator::NotEqual,
                    PrimitiveLiteral::Integer(1)
                )
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_is_null_expression() {
        let mut strings = StringTable::new();

        let parsed = parse("exchange_id is null", &mut strings);

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "exchange_id".to_string(),
                kind: PredicateKind::Null(NullOperator::IsNull)
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_is_not_null_expression() {
        let mut strings = StringTable::new();

        let parsed = parse("exchange_id is not null", &mut strings);

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "exchange_id".to_string(),
                kind: PredicateKind::Null(NullOperator::IsNotNull)
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_is_empty_expression() {
        let mut strings = StringTable::new();

        let parsed = parse("deals is empty", &mut strings);

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "deals".to_string(),
                kind: PredicateKind::Null(NullOperator::IsEmpty)
            })),
            parsed
        );
    }

    #[test]
    fn return_an_error_on_an_empty_list() {
        let mut strings = StringTable::new();

        let parsed = parse("deals one of ()", &mut strings);

        assert!(parsed.is_err());
    }

    #[test]
    fn can_parse_one_of_list_expression_with_single_element_integer_list() {
        let mut strings = StringTable::new();

        let parsed = parse("ids one of (1)", &mut strings);

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "ids".to_string(),
                kind: PredicateKind::List(ListOperator::OneOf, ListLiteral::IntegerList(vec![1]))
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_one_of_list_expression_with_integer_list() {
        let mut strings = StringTable::new();

        let parsed = parse("ids one of (1, 2, 3)", &mut strings);

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "ids".to_string(),
                kind: PredicateKind::List(
                    ListOperator::OneOf,
                    ListLiteral::IntegerList(vec![1, 2, 3])
                )
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_one_of_list_expression_with_single_element_string_list() {
        let mut strings = StringTable::new();

        let parsed = parse(r##"deals one of ("deal-1")"##, &mut strings);

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "deals".to_string(),
                kind: PredicateKind::List(
                    ListOperator::OneOf,
                    ListLiteral::StringList(vec![strings.get("deal-1")])
                )
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_one_of_list_expression_with_string_list() {
        let mut strings = StringTable::new();

        let parsed = parse(
            r##"deals one of ("deal-1", "deal-2", "deal-3")"##,
            &mut strings,
        );

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "deals".to_string(),
                kind: PredicateKind::List(
                    ListOperator::OneOf,
                    ListLiteral::StringList(vec![
                        strings.get("deal-1"),
                        strings.get("deal-2"),
                        strings.get("deal-3")
                    ])
                )
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_all_of_list_expression_with_integer_list() {
        let mut strings = StringTable::new();

        let parsed = parse("ids all of (1, 2, 3)", &mut strings);

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "ids".to_string(),
                kind: PredicateKind::List(
                    ListOperator::AllOf,
                    ListLiteral::IntegerList(vec![1, 2, 3])
                )
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_all_of_list_expression_with_string_list() {
        let mut strings = StringTable::new();

        let parsed = parse(
            r##"deals all of ("deal-1", "deal-2", "deal-3")"##,
            &mut strings,
        );

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "deals".to_string(),
                kind: PredicateKind::List(
                    ListOperator::AllOf,
                    ListLiteral::StringList(vec![
                        strings.get("deal-1"),
                        strings.get("deal-2"),
                        strings.get("deal-3")
                    ])
                )
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_none_of_list_expression_with_integer_list() {
        let mut strings = StringTable::new();

        let parsed = parse("ids none of (1, 2, 3)", &mut strings);

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "ids".to_string(),
                kind: PredicateKind::List(
                    ListOperator::NoneOf,
                    ListLiteral::IntegerList(vec![1, 2, 3])
                )
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_none_of_list_expression_with_string_list() {
        let mut strings = StringTable::new();

        let parsed = parse(
            r##"deals none of ("deal-1", "deal-2", "deal-3")"##,
            &mut strings,
        );

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "deals".to_string(),
                kind: PredicateKind::List(
                    ListOperator::NoneOf,
                    ListLiteral::StringList(vec![
                        strings.get("deal-1"),
                        strings.get("deal-2"),
                        strings.get("deal-3")
                    ])
                )
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_an_expression_enclosed_in_parenthesis() {
        let mut strings = StringTable::new();

        let parsed = parse(
            r##"(deals none of ("deal-1", "deal-2", "deal-3"))"##,
            &mut strings,
        );

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "deals".to_string(),
                kind: PredicateKind::List(
                    ListOperator::NoneOf,
                    ListLiteral::StringList(vec![
                        strings.get("deal-1"),
                        strings.get("deal-2"),
                        strings.get("deal-3")
                    ])
                )
            })),
            parsed
        );
    }

    #[test]
    fn return_an_error_on_empty_parenthesis() {
        let mut strings = StringTable::new();

        let parsed = parse(r##"()"##, &mut strings);

        assert!(parsed.is_err());
    }

    #[test]
    fn can_parse_in_expression() {
        let mut strings = StringTable::new();

        let parsed = parse(r##"deal in ("deal-1", "deal-2", "deal-3")"##, &mut strings);

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "deal".to_string(),
                kind: PredicateKind::Set(
                    SetOperator::In,
                    ListLiteral::StringList(vec![
                        strings.get("deal-1"),
                        strings.get("deal-2"),
                        strings.get("deal-3")
                    ])
                )
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_not_in_expression() {
        let mut strings = StringTable::new();

        let parsed = parse(r##"exchange_id not in (1, 2, 3)"##, &mut strings);

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "exchange_id".to_string(),
                kind: PredicateKind::Set(
                    SetOperator::NotIn,
                    ListLiteral::IntegerList(vec![1, 2, 3])
                )
            })),
            parsed
        );
    }

    #[test]
    fn return_an_error_on_set_expression_with_empty_set() {
        let mut strings = StringTable::new();

        let parsed = parse(r##"exchange_id not in ()"##, &mut strings);

        assert!(parsed.is_err());
    }

    #[test]
    fn can_parse_binary_and_expression() {
        let mut strings = StringTable::new();

        let parsed = parse(
            r##"deal_ids none of ("deal-2", "deal-4") and deal_ids one of ("deal-1", "deal-3")"##,
            &mut strings,
        );

        assert_eq!(
            Ok(Node::And(
                Box::new(Node::Value(Predicate {
                    attribute: "deal_ids".to_string(),
                    kind: PredicateKind::List(
                        ListOperator::NoneOf,
                        ListLiteral::StringList(vec![strings.get("deal-2"), strings.get("deal-4")])
                    )
                })),
                Box::new(Node::Value(Predicate {
                    attribute: "deal_ids".to_string(),
                    kind: PredicateKind::List(
                        ListOperator::OneOf,
                        ListLiteral::StringList(vec![strings.get("deal-1"), strings.get("deal-3")])
                    )
                }))
            )),
            parsed
        );
    }

    #[test]
    fn can_parse_even_number_of_binary_and_expression() {
        let mut strings = StringTable::new();

        let parsed = parse(
            r##"exchange_id = 1 and private and deal_ids none of ("deal-2", "deal-4")"##,
            &mut strings,
        );

        assert_eq!(
            Ok(Node::And(
                Box::new(Node::And(
                    Box::new(Node::Value(Predicate {
                        attribute: "exchange_id".to_string(),
                        kind: PredicateKind::Equality(
                            EqualityOperator::Equal,
                            PrimitiveLiteral::Integer(1)
                        )
                    })),
                    Box::new(Node::Value(Predicate {
                        attribute: "private".to_string(),
                        kind: PredicateKind::Variable
                    }))
                )),
                Box::new(Node::Value(Predicate {
                    attribute: "deal_ids".to_string(),
                    kind: PredicateKind::List(
                        ListOperator::NoneOf,
                        ListLiteral::StringList(vec![strings.get("deal-2"), strings.get("deal-4")])
                    )
                }))
            ),),
            parsed
        );
    }

    #[test]
    fn can_parse_odd_number_of_binary_and_expression() {
        let mut strings = StringTable::new();

        let parsed = parse(
            r##"exchange_id = 1 and private and deal_ids none of ("deal-2", "deal-4") and deal_ids one of ("deal-1", "deal-3")"##,
            &mut strings,
        );

        assert_eq!(
            Ok(Node::And(
                Box::new(Node::And(
                    Box::new(Node::And(
                        Box::new(Node::Value(Predicate {
                            attribute: "exchange_id".to_string(),
                            kind: PredicateKind::Equality(
                                EqualityOperator::Equal,
                                PrimitiveLiteral::Integer(1)
                            )
                        })),
                        Box::new(Node::Value(Predicate {
                            attribute: "private".to_string(),
                            kind: PredicateKind::Variable
                        }))
                    )),
                    Box::new(Node::Value(Predicate {
                        attribute: "deal_ids".to_string(),
                        kind: PredicateKind::List(
                            ListOperator::NoneOf,
                            ListLiteral::StringList(vec![
                                strings.get("deal-2"),
                                strings.get("deal-4")
                            ])
                        )
                    }))
                )),
                Box::new(Node::Value(Predicate {
                    attribute: "deal_ids".to_string(),
                    kind: PredicateKind::List(
                        ListOperator::OneOf,
                        ListLiteral::StringList(vec![strings.get("deal-1"), strings.get("deal-3")])
                    )
                }))
            )),
            parsed
        );
    }

    #[test]
    fn can_parse_binary_or_expression() {
        let mut strings = StringTable::new();

        let parsed = parse(
            r##"deal_ids none of ("deal-2", "deal-4") or deal_ids one of ("deal-1", "deal-3")"##,
            &mut strings,
        );

        assert_eq!(
            Ok(Node::Or(
                Box::new(Node::Value(Predicate {
                    attribute: "deal_ids".to_string(),
                    kind: PredicateKind::List(
                        ListOperator::NoneOf,
                        ListLiteral::StringList(vec![strings.get("deal-2"), strings.get("deal-4")])
                    )
                })),
                Box::new(Node::Value(Predicate {
                    attribute: "deal_ids".to_string(),
                    kind: PredicateKind::List(
                        ListOperator::OneOf,
                        ListLiteral::StringList(vec![strings.get("deal-1"), strings.get("deal-3")])
                    )
                }))
            )),
            parsed
        );
    }

    #[test]
    fn can_parse_even_number_of_binary_or_expression() {
        let mut strings = StringTable::new();

        let parsed = parse(
            r##"exchange_id = 1 or private or deal_ids none of ("deal-2", "deal-4")"##,
            &mut strings,
        );

        assert_eq!(
            Ok(Node::Or(
                Box::new(Node::Or(
                    Box::new(Node::Value(Predicate {
                        attribute: "exchange_id".to_string(),
                        kind: PredicateKind::Equality(
                            EqualityOperator::Equal,
                            PrimitiveLiteral::Integer(1)
                        )
                    })),
                    Box::new(Node::Value(Predicate {
                        attribute: "private".to_string(),
                        kind: PredicateKind::Variable
                    }))
                )),
                Box::new(Node::Value(Predicate {
                    attribute: "deal_ids".to_string(),
                    kind: PredicateKind::List(
                        ListOperator::NoneOf,
                        ListLiteral::StringList(vec![strings.get("deal-2"), strings.get("deal-4")])
                    )
                }))
            ),),
            parsed
        );
    }

    #[test]
    fn can_parse_odd_number_of_binary_or_expression() {
        let mut strings = StringTable::new();

        let parsed = parse(
            r##"exchange_id = 1 or private or deal_ids none of ("deal-2", "deal-4") or deal_ids one of ("deal-1", "deal-3")"##,
            &mut strings,
        );

        assert_eq!(
            Ok(Node::Or(
                Box::new(Node::Or(
                    Box::new(Node::Or(
                        Box::new(Node::Value(Predicate {
                            attribute: "exchange_id".to_string(),
                            kind: PredicateKind::Equality(
                                EqualityOperator::Equal,
                                PrimitiveLiteral::Integer(1)
                            )
                        })),
                        Box::new(Node::Value(Predicate {
                            attribute: "private".to_string(),
                            kind: PredicateKind::Variable
                        }))
                    )),
                    Box::new(Node::Value(Predicate {
                        attribute: "deal_ids".to_string(),
                        kind: PredicateKind::List(
                            ListOperator::NoneOf,
                            ListLiteral::StringList(vec![
                                strings.get("deal-2"),
                                strings.get("deal-4")
                            ])
                        )
                    }))
                )),
                Box::new(Node::Value(Predicate {
                    attribute: "deal_ids".to_string(),
                    kind: PredicateKind::List(
                        ListOperator::OneOf,
                        ListLiteral::StringList(vec![strings.get("deal-1"), strings.get("deal-3")])
                    )
                }))
            )),
            parsed
        );
    }

    #[test]
    fn can_parse_negated_expression() {
        let mut strings = StringTable::new();

        let parsed = parse(r##"not exchange_id > 2"##, &mut strings);

        assert_eq!(
            Ok(Node::Not(Box::new(Node::Value(Predicate {
                attribute: "exchange_id".to_string(),
                kind: PredicateKind::Comparison(
                    ComparisonOperator::GreaterThan,
                    ComparisonValue::Integer(2)
                )
            })),)),
            parsed
        );
    }

    #[test]
    fn can_parse_a_variable() {
        let mut strings = StringTable::new();

        let parsed = parse(r##"private"##, &mut strings);

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "private".to_string(),
                kind: PredicateKind::Variable
            })),
            parsed
        );
    }

    #[test]
    fn can_an_expression_with_mixed_binary_operator() {
        let mut strings = StringTable::new();

        let parsed = parse(
            r##"(exchange_id = 1) and private and (deal_ids one of ("deal-1", "deal-2")) or (exchange_id = 2) and private and (deal_ids one of ("deal-3", "deal-4")) and (segment_ids one of (1, 2, 3, 4, 5, 6)) and (continent in ('NA')) and (country in ("US", "CA")) and (city in ("QC", "TN"))"##,
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
                                                Box::new(Node::Value(Predicate {
                                                    attribute: "exchange_id".to_string(),
                                                    kind: PredicateKind::Equality(
                                                        EqualityOperator::Equal,
                                                        PrimitiveLiteral::Integer(1)
                                                    )
                                                })),
                                                Box::new(Node::Value(Predicate {
                                                    attribute: "private".to_string(),
                                                    kind: PredicateKind::Variable
                                                }))
                                            )),
                                            Box::new(Node::Value(Predicate {
                                                attribute: "deal_ids".to_string(),
                                                kind: PredicateKind::List(
                                                    ListOperator::OneOf,
                                                    ListLiteral::StringList(vec![
                                                        strings.get("deal-1"),
                                                        strings.get("deal-2")
                                                    ])
                                                )
                                            }))
                                        )),
                                        Box::new(Node::Value(Predicate {
                                            attribute: "exchange_id".to_string(),
                                            kind: PredicateKind::Equality(
                                                EqualityOperator::Equal,
                                                PrimitiveLiteral::Integer(2)
                                            )
                                        }))
                                    )),
                                    Box::new(Node::Value(Predicate {
                                        attribute: "private".to_string(),
                                        kind: PredicateKind::Variable
                                    }))
                                )),
                                Box::new(Node::Value(Predicate {
                                    attribute: "deal_ids".to_string(),
                                    kind: PredicateKind::List(
                                        ListOperator::OneOf,
                                        ListLiteral::StringList(vec![
                                            strings.get("deal-3"),
                                            strings.get("deal-4")
                                        ])
                                    )
                                }))
                            )),
                            Box::new(Node::Value(Predicate {
                                attribute: "segment_ids".to_string(),
                                kind: PredicateKind::List(
                                    ListOperator::OneOf,
                                    ListLiteral::IntegerList(vec![1, 2, 3, 4, 5, 6])
                                )
                            }))
                        )),
                        Box::new(Node::Value(Predicate {
                            attribute: "continent".to_string(),
                            kind: PredicateKind::Set(
                                SetOperator::In,
                                ListLiteral::StringList(vec![strings.get("NA")])
                            )
                        }))
                    )),
                    Box::new(Node::Value(Predicate {
                        attribute: "country".to_string(),
                        kind: PredicateKind::Set(
                            SetOperator::In,
                            ListLiteral::StringList(vec![strings.get("US"), strings.get("CA")])
                        )
                    }))
                )),
                Box::new(Node::Value(Predicate {
                    attribute: "city".to_string(),
                    kind: PredicateKind::Set(
                        SetOperator::In,
                        ListLiteral::StringList(vec![strings.get("QC"), strings.get("TN")])
                    )
                }))
            )),
            parsed
        );
    }
}
