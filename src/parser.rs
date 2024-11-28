use crate::{
    ast::Node,
    lexer::{Lexer, LexicalError, Token},
};
use lalrpop_util::{lalrpop_mod, ParseError};

lalrpop_mod!(grammar);

use self::grammar::TreeParser;

pub type ATreeParseError<'a> = ParseError<usize, Token<'a>, LexicalError>;

pub fn parse(input: &str) -> Result<Node, ATreeParseError> {
    let lexer = Lexer::new(input);
    let parser = TreeParser::new();
    parser.parse(lexer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{ListLiteral, Node, Predicate, PredicateKind, PrimitiveLiteral};

    #[test]
    fn return_an_error_on_empty_input() {
        let parsed = parse("");

        assert!(parsed.is_err());
    }

    #[test]
    fn return_an_error_on_invalid_input() {
        let parsed = parse(")(invalid-");

        assert!(parsed.is_err());
    }

    #[test]
    fn can_parse_less_than_expression_with_left_identifier() {
        use crate::ast::comparison::{Operator, Value};

        let parsed = parse("price < 15");

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "price".to_string(),
                kind: PredicateKind::Comparison(Operator::LessThan, Value::Integer(15))
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_less_than_expression_with_right_identifier() {
        use crate::ast::comparison::{Operator, Value};

        let parsed = parse("15 < price");

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "price".to_string(),
                kind: PredicateKind::Comparison(Operator::GreaterThan, Value::Integer(15))
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_less_than_equal_expression_with_left_identifier() {
        use crate::ast::comparison::{Operator, Value};

        let parsed = parse("price <= 15");

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "price".to_string(),
                kind: PredicateKind::Comparison(Operator::LessThanEqual, Value::Integer(15))
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_less_than_equal_expression_with_right_identifier() {
        use crate::ast::comparison::{Operator, Value};

        let parsed = parse("15 <= price");

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "price".to_string(),
                kind: PredicateKind::Comparison(Operator::GreaterThanEqual, Value::Integer(15))
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_greater_than_expression_with_left_identifier() {
        use crate::ast::comparison::{Operator, Value};

        let parsed = parse("price > 15");

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "price".to_string(),
                kind: PredicateKind::Comparison(Operator::GreaterThan, Value::Integer(15))
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_greater_than_equal_expression_with_left_identifier() {
        use crate::ast::comparison::{Operator, Value};

        let parsed = parse("price >= 15");

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "price".to_string(),
                kind: PredicateKind::Comparison(Operator::GreaterThanEqual, Value::Integer(15))
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_greater_expression_with_right_identifier() {
        use crate::ast::comparison::{Operator, Value};

        let parsed = parse("15 > price");

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "price".to_string(),
                kind: PredicateKind::Comparison(Operator::LessThan, Value::Integer(15))
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_greater_than_equal_expression_with_right_identifier() {
        use crate::ast::comparison::{Operator, Value};

        let parsed = parse("15 >= price");

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "price".to_string(),
                kind: PredicateKind::Comparison(Operator::LessThanEqual, Value::Integer(15))
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_equal_expression_with_left_identifier() {
        use crate::ast::equality::Operator;

        let parsed = parse("exchange_id = 1");

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "exchange_id".to_string(),
                kind: PredicateKind::Equality(Operator::Equal, PrimitiveLiteral::Integer(1))
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_equal_expression_with_right_identifier() {
        use crate::ast::equality::Operator;

        let parsed = parse("1 = exchange_id");

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "exchange_id".to_string(),
                kind: PredicateKind::Equality(Operator::Equal, PrimitiveLiteral::Integer(1))
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_not_equal_expression_with_left_identifier() {
        use crate::ast::equality::Operator;

        let parsed = parse("exchange_id <> 1");

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "exchange_id".to_string(),
                kind: PredicateKind::Equality(Operator::NotEqual, PrimitiveLiteral::Integer(1))
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_not_equal_expression_with_right_identifier() {
        use crate::ast::equality::Operator;

        let parsed = parse("1 <> exchange_id");

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "exchange_id".to_string(),
                kind: PredicateKind::Equality(Operator::NotEqual, PrimitiveLiteral::Integer(1))
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_is_null_expression() {
        use crate::ast::null::Operator;

        let parsed = parse("exchange_id is null");

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "exchange_id".to_string(),
                kind: PredicateKind::Null(Operator::IsNull)
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_is_not_null_expression() {
        use crate::ast::null::Operator;

        let parsed = parse("exchange_id is not null");

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "exchange_id".to_string(),
                kind: PredicateKind::Null(Operator::IsNotNull)
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_is_empty_expression() {
        use crate::ast::null::Operator;

        let parsed = parse("deals is empty");

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "deals".to_string(),
                kind: PredicateKind::Null(Operator::IsEmpty)
            })),
            parsed
        );
    }

    #[test]
    fn return_an_error_on_an_empty_list() {
        let parsed = parse("deals one of ()");

        assert!(parsed.is_err());
    }

    #[test]
    fn can_parse_one_of_list_expression_with_single_element_integer_list() {
        use crate::ast::list::Operator;

        let parsed = parse("ids one of (1)");

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "ids".to_string(),
                kind: PredicateKind::List(Operator::OneOf, ListLiteral::IntegerList(vec![1]))
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_one_of_list_expression_with_integer_list() {
        use crate::ast::list::Operator;

        let parsed = parse("ids one of (1, 2, 3)");

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "ids".to_string(),
                kind: PredicateKind::List(Operator::OneOf, ListLiteral::IntegerList(vec![1, 2, 3]))
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_one_of_list_expression_with_single_element_string_list() {
        use crate::ast::list::Operator;

        let parsed = parse(r##"deals one of ("deal-1")"##);

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "deals".to_string(),
                kind: PredicateKind::List(
                    Operator::OneOf,
                    ListLiteral::StringList(vec!["deal-1".to_string(),])
                )
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_one_of_list_expression_with_string_list() {
        use crate::ast::list::Operator;

        let parsed = parse(r##"deals one of ("deal-1", "deal-2", "deal-3")"##);

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "deals".to_string(),
                kind: PredicateKind::List(
                    Operator::OneOf,
                    ListLiteral::StringList(vec![
                        "deal-1".to_string(),
                        "deal-2".to_string(),
                        "deal-3".to_string()
                    ])
                )
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_all_of_list_expression_with_integer_list() {
        use crate::ast::list::Operator;

        let parsed = parse("ids all of (1, 2, 3)");

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "ids".to_string(),
                kind: PredicateKind::List(Operator::AllOf, ListLiteral::IntegerList(vec![1, 2, 3]))
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_all_of_list_expression_with_string_list() {
        use crate::ast::list::Operator;

        let parsed = parse(r##"deals all of ("deal-1", "deal-2", "deal-3")"##);

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "deals".to_string(),
                kind: PredicateKind::List(
                    Operator::AllOf,
                    ListLiteral::StringList(vec![
                        "deal-1".to_string(),
                        "deal-2".to_string(),
                        "deal-3".to_string()
                    ])
                )
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_none_of_list_expression_with_integer_list() {
        use crate::ast::list::Operator;

        let parsed = parse("ids none of (1, 2, 3)");

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "ids".to_string(),
                kind: PredicateKind::List(
                    Operator::NoneOf,
                    ListLiteral::IntegerList(vec![1, 2, 3])
                )
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_none_of_list_expression_with_string_list() {
        use crate::ast::list::Operator;

        let parsed = parse(r##"deals none of ("deal-1", "deal-2", "deal-3")"##);

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "deals".to_string(),
                kind: PredicateKind::List(
                    Operator::NoneOf,
                    ListLiteral::StringList(vec![
                        "deal-1".to_string(),
                        "deal-2".to_string(),
                        "deal-3".to_string()
                    ])
                )
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_an_expression_enclosed_in_parenthesis() {
        use crate::ast::list::Operator;

        let parsed = parse(r##"(deals none of ("deal-1", "deal-2", "deal-3"))"##);

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "deals".to_string(),
                kind: PredicateKind::List(
                    Operator::NoneOf,
                    ListLiteral::StringList(vec![
                        "deal-1".to_string(),
                        "deal-2".to_string(),
                        "deal-3".to_string()
                    ])
                )
            })),
            parsed
        );
    }

    #[test]
    fn return_an_error_on_empty_parenthesis() {
        let parsed = parse(r##"()"##);

        assert!(parsed.is_err());
    }

    #[test]
    fn can_parse_in_expression() {
        use crate::ast::set::Operator;

        let parsed = parse(r##"deal in ("deal-1", "deal-2", "deal-3")"##);

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "deal".to_string(),
                kind: PredicateKind::Set(
                    Operator::In,
                    ListLiteral::StringList(vec![
                        "deal-1".to_string(),
                        "deal-2".to_string(),
                        "deal-3".to_string()
                    ])
                )
            })),
            parsed
        );
    }

    #[test]
    fn can_parse_not_in_expression() {
        use crate::ast::set::Operator;

        let parsed = parse(r##"exchange_id not in (1, 2, 3)"##);

        assert_eq!(
            Ok(Node::Value(Predicate {
                attribute: "exchange_id".to_string(),
                kind: PredicateKind::Set(Operator::NotIn, ListLiteral::IntegerList(vec![1, 2, 3]))
            })),
            parsed
        );
    }

    #[test]
    fn return_an_error_on_set_expression_with_empty_set() {
        let parsed = parse(r##"exchange_id not in ()"##);

        assert!(parsed.is_err());
    }

    #[test]
    fn can_parse_binary_and_expression() {
        use crate::ast::list::Operator;

        let parsed = parse(
            r##"deal_ids none of ("deal-2", "deal-4") and deal_ids one of ("deal-1", "deal-3")"##,
        );

        assert_eq!(
            Ok(Node::And(
                Box::new(Node::Value(Predicate {
                    attribute: "deal_ids".to_string(),
                    kind: PredicateKind::List(
                        Operator::NoneOf,
                        ListLiteral::StringList(vec!["deal-2".to_string(), "deal-4".to_string()])
                    )
                })),
                Box::new(Node::Value(Predicate {
                    attribute: "deal_ids".to_string(),
                    kind: PredicateKind::List(
                        Operator::OneOf,
                        ListLiteral::StringList(vec!["deal-1".to_string(), "deal-3".to_string()])
                    )
                }))
            )),
            parsed
        );
    }

    #[test]
    fn can_parse_even_number_of_binary_and_expression() {
        use crate::ast::*;

        let parsed =
            parse(r##"exchange_id = 1 and private and deal_ids none of ("deal-2", "deal-4")"##);

        assert_eq!(
            Ok(Node::And(
                Box::new(Node::And(
                    Box::new(Node::Value(Predicate {
                        attribute: "exchange_id".to_string(),
                        kind: PredicateKind::Equality(
                            equality::Operator::Equal,
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
                        list::Operator::NoneOf,
                        ListLiteral::StringList(vec!["deal-2".to_string(), "deal-4".to_string()])
                    )
                }))
            ),),
            parsed
        );
    }

    #[test]
    fn can_parse_odd_number_of_binary_and_expression() {
        use crate::ast::*;

        let parsed = parse(
            r##"exchange_id = 1 and private and deal_ids none of ("deal-2", "deal-4") and deal_ids one of ("deal-1", "deal-3")"##,
        );

        assert_eq!(
            Ok(Node::And(
                Box::new(Node::And(
                    Box::new(Node::And(
                        Box::new(Node::Value(Predicate {
                            attribute: "exchange_id".to_string(),
                            kind: PredicateKind::Equality(
                                equality::Operator::Equal,
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
                            list::Operator::NoneOf,
                            ListLiteral::StringList(vec![
                                "deal-2".to_string(),
                                "deal-4".to_string()
                            ])
                        )
                    }))
                )),
                Box::new(Node::Value(Predicate {
                    attribute: "deal_ids".to_string(),
                    kind: PredicateKind::List(
                        list::Operator::OneOf,
                        ListLiteral::StringList(vec!["deal-1".to_string(), "deal-3".to_string()])
                    )
                }))
            )),
            parsed
        );
    }

    #[test]
    fn can_parse_binary_or_expression() {
        use crate::ast::list::Operator;

        let parsed = parse(
            r##"deal_ids none of ("deal-2", "deal-4") or deal_ids one of ("deal-1", "deal-3")"##,
        );

        assert_eq!(
            Ok(Node::Or(
                Box::new(Node::Value(Predicate {
                    attribute: "deal_ids".to_string(),
                    kind: PredicateKind::List(
                        Operator::NoneOf,
                        ListLiteral::StringList(vec!["deal-2".to_string(), "deal-4".to_string()])
                    )
                })),
                Box::new(Node::Value(Predicate {
                    attribute: "deal_ids".to_string(),
                    kind: PredicateKind::List(
                        Operator::OneOf,
                        ListLiteral::StringList(vec!["deal-1".to_string(), "deal-3".to_string()])
                    )
                }))
            )),
            parsed
        );
    }

    #[test]
    fn can_parse_even_number_of_binary_or_expression() {
        use crate::ast::*;

        let parsed =
            parse(r##"exchange_id = 1 or private or deal_ids none of ("deal-2", "deal-4")"##);

        assert_eq!(
            Ok(Node::Or(
                Box::new(Node::Or(
                    Box::new(Node::Value(Predicate {
                        attribute: "exchange_id".to_string(),
                        kind: PredicateKind::Equality(
                            equality::Operator::Equal,
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
                        list::Operator::NoneOf,
                        ListLiteral::StringList(vec!["deal-2".to_string(), "deal-4".to_string()])
                    )
                }))
            ),),
            parsed
        );
    }

    #[test]
    fn can_parse_odd_number_of_binary_or_expression() {
        use crate::ast::*;

        let parsed = parse(
            r##"exchange_id = 1 or private or deal_ids none of ("deal-2", "deal-4") or deal_ids one of ("deal-1", "deal-3")"##,
        );

        assert_eq!(
            Ok(Node::Or(
                Box::new(Node::Or(
                    Box::new(Node::Or(
                        Box::new(Node::Value(Predicate {
                            attribute: "exchange_id".to_string(),
                            kind: PredicateKind::Equality(
                                equality::Operator::Equal,
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
                            list::Operator::NoneOf,
                            ListLiteral::StringList(vec![
                                "deal-2".to_string(),
                                "deal-4".to_string()
                            ])
                        )
                    }))
                )),
                Box::new(Node::Value(Predicate {
                    attribute: "deal_ids".to_string(),
                    kind: PredicateKind::List(
                        list::Operator::OneOf,
                        ListLiteral::StringList(vec!["deal-1".to_string(), "deal-3".to_string()])
                    )
                }))
            )),
            parsed
        );
    }

    #[test]
    fn can_parse_negated_expression() {
        use crate::ast::comparison::{Operator, Value};

        let parsed = parse(r##"not exchange_id > 2"##);

        assert_eq!(
            Ok(Node::Not(Box::new(Node::Value(Predicate {
                attribute: "exchange_id".to_string(),
                kind: PredicateKind::Comparison(Operator::GreaterThan, Value::Integer(2))
            })),)),
            parsed
        );
    }

    #[test]
    fn can_parse_a_variable() {
        let parsed = parse(r##"private"##);

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
        use crate::ast::*;

        let parsed = parse(
            r##"(exchange_id = 1) and private and (deal_ids one of ("deal-1", "deal-2")) or (exchange_id = 2) and private and (deal_ids one of ("deal-3", "deal-4")) and (segment_ids one of (1, 2, 3, 4, 5, 6)) and (continent in ('NA')) and (country in ("US", "CA")) and (city in ("QC", "TN"))"##,
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
                                                        equality::Operator::Equal,
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
                                                    list::Operator::OneOf,
                                                    ListLiteral::StringList(vec![
                                                        "deal-1".to_string(),
                                                        "deal-2".to_string()
                                                    ])
                                                )
                                            }))
                                        )),
                                        Box::new(Node::Value(Predicate {
                                            attribute: "exchange_id".to_string(),
                                            kind: PredicateKind::Equality(
                                                equality::Operator::Equal,
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
                                        list::Operator::OneOf,
                                        ListLiteral::StringList(vec![
                                            "deal-3".to_string(),
                                            "deal-4".to_string()
                                        ])
                                    )
                                }))
                            )),
                            Box::new(Node::Value(Predicate {
                                attribute: "segment_ids".to_string(),
                                kind: PredicateKind::List(
                                    list::Operator::OneOf,
                                    ListLiteral::IntegerList(vec![1, 2, 3, 4, 5, 6])
                                )
                            }))
                        )),
                        Box::new(Node::Value(Predicate {
                            attribute: "continent".to_string(),
                            kind: PredicateKind::Set(
                                set::Operator::In,
                                ListLiteral::StringList(vec!["NA".to_string()])
                            )
                        }))
                    )),
                    Box::new(Node::Value(Predicate {
                        attribute: "country".to_string(),
                        kind: PredicateKind::Set(
                            set::Operator::In,
                            ListLiteral::StringList(vec!["US".to_string(), "CA".to_string()])
                        )
                    }))
                )),
                Box::new(Node::Value(Predicate {
                    attribute: "city".to_string(),
                    kind: PredicateKind::Set(
                        set::Operator::In,
                        ListLiteral::StringList(vec!["QC".to_string(), "TN".to_string()])
                    )
                }))
            )),
            parsed
        );
    }
}
