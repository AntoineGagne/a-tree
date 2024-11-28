use logos::{Logos, SpannedIter};
use rust_decimal::Decimal;
use std::{num::ParseIntError, str::FromStr};
use thiserror::Error;

#[derive(Default, Error, Debug, Clone, PartialEq)]
pub enum LexicalError {
    #[default]
    #[error("invalid token")]
    InvalidToken,
    #[error("failed to parse integer: {0:?}")]
    Integer(ParseIntError),
    #[error("failed to parse float: {0:?}")]
    Float(rust_decimal::Error),
}

#[derive(Clone, Debug, Logos, PartialEq)]
#[logos(skip r"[\s\t\n\f]+", error = LexicalError)]
pub enum Token<'source> {
    #[token("<")]
    LessThan,
    #[token("<=")]
    LessThanEqual,
    #[token(">")]
    GreaterThan,
    #[token(">=")]
    GreaterThanEqual,
    #[token("not")]
    #[token("!")]
    Not,
    #[token("=")]
    Equal,
    #[token("<>")]
    NotEqual,
    #[token("in")]
    In,
    #[token("not in")]
    NotIn,
    #[token("one of")]
    OneOf,
    #[token("none of")]
    NoneOf,
    #[token("all of")]
    AllOf,
    #[token("is null")]
    IsNull,
    #[token("is not null")]
    IsNotNull,
    #[token("is empty")]
    IsEmpty,
    #[token("and")]
    #[token("&&")]
    And,
    #[token("or")]
    #[token("||")]
    Or,
    #[token("(")]
    LeftParenthesis,
    #[token(")")]
    RightParenthesis,
    #[token(",")]
    Comma,
    #[regex(r"-?[0-9]+", |lex| lex.slice().parse::<i64>().map_err(LexicalError::Integer))]
    IntegerLiteral(i64),
    #[regex(r#"(\"(\\.|[^"\\])*\"|\'(\\.|[^'\\])*\')"#, |lex| lex.slice().trim_matches(['\'', '"']))]
    StringLiteral(&'source str),
    #[regex(r"[0-9]+\.[0-9]*", |lex| Decimal::from_str(lex.slice()).map_err(LexicalError::Float))]
    FloatLiteral(Decimal),
    #[token("true", |_| true)]
    #[token("false", |_| false)]
    BooleanLiteral(bool),
    #[regex("[a-zA-Z_][a-zA-Z0-9_-]*", |lex| lex.slice())]
    Identifier(&'source str),
}

impl<'source> std::fmt::Display for Token<'source> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

pub type Spanned<Tok, Location, Error> = Result<(Location, Tok, Location), Error>;

pub struct Lexer<'input> {
    token_stream: SpannedIter<'input, Token<'input>>,
}

impl<'input> Lexer<'input> {
    pub fn new(input: &'input str) -> Self {
        Self {
            token_stream: Token::lexer(input).spanned(),
        }
    }
}

impl<'input> Iterator for Lexer<'input> {
    type Item = Spanned<Token<'input>, usize, LexicalError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.token_stream.next().map(|(token, span)| {
            let token = match token {
                // FIXME: This is a bug in Locos where regex take priority over all...
                Ok(Token::Identifier("not")) => Ok(Token::Not),
                other => other,
            };

            Ok((span.start, token?, span.end))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lex_tokens(input: &str) -> Result<Vec<Token>, LexicalError> {
        Lexer::new(input)
            .map(|value| match value {
                Ok((_, token, _)) => Ok(token),
                Err(error) => Err(error),
            })
            .collect()
    }

    #[test]
    fn can_lex_less_than() {
        let actual = lex_tokens("<").unwrap();
        assert_eq!(vec![Token::LessThan], actual);
    }

    #[test]
    fn can_lex_less_than_equal() {
        let actual = lex_tokens("<=").unwrap();
        assert_eq!(vec![Token::LessThanEqual], actual);
    }

    #[test]
    fn can_lex_greater_than() {
        let actual = lex_tokens(">").unwrap();
        assert_eq!(vec![Token::GreaterThan], actual);
    }

    #[test]
    fn can_lex_greater_than_equal() {
        let actual = lex_tokens(">=").unwrap();
        assert_eq!(vec![Token::GreaterThanEqual], actual);
    }

    #[test]
    fn can_lex_not() {
        let actual = lex_tokens("not").unwrap();
        let other = lex_tokens("!").unwrap();
        assert_eq!(vec![Token::Not], actual);
        assert_eq!(vec![Token::Not], other);
    }

    #[test]
    fn can_lex_equal() {
        let actual = lex_tokens("=").unwrap();
        assert_eq!(vec![Token::Equal], actual);
    }

    #[test]
    fn can_lex_not_equal() {
        let actual = lex_tokens("<>").unwrap();
        assert_eq!(vec![Token::NotEqual], actual);
    }

    #[test]
    fn can_lex_not_in() {
        let actual = lex_tokens("not in").unwrap();
        assert_eq!(vec![Token::NotIn], actual);
    }

    #[test]
    fn can_lex_in() {
        let actual = lex_tokens("in").unwrap();
        assert_eq!(vec![Token::In], actual);
    }

    #[test]
    fn can_lex_one_of() {
        let actual = lex_tokens("one of").unwrap();
        assert_eq!(vec![Token::OneOf], actual);
    }

    #[test]
    fn can_lex_none_of() {
        let actual = lex_tokens("none of").unwrap();
        assert_eq!(vec![Token::NoneOf], actual);
    }

    #[test]
    fn can_lex_all_of() {
        let actual = lex_tokens("all of").unwrap();
        assert_eq!(vec![Token::AllOf], actual);
    }

    #[test]
    fn can_lex_is_null() {
        let actual = lex_tokens("is null").unwrap();
        assert_eq!(vec![Token::IsNull], actual);
    }

    #[test]
    fn can_lex_is_not_null() {
        let actual = lex_tokens("is not null").unwrap();
        assert_eq!(vec![Token::IsNotNull], actual);
    }

    #[test]
    fn can_lex_is_empty() {
        let actual = lex_tokens("is empty").unwrap();
        assert_eq!(vec![Token::IsEmpty], actual);
    }

    #[test]
    fn can_lex_and() {
        let actual = lex_tokens("and").unwrap();
        let other = lex_tokens("&&").unwrap();
        assert_eq!(vec![Token::And], actual);
        assert_eq!(vec![Token::And], other);
    }

    #[test]
    fn can_lex_or() {
        let actual = lex_tokens("or").unwrap();
        let other = lex_tokens("||").unwrap();
        assert_eq!(vec![Token::Or], actual);
        assert_eq!(vec![Token::Or], other);
    }

    #[test]
    fn can_lex_parenthesis() {
        let actual = lex_tokens("(").unwrap();
        let other = lex_tokens(")").unwrap();
        assert_eq!(vec![Token::LeftParenthesis], actual);
        assert_eq!(vec![Token::RightParenthesis], other);
    }

    #[test]
    fn can_lex_comma() {
        let actual = lex_tokens(",").unwrap();
        assert_eq!(vec![Token::Comma], actual);
    }

    #[test]
    fn can_lex_integer() {
        let actual = lex_tokens("123").unwrap();
        assert_eq!(vec![Token::IntegerLiteral(123)], actual);
    }

    #[test]
    fn can_lex_negative_integer() {
        let actual = lex_tokens("-123").unwrap();
        assert_eq!(vec![Token::IntegerLiteral(-123)], actual);
    }

    #[test]
    fn can_lex_float() {
        let actual = lex_tokens("123.123").unwrap();
        let other = lex_tokens("123.").unwrap();
        assert_eq!(vec![Token::FloatLiteral(Decimal::new(123123, 3))], actual);
        assert_eq!(vec![Token::FloatLiteral(Decimal::new(123, 0))], other);
    }

    #[test]
    fn can_lex_boolean() {
        let actual = lex_tokens("true").unwrap();
        let other = lex_tokens("false").unwrap();
        assert_eq!(vec![Token::BooleanLiteral(true)], actual);
        assert_eq!(vec![Token::BooleanLiteral(false)], other);
    }

    #[test]
    fn can_lex_identifier() {
        let actual = lex_tokens("deal_ids").unwrap();
        assert_eq!(vec![Token::Identifier("deal_ids")], actual);
    }

    #[test]
    fn can_lex_empty_string() {
        let actual = lex_tokens("\"\"").unwrap();
        assert_eq!(vec![Token::StringLiteral("")], actual);
        let actual = lex_tokens("''").unwrap();
        assert_eq!(vec![Token::StringLiteral("")], actual);
    }

    #[test]
    fn can_lex_string() {
        let actual = lex_tokens("\"deal_1\"").unwrap();
        assert_eq!(vec![Token::StringLiteral("deal_1")], actual);
        let actual = lex_tokens("'deal_1'").unwrap();
        assert_eq!(vec![Token::StringLiteral("deal_1")], actual);
    }

    #[test]
    fn can_lex_string_with_escaped_quotes() {
        let actual = lex_tokens(r##""deal\"_1""##).unwrap();
        assert_eq!(vec![Token::StringLiteral("deal\\\"_1")], actual);
        let actual = lex_tokens("'deal\\'_1'").unwrap();
        assert_eq!(vec![Token::StringLiteral("deal\\'_1")], actual);
    }

    #[test]
    fn can_lex_string_with_escaped_chars() {
        let actual = lex_tokens("\"deal_1\n\\dsad\\a\"").unwrap();
        assert_eq!(vec![Token::StringLiteral("deal_1\n\\dsad\\a")], actual);
        let actual = lex_tokens("'deal_1\n\\dsad\\a'").unwrap();
        assert_eq!(vec![Token::StringLiteral("deal_1\n\\dsad\\a")], actual);
    }

    #[test]
    fn can_lex_multiple_expressions() {
        let actual = lex_tokens(
            r#"(not private) or (exchange = 1 and deal_ids one of ("deal_1", "deal_2", "deal_3"))"#,
        );

        assert_eq!(
            Ok(vec![
                Token::LeftParenthesis,
                Token::Not,
                Token::Identifier("private"),
                Token::RightParenthesis,
                Token::Or,
                Token::LeftParenthesis,
                Token::Identifier("exchange"),
                Token::Equal,
                Token::IntegerLiteral(1),
                Token::And,
                Token::Identifier("deal_ids"),
                Token::OneOf,
                Token::LeftParenthesis,
                Token::StringLiteral("deal_1"),
                Token::Comma,
                Token::StringLiteral("deal_2"),
                Token::Comma,
                Token::StringLiteral("deal_3"),
                Token::RightParenthesis,
                Token::RightParenthesis,
            ]),
            actual
        );
    }

    #[test]
    fn can_lex_complex_expression() {
        let actual = lex_tokens(
            r##"((not private) or (exchange = 1 and deal_ids one of ("deal_1", "deal_2", "deal_3"))) and (continent <> "EU" and country not in ("US", "CA"))"##,
        );

        assert_eq!(
            Ok(vec![
                Token::LeftParenthesis,
                Token::LeftParenthesis,
                Token::Not,
                Token::Identifier("private"),
                Token::RightParenthesis,
                Token::Or,
                Token::LeftParenthesis,
                Token::Identifier("exchange"),
                Token::Equal,
                Token::IntegerLiteral(1),
                Token::And,
                Token::Identifier("deal_ids"),
                Token::OneOf,
                Token::LeftParenthesis,
                Token::StringLiteral("deal_1"),
                Token::Comma,
                Token::StringLiteral("deal_2"),
                Token::Comma,
                Token::StringLiteral("deal_3"),
                Token::RightParenthesis,
                Token::RightParenthesis,
                Token::RightParenthesis,
                Token::And,
                Token::LeftParenthesis,
                Token::Identifier("continent"),
                Token::NotEqual,
                Token::StringLiteral("EU"),
                Token::And,
                Token::Identifier("country"),
                Token::NotIn,
                Token::LeftParenthesis,
                Token::StringLiteral("US"),
                Token::Comma,
                Token::StringLiteral("CA"),
                Token::RightParenthesis,
                Token::RightParenthesis,
            ]),
            actual
        );
    }
}
