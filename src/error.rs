use crate::{events::EventError, lexer::LexicalError, parser::ATreeParseError};
use thiserror::Error;

#[derive(Debug, PartialEq, Error)]
pub enum ParserError {
    #[error("failed to lex the expression with {0:?}")]
    Lexical(LexicalError),
}

#[derive(Debug, Error)]
pub enum ATreeError<'a> {
    #[error("failed to parse the expression with {0:?}")]
    ParseError(ATreeParseError<'a>),
    #[error("failed with {0:?}")]
    Event(EventError),
}
