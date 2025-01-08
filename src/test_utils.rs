pub mod ast {
    macro_rules! or {
        ($left:expr, $right:expr) => {
            Node::Or(Box::new($left), Box::new($right))
        };
    }

    macro_rules! and {
        ($left:expr, $right:expr) => {
            Node::And(Box::new($left), Box::new($right))
        };
    }

    macro_rules! not {
        ($value:expr) => {
            Node::Not(Box::new($value))
        };
    }

    macro_rules! value {
        ($value:expr) => {
            Node::Value($value)
        };
    }

    pub(crate) use and;
    pub(crate) use not;
    pub(crate) use or;
    pub(crate) use value;
}

pub mod optimized_node {
    macro_rules! or {
        ($left:expr, $right:expr) => {
            OptimizedNode::Or(Box::new($left), Box::new($right))
        };
    }

    macro_rules! and {
        ($left:expr, $right:expr) => {
            OptimizedNode::And(Box::new($left), Box::new($right))
        };
    }

    macro_rules! value {
        ($value:expr) => {
            OptimizedNode::Value($value)
        };
    }

    pub(crate) use and;
    pub(crate) use or;
    pub(crate) use value;
}

pub mod predicates {
    macro_rules! variable {
        ($attributes:expr, $name:expr) => {
            predicate!($attributes, $name, PredicateKind::Variable)
        };
    }

    macro_rules! negated_variable {
        ($attributes:expr, $name:expr) => {
            predicate!($attributes, $name, PredicateKind::NegatedVariable)
        };
    }

    macro_rules! is_null {
        ($attributes:expr, $name:expr) => {
            predicate!(
                $attributes,
                $name,
                PredicateKind::Null(NullOperator::IsNull)
            )
        };
    }

    macro_rules! is_not_null {
        ($attributes:expr, $name:expr) => {
            predicate!(
                $attributes,
                $name,
                PredicateKind::Null(NullOperator::IsNotNull)
            )
        };
    }

    macro_rules! is_empty {
        ($attributes:expr, $name:expr) => {
            predicate!(
                $attributes,
                $name,
                PredicateKind::Null(NullOperator::IsEmpty)
            )
        };
    }

    macro_rules! is_not_empty {
        ($attributes:expr, $name:expr) => {
            predicate!(
                $attributes,
                $name,
                PredicateKind::Null(NullOperator::IsNotEmpty)
            )
        };
    }

    macro_rules! set_in {
        ($attributes:expr, $name:expr, $value:expr) => {
            predicate!(
                $attributes,
                $name,
                PredicateKind::Set(SetOperator::In, $value)
            )
        };
    }

    macro_rules! set_not_in {
        ($attributes:expr, $name:expr, $value:expr) => {
            predicate!(
                $attributes,
                $name,
                PredicateKind::Set(SetOperator::NotIn, $value)
            )
        };
    }

    macro_rules! equal {
        ($attributes:expr, $name:expr, $value:expr) => {
            predicate!(
                $attributes,
                $name,
                PredicateKind::Equality(EqualityOperator::Equal, $value)
            )
        };
    }

    macro_rules! not_equal {
        ($attributes:expr, $name:expr, $value:expr) => {
            predicate!(
                $attributes,
                $name,
                PredicateKind::Equality(EqualityOperator::NotEqual, $value)
            )
        };
    }

    macro_rules! less_than {
        ($attributes:expr, $name:expr, $value:expr) => {
            predicate!(
                $attributes,
                $name,
                PredicateKind::Comparison(ComparisonOperator::LessThan, $value)
            )
        };
    }

    macro_rules! less_than_equal {
        ($attributes:expr, $name:expr, $value:expr) => {
            predicate!(
                $attributes,
                $name,
                PredicateKind::Comparison(ComparisonOperator::LessThanEqual, $value)
            )
        };
    }

    macro_rules! greater_than {
        ($attributes:expr, $name:expr, $value:expr) => {
            predicate!(
                $attributes,
                $name,
                PredicateKind::Comparison(ComparisonOperator::GreaterThan, $value)
            )
        };
    }

    macro_rules! greater_than_equal {
        ($attributes:expr, $name:expr, $value:expr) => {
            predicate!(
                $attributes,
                $name,
                PredicateKind::Comparison(ComparisonOperator::GreaterThanEqual, $value)
            )
        };
    }

    macro_rules! all_of {
        ($attributes:expr, $name:expr, $value:expr) => {
            predicate!(
                $attributes,
                $name,
                PredicateKind::List(ListOperator::AllOf, $value)
            )
        };
    }

    macro_rules! one_of {
        ($attributes:expr, $name:expr, $value:expr) => {
            predicate!(
                $attributes,
                $name,
                PredicateKind::List(ListOperator::OneOf, $value)
            )
        };
    }

    macro_rules! none_of {
        ($attributes:expr, $name:expr, $value:expr) => {
            predicate!(
                $attributes,
                $name,
                PredicateKind::List(ListOperator::NoneOf, $value)
            )
        };
    }

    macro_rules! comparison_float {
        ($value:expr) => {
            ComparisonValue::Float($value)
        };
    }

    macro_rules! comparison_integer {
        ($value:expr) => {
            ComparisonValue::Integer($value)
        };
    }

    macro_rules! string_list {
        ($value:expr) => {
            ListLiteral::StringList($value)
        };
    }

    macro_rules! integer_list {
        ($value:expr) => {
            ListLiteral::IntegerList($value)
        };
    }

    macro_rules! primitive_integer {
        ($value:expr) => {
            PrimitiveLiteral::Integer($value)
        };
    }

    macro_rules! primitive_string {
        ($value:expr) => {
            PrimitiveLiteral::String($value)
        };
    }

    macro_rules! predicate {
        ($attributes:expr, $name:expr, $kind:expr) => {
            Predicate::new($attributes, $name, $kind).unwrap()
        };
    }

    pub(crate) use all_of;
    pub(crate) use comparison_float;
    pub(crate) use comparison_integer;
    pub(crate) use equal;
    pub(crate) use greater_than;
    pub(crate) use greater_than_equal;
    pub(crate) use integer_list;
    pub(crate) use is_empty;
    pub(crate) use is_not_empty;
    pub(crate) use is_not_null;
    pub(crate) use is_null;
    pub(crate) use less_than;
    pub(crate) use less_than_equal;
    pub(crate) use negated_variable;
    pub(crate) use none_of;
    pub(crate) use not_equal;
    pub(crate) use one_of;
    pub(crate) use predicate;
    pub(crate) use primitive_integer;
    pub(crate) use primitive_string;
    pub(crate) use set_in;
    pub(crate) use set_not_in;
    pub(crate) use string_list;
    pub(crate) use variable;
}
