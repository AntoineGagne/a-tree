# a-tree

[![Rust](https://github.com/AntoineGagne/a-tree/actions/workflows/check.yml/badge.svg)](https://github.com/AntoineGagne/a-tree/actions/workflows/check.yml)
[![codecov](https://codecov.io/gh/AntoineGagne/a-tree/graph/badge.svg?token=JUKK1W5L2D)](https://codecov.io/gh/AntoineGagne/a-tree)

This is a work-in-progress implementation of the [A-Tree: A Dynamic Data Structure for Efficiently Indexing Arbitrary Boolean Expressions](https://dl.acm.org/doi/10.1145/3448016.3457266) paper.

# Description

The A-Tree data structure is used to evaluate a large amount of boolean expressions as fast as possible. To achieve this, the data structure tries to reuse the intermediary nodes of the incoming expressions to minimize the amount of expressions that have to be evaluated.

# Domain Specific Language

To easily create arbitrary boolean expressions (ABE), the library supports a domain specific language (DSL) that allows for easy configuration. The DSL supports the following:

* Boolean operators: `and` (`&&`), `or` (`||`), `not` (`!`) and `variable` where `variable` is a defined attribute for the A-Tree;
* Comparison: `<`, `<=`, `>`, `>=`. They work for `integer` and `float`;
* Equality: `=` and `<>`. They work for `integer`, `float` and `string`;
* Null: `is null`, `is not null` (for variables) and `is empty` (for lists);
* Set: `in` and `not in`. They work for list of `integer` or for list of `string`;
* List: `one of`, `none of` and `all of`. They work for list of `integer` and list of `string`.

As an example, the following would all be valid ABEs:

```text
(exchange_id = 1 and deals one of ["deal-1", "deal-2", "deal-3"]) and (segment_ids one of [1, 2, 3]) and (continent = 'NA' and country in ["US", "CA"])
(log_level >= 1) and (month in [1, 2, 3] and day in [15, 16]) or (month in [4, 5, 6] and day in [10, 11])
```

# Inspirations

* https://github.com/FrankBro/be-tree
* https://github.com/getumen/IndexingBooleanExpressions
