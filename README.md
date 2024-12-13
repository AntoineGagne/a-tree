# a-tree

[![Rust](https://github.com/AntoineGagne/a-tree/actions/workflows/check.yml/badge.svg)](https://github.com/AntoineGagne/a-tree/actions/workflows/check.yml)
[![codecov](https://codecov.io/gh/AntoineGagne/a-tree/graph/badge.svg?token=JUKK1W5L2D)](https://codecov.io/gh/AntoineGagne/a-tree)

This is a work-in-progress implementation of the [A-Tree: A Dynamic Data Structure for Efficiently Indexing Arbitrary Boolean Expressions](https://dl.acm.org/doi/10.1145/3448016.3457266) paper.

The A-Tree data structure is used to evaluate a large amount of boolean expressions as fast as possible. To achieve this, the data structure tries to reuse the intermediary nodes of the incoming expressions to minimize the amount of expressions that have to be evaluated.

# See Also

* https://github.com/FrankBro/be-tree
* https://github.com/getumen/IndexingBooleanExpressions
