# a-tree

[![Rust](https://github.com/AntoineGagne/a-tree/actions/workflows/check.yml/badge.svg)](https://github.com/AntoineGagne/a-tree/actions/workflows/check.yml)
[![Latest version](https://img.shields.io/crates/v/a-tree.svg)](https://crates.io/crates/a-tree)
[![Documentation](https://docs.rs/a-tree/badge.svg)](https://docs.rs/a-tree)
[![Code Coverage](https://codecov.io/gh/AntoineGagne/a-tree/graph/badge.svg?token=JUKK1W5L2D)](https://codecov.io/gh/AntoineGagne/a-tree)

This is an implementation of the [A-Tree: A Dynamic Data Structure for Efficiently Indexing Arbitrary Boolean Expressions](https://dl.acm.org/doi/10.1145/3448016.3457266) paper.

The A-Tree data structure is used to evaluate a large amount of boolean expressions as fast as possible. To achieve this, the data structure tries to reuse the intermediary nodes of the incoming expressions to minimize the amount of expressions that have to be evaluated.

## Features

This crate supports the following features:

* Insertion of arbitrary boolean expressions via a domain specific language;
* Deletion of subscriptions;
* Export to Graphviz format;
* Search with events for matching arbitrary boolean expressions.

## Documentation

The documentation is available on [doc.rs](https://docs.rs/crate/a-tree/latest).

## C/C++ Bindings

C and C++ bindings are available in the [`a-tree-ffi`](a-tree-ffi/) subdirectory. The FFI provides:

* Clean C API with explicit memory management
* Auto-generated C header file
* RAII C++ wrapper examples
* Both static and dynamic linking support

See the [a-tree-ffi README](a-tree-ffi/README.md) for usage examples and build instructions.

## License

This project is licensed under the [Apache 2.0](LICENSE-APACHE) and the [MIT License](LICENSE-MIT).
