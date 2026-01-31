# a-tree-ffi

C/C++ Foreign Function Interface (FFI) bindings for the [a-tree](https://crates.io/crates/a-tree) library.

## Overview

This crate provides a simple, stable C API for using the A-Tree data structure from C and C++ applications. The A-Tree efficiently indexes and evaluates large numbers of arbitrary boolean expressions.

## Features

- **Clean C API**: Simple, well-documented C functions with all attribute types supported
- **Modern C++ Wrapper**: `atree.hpp` provides RAII, Result types, and fluent builders
- **Complete Functionality**: Float, lists, undefined attributes, delete, and graphviz export
- **Type-safe**: Uses C enums and structs for safety
- **Zero dependencies**: Only depends on the core `a-tree` crate
- **Static and dynamic linking**: Builds both `.a` and `.so`/`.dylib` libraries

## Supported Attribute Types

| Type | C API | C++ API | Description |
|------|-------|---------|-------------|
| Boolean | `atree_event_builder_with_boolean` | `.with_boolean()` | Single boolean value |
| Integer | `atree_event_builder_with_integer` | `.with_integer()` | 64-bit signed integer |
| Float | `atree_event_builder_with_float` | `.with_float()` | Decimal number (mantissa + scale) |
| String | `atree_event_builder_with_string` | `.with_string()` | Text string |
| StringList | `atree_event_builder_with_string_list` | `.with_string_list()` | Array of strings |
| IntegerList | `atree_event_builder_with_integer_list` | `.with_integer_list()` | Array of integers |
| Undefined | `atree_event_builder_with_undefined` | `.with_undefined()` | Null/missing value |

## Building

### Build Everything

```bash
cd a-tree-ffi
./build_examples.sh
```

This generates:
- `target/release/liba_tree_ffi.so` (or `.dylib` on macOS, `.dll` on Windows)
- `target/release/liba_tree_ffi.a`
- `atree.h` - Auto-generated C header file
- `example_c` - C example executable
- `example_cpp` - C++ example (basic wrapper)
- `example_advanced_cpp` - Advanced C++ example with atree.hpp

### Manual Build Steps

```bash
# Build just the FFI library
cargo build --release

# Build C example
gcc examples/example.c -I. -L target/release -la_tree_ffi -lpthread -ldl -lm \
    -Wl,-rpath,$(pwd)/target/release -o example_c

# Build C++ examples
g++ -std=c++17 examples/example.cpp -I. -L target/release -la_tree_ffi \
    -lpthread -ldl -lm -Wl,-rpath,$(pwd)/target/release -o example_cpp

g++ -std=c++17 examples/advanced_cpp.cpp -I. -L target/release -la_tree_ffi \
    -lpthread -ldl -lm -Wl,-rpath,$(pwd)/target/release -o example_advanced_cpp
```

## Quick Start (C++)

Using the modern `atree.hpp` wrapper (recommended):

```cpp
#include "atree.hpp"

using namespace atree;

int main() {
    // Create A-Tree with attribute definitions
    Tree tree({
        AttributeDefinition::integer("user_id"),
        AttributeDefinition::float_attr("price"),
        AttributeDefinition::boolean("is_active")
    });

    // Insert expressions with Result-based error handling
    auto result = tree.insert(1, "user_id > 100 and price < 50.0 and is_active");
    if (result.is_err()) {
        std::cerr << "Error: " << result.error() << "\n";
        return 1;
    }

    // Build and search with fluent API
    auto matches = tree.search(
        tree.make_event()
            .with_integer("user_id", 150)
            .with_float("price", 45.99)  // Automatic decimal conversion
            .with_boolean("is_active", true)
    ).unwrap();

    std::cout << "Found " << matches.size() << " matches\n";

    // Export tree visualization
    auto dot = tree.to_graphviz().unwrap();
    // Save to file and visualize with: dot -Tpng tree.dot -o tree.png

    return 0;
}
```

### Key C++ Features

- **RAII**: Automatic memory management, no manual cleanup needed
- **Result Types**: `Result<T>` for explicit error handling without exceptions
- **Fluent Builder**: Method chaining for readable event construction
- **Type Safety**: Compile-time checking and templates
- **Move Semantics**: Efficient ownership transfer
- **String Views**: `std::string_view` for zero-copy string passing

## Quick Start (C)

Using the raw C API:

```c
#include "atree.h"
#include <stdio.h>

int main(void) {
    // Define attributes
    AtreeAttributeDef defs[] = {
        { .name = "user_id", .attr_type = Integer },
        { .name = "price", .attr_type = Float },
        { .name = "is_active", .attr_type = Boolean }
    };

    // Create A-Tree
    ATreeHandle *tree = atree_new(defs, 3);
    if (!tree) {
        fprintf(stderr, "Failed to create tree\n");
        return 1;
    }

    // Insert expression
    AtreeResult result = atree_insert(tree, 1,
        "user_id > 100 and price < 50.0 and is_active");
    if (!result.success) {
        fprintf(stderr, "Error: %s\n", result.error_message);
        atree_free_error(result.error_message);
        atree_free(tree);
        return 1;
    }

    // Build event
    void *builder = atree_event_builder_new(tree);
    atree_event_builder_with_integer(builder, "user_id", 150);
    atree_event_builder_with_float(builder, "price", 4599, 2);  // 45.99
    atree_event_builder_with_boolean(builder, "is_active", true);

    // Search
    AtreeSearchResult search_result = atree_search(tree, builder);
    printf("Found %zu matches\n", search_result.count);

    // Cleanup
    atree_search_result_free(search_result);
    atree_free(tree);

    return 0;
}
```

## Advanced C++ Features

### Float Precision

```cpp
// Automatic conversion (6 decimal places)
builder.with_float("price", 123.456);

// Precise decimal representation
// 123.45 = mantissa: 12345, scale: 2
builder.with_float("price", 12345, 2);
```

### Lists

```cpp
// String lists
builder.with_string_list("tags", {"premium", "featured", "new"});

// Integer lists
builder.with_integer_list("categories", {10, 20, 30});
```

### Undefined Attributes

```cpp
// Mark attribute as not set
builder.with_undefined("optional_field");
```

### Delete and Graphviz

```cpp
// Delete a subscription
tree.delete_subscription(42);

// Export tree structure
auto dot_result = tree.to_graphviz();
if (dot_result) {
    std::ofstream file("tree.dot");
    file << dot_result.unwrap();
}
```

### Error Handling

```cpp
// Option 1: Result-based (no exceptions)
auto result = tree.insert(1, "expression");
if (result.is_err()) {
    std::cerr << "Error: " << result.error() << "\n";
}

// Option 2: Exception-based (throws on error)
try {
    tree.insert(1, "expression").unwrap();
} catch (const atree::Error& e) {
    std::cerr << "Error: " << e.what() << "\n";
}
```

## Complete C API Reference

### Tree Management
- `ATreeHandle* atree_new(defs, count)` - Create tree with attribute definitions
- `void atree_free(handle)` - Free tree
- `void atree_delete(handle, subscription_id)` - Delete subscription by ID
- `char* atree_to_graphviz(handle)` - Export tree as Graphviz DOT format

### Expression Management
- `AtreeResult atree_insert(handle, id, expression)` - Insert boolean expression

### Event Building
- `void* atree_event_builder_new(handle)` - Create event builder
- `AtreeResult atree_event_builder_with_boolean(builder, name, value)`
- `AtreeResult atree_event_builder_with_integer(builder, name, value)`
- `AtreeResult atree_event_builder_with_float(builder, name, number, scale)`
- `AtreeResult atree_event_builder_with_string(builder, name, value)`
- `AtreeResult atree_event_builder_with_string_list(builder, name, values, count)`
- `AtreeResult atree_event_builder_with_integer_list(builder, name, values, count)`
- `AtreeResult atree_event_builder_with_undefined(builder, name)`
- `void atree_event_builder_free(builder)` - Free unused builder

### Searching
- `AtreeSearchResult atree_search(handle, builder)` - Search (consumes builder)
- `void atree_search_result_free(result)` - Free search results

### Memory Management
- `void atree_free_error(error)` - Free error message string
- `void atree_free_string(string)` - Free string returned by library

## Memory Management

**C API**:
- All `_new()` functions return pointers that must be freed with corresponding `_free()` functions
- `atree_search()` consumes the EventBuilder - don't use it after calling search
- Error messages must be freed with `atree_free_error()` when `success == false`
- Graphviz strings must be freed with `atree_free_string()`

**C++ API**:
- All memory is managed automatically via RAII
- No manual cleanup needed
- EventBuilder is consumed by `search()` (enforced at compile time via move semantics)

## Thread Safety

The A-Tree is **not** thread-safe. For concurrent access:
- Protect the tree with a mutex/lock
- Use multiple trees (one per thread)
- See `examples/thread_safe.cpp` (future) for a thread-safe wrapper example

## Integration

### CMake

```cmake
cmake_minimum_required(VERSION 3.15)
project(my_project)

# For C
find_library(ATREE_FFI_LIB a_tree_ffi HINTS path/to/a-tree-ffi/target/release)
add_executable(my_app main.c)
target_include_directories(my_app PRIVATE path/to/a-tree-ffi)
target_link_libraries(my_app PRIVATE ${ATREE_FFI_LIB} pthread dl m)

# For C++ (with atree.hpp)
add_executable(my_cpp_app main.cpp)
target_compile_features(my_cpp_app PRIVATE cxx_std_17)
target_include_directories(my_cpp_app PRIVATE path/to/a-tree-ffi)
target_link_libraries(my_cpp_app PRIVATE ${ATREE_FFI_LIB} pthread dl m)
```

### pkg-config (future)

```bash
pkg-config --cflags --libs atree-ffi
```

## Examples

- `examples/example.c` - Basic C usage
- `examples/example.cpp` - Basic C++ with simple RAII wrappers
- `examples/advanced_cpp.cpp` - Complete demonstration using `atree.hpp`
  - All attribute types
  - Float with decimal precision
  - Lists and undefined values
  - Delete operations
  - Graphviz export
  - Result-based error handling

Run the advanced example to see all features:
```bash
./example_advanced_cpp
```

## Files

- `atree.h` - Auto-generated C header (from cbindgen)
- `atree.hpp` - Modern C++ wrapper library (header-only)
- `src/lib.rs` - FFI implementation
- `build.rs` - Builds C header during compilation

## License

This project is licensed under the same terms as a-tree: [Apache 2.0](../LICENSE-APACHE) or [MIT License](../LICENSE-MIT).
