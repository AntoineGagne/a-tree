cmake_minimum_required(VERSION 3.15)
project(a_tree_cpp_example)

# Locate the Rust-built static library and include path produced by cxx.
# How you integrate depends on how you build the Rust side: cargo build --release producing a static lib,
# and the generated headers location. cxxbridge-cmake can help automate that step.
#
# Minimal guidance:
# - Build Rust crate first: cargo build --release
# - Add the directory containing generated headers to include_directories()
# - Link the static library produced by cargo (liba-tree.a or similar)

add_executable(example_main main.cpp)

# TODO: adjust these paths to match your build outputs
target_include_directories(example_main PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../target/cxxbridge)
target_link_libraries(example_main PRIVATE /path/to/your/liba-tree.a)
