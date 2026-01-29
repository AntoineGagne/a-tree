#!/usr/bin/env bash
# Build script for a-tree-ffi examples
set -euo pipefail

echo "[1/3] Building FFI library..."
cargo build --release

echo "[2/3] Building C example..."
gcc examples/example.c \
    -I. \
    -L target/release \
    -la_tree_ffi \
    -lpthread -ldl -lm \
    -Wl,-rpath,$(pwd)/target/release \
    -o example_c

echo "[3/3] Building C++ example..."
g++ -std=c++17 examples/example.cpp \
    -I. \
    -L target/release \
    -la_tree_ffi \
    -lpthread -ldl -lm \
    -Wl,-rpath,$(pwd)/target/release \
    -o example_cpp

echo ""
echo "Build complete!"
echo "Run examples with:"
echo "  ./example_c"
echo "  ./example_cpp"
