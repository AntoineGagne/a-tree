#!/usr/bin/env bash
# Combine Rust staticlib and cxxbridge archive into one archive and link example/main.cpp
# Run from repository root.
set -euo pipefail

# find OUT_DIR (cxxbridge output)
OUT_DIR=$(find target/release/build -type d -path '*/out/cxxbridge' -print -quit || true)
if [ -z "$OUT_DIR" ]; then
  echo "ERROR: cxxbridge out dir not found under target/release/build"
  exit 1
fi
echo "OUT_DIR = $OUT_DIR"

# find Rust static lib (look for liba_tree.* in target/release)
RUST_LIB=$(find target/release -maxdepth 1 -type f -name 'liba_tree.*' -print -quit || true)
if [ -z "$RUST_LIB" ]; then
  echo "ERROR: Rust static library liba_tree.* not found in target/release"
  ls -la target/release || true
  exit 1
fi
echo "RUST_LIB = $RUST_LIB"

BRIDGE_LIB="$OUT_DIR/lib/liba-tree-cxxbridge.a"
if [ ! -f "$BRIDGE_LIB" ]; then
  echo "ERROR: cxxbridge archive not found at $BRIDGE_LIB"
  echo "Contents of $OUT_DIR/lib:"
  ls -la "$OUT_DIR/lib" || true
  exit 1
fi
echo "BRIDGE_LIB = $BRIDGE_LIB"

# check required tools
for tool in ar g++; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "ERROR: required tool '$tool' not found in PATH"
    exit 1
  fi
done

# absolute paths (use readlink -f or fallback)
abs() {
  if command -v readlink >/dev/null 2>&1; then
    readlink -f "$1"
  else
    # fallback: (works on most systems)
    (cd "$(dirname "$1")" && echo "$(pwd)/$(basename "$1")")
  fi
}

RUST_LIB_ABS=$(abs "$RUST_LIB")
BRIDGE_LIB_ABS=$(abs "$BRIDGE_LIB")
OUT_INCLUDE="$OUT_DIR/include"
OUT_HEADER_DIR="$OUT_DIR/include/a-tree/src"  # path for ffi.rs.h

TMPDIR=$(mktemp -d /tmp/a-tree-link-XXXXXX)
echo "Using temp dir: $TMPDIR"
pushd "$TMPDIR" >/dev/null

# Extract object files from archives
echo "Extracting objects from $RUST_LIB_ABS"
ar x "$RUST_LIB_ABS"
echo "Extracting objects from $BRIDGE_LIB_ABS"
ar x "$BRIDGE_LIB_ABS"

# Create combined archive
COMBINED="$TMPDIR/liba_tree_combined.a"
ar rcs "$COMBINED" ./*.o
echo "Created combined archive: $COMBINED (contains $(ls -1 *.o | wc -l) .o files)"

popd >/dev/null

# Link example using the combined archive
echo "Linking example/main.cpp..."
g++ -std=c++17 example/main.cpp \
  -I"$OUT_HEADER_DIR" -I"$OUT_INCLUDE" \
  "$COMBINED" -lpthread -ldl -o example_main

echo "Link succeeded. Binary: ./example_main"
echo "Temporary files left in: $TMPDIR (remove when happy: rm -rf \"$TMPDIR\")"
