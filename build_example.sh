#!/usr/bin/env bash
# Build script: cargo build --release, compile all cxxbridge generated .cc (recursively)
# into liba-tree-cxxbridge.a, then link example/main.cpp with the Rust staticlib and the cxxbridge archive.
# Usage: ./build_example.sh
set -euo pipefail

PROFILE=release
TARGET_DIR=${CARGO_TARGET_DIR:-target}

echo "[0] Remove stale cxxbridge generated crate to avoid lalrpop recursion..."
if [ -d "${TARGET_DIR}" ]; then
  find "${TARGET_DIR}" -type d -path '*/out/cxxbridge/crate/a-tree' -print -exec rm -rf {} + || true
fi
if [ -d "${TARGET_DIR}/build" ]; then
  find "${TARGET_DIR}/build" -type d -path '*/out/cxxbridge/crate/a-tree' -print -exec rm -rf {} + || true
fi

echo "[1/5] Building Rust crate (cargo build --${PROFILE})..."
cargo build --${PROFILE}

echo "[2/5] Locating cxxbridge output directory..."
OUT_DIR=$(find "${TARGET_DIR}/${PROFILE}/build" -type d -path '*/out/cxxbridge' -print -quit || true)
if [ -z "$OUT_DIR" ]; then
  echo "ERROR: cxxbridge out dir not found under ${TARGET_DIR}/${PROFILE}/build"
  exit 1
fi
echo "  OUT_DIR = $OUT_DIR"

echo "[3/5] Locating generated header..."
HEADER_PATH=$(find "$OUT_DIR" -type f -name "ffi.rs.h" -print -quit || true)
if [ -z "$HEADER_PATH" ]; then
  echo "ERROR: ffi.rs.h not found in $OUT_DIR (did cargo build succeed?)"
  exit 1
fi
HEADER_DIR=$(dirname "$HEADER_PATH")
echo "  HEADER_DIR = $HEADER_DIR"

echo "[4/5] Compiling generated C++ sources into liba-tree-cxxbridge.a..."
mkdir -p "$OUT_DIR/lib"

# Collect .cc files recursively under sources using a portable loop
cc_files=()
if [ -d "$OUT_DIR/sources" ]; then
  while IFS= read -r -d '' f; do
    cc_files+=("$f")
  done < <(find "$OUT_DIR/sources" -type f -name '*.cc' -print0)
fi

if [ ${#cc_files[@]} -eq 0 ]; then
  echo "ERROR: No generated .cc files found in $OUT_DIR/sources"
  echo "Diagnose:"
  ls -la "$OUT_DIR" || true
  ls -la "$OUT_DIR/sources" || true
  exit 1
fi

obj_files=()
for f in "${cc_files[@]}"; do
  # create a stable object filename derived from path to avoid collisions
  rel="${f#$OUT_DIR/sources/}"
  objname="${rel//\//_}"
  objname="${objname%.cc}.o"
  obj="$OUT_DIR/lib/$objname"
  echo "  compiling $f -> $obj"
  g++ -std=c++17 -I"$OUT_DIR/include" -fPIC -O2 -c "$f" -o "$obj"
  obj_files+=("$obj")
done

# create/replace static archive
ar rcs "$OUT_DIR/lib/liba-tree-cxxbridge.a" "${obj_files[@]}"
echo "  created $OUT_DIR/lib/liba-tree-cxxbridge.a"

echo "[5/5] Compiling example and linking with Rust lib and cxxbridge archive..."
RUST_LIB="${TARGET_DIR}/${PROFILE}/liba_tree.a"
if [ ! -f "$RUST_LIB" ]; then
  echo "ERROR: Rust static lib not found at $RUST_LIB"
  exit 1
fi

g++ -std=c++17 example/main.cpp \
  -I"$HEADER_DIR" -I"$OUT_DIR/include" \
  -L"${TARGET_DIR}/${PROFILE}" -la_tree \
  -L"$OUT_DIR/lib" -la-tree-cxxbridge \
  -lpthread -ldl -o example_main

echo "Done. Binary: ./example_main"
