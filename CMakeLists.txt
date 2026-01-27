cmake_minimum_required(VERSION 3.15)
project(a_tree_cpp_example LANGUAGES CXX)

# This CMakeLists automates:
#  - running `cargo build --release`
#  - compiling cxxbridge generated .cc files into a static archive
#  - building example/main.cpp, including generated headers and linking with rust staticlib and the cxxbridge archive
#
# Usage (from repo root):
#   mkdir -p build && cd build
#   cmake ..
#   cmake --build .

enable_language(CXX)
find_package(Threads REQUIRED)

set(CARGO_PROFILE release)

# 1) Run cargo build --release
execute_process(
  COMMAND cargo build --${CARGO_PROFILE}
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  RESULT_VARIABLE CARGO_RES
  OUTPUT_QUIET
  ERROR_QUIET
)
if(NOT CARGO_RES EQUAL 0)
  message(FATAL_ERROR "cargo build failed")
endif()

# 2) Find cxxbridge out dir
execute_process(
  COMMAND bash -c "find ${CMAKE_SOURCE_DIR}/target/${CARGO_PROFILE}/build -type d -path '*/out/cxxbridge' -print -quit"
  OUTPUT_VARIABLE CXXBRIDGE_OUT
  OUTPUT_STRIP_TRAILING_WHITESPACE
)
if(NOT CXXBRIDGE_OUT)
  message(FATAL_ERROR "cxxbridge out dir not found")
endif()
message(STATUS "cxxbridge out: ${CXXBRIDGE_OUT}")

# Headers and sources
set(CXXBRIDGE_INCLUDE "${CXXBRIDGE_OUT}/include")
# the generated ffi header is usually under include/a-tree/src/ffi.rs.h
set(CXXBRIDGE_HEADERDIR "${CXXBRIDGE_OUT}/include/a-tree/src")
set(CXXBRIDGE_SOURCES_DIR "${CXXBRIDGE_OUT}/sources")
set(CXXBRIDGE_LIB_DIR "${CXXBRIDGE_OUT}/lib")
file(MAKE_DIRECTORY "${CXXBRIDGE_LIB_DIR}")

# 3) Compile generated .cc files into an archive (if archive does not exist)
set(CXXBRIDGE_ARCHIVE "${CXXBRIDGE_LIB_DIR}/liba-tree-cxxbridge.a")
if(NOT EXISTS "${CXXBRIDGE_ARCHIVE}")
  execute_process(
    COMMAND bash -c " \
      pushd \"${CXXBRIDGE_SOURCES_DIR}\" >/dev/null; \
      for f in ./*.cc; do [ -e \"\$f\" ] || continue; echo compiling \$f; g++ -std=c++17 -I\"${CXXBRIDGE_INCLUDE}\" -fPIC -O2 -c \"\$f\" -o \"\${f%.cc}.o\"; done; \
      ar rcs \"${CXXBRIDGE_ARCHIVE}\" ./*.o; \
      popd >/dev/null"
    RESULT_VARIABLE _res
  )
  if(NOT _res EQUAL 0)
    message(FATAL_ERROR "Failed to compile cxxbridge sources into ${CXXBRIDGE_ARCHIVE}")
  endif()
endif()
message(STATUS "cxxbridge archive: ${CXXBRIDGE_ARCHIVE}")

# 4) Build example executable
set(EXAMPLE_SRC "${CMAKE_SOURCE_DIR}/example/main.cpp")
add_executable(example_main ${EXAMPLE_SRC})

# include directories needed for the generated header(s)
target_include_directories(example_main PRIVATE "${CXXBRIDGE_HEADERDIR}" "${CXXBRIDGE_INCLUDE}")

# link with Rust staticlib and the generated cxxbridge archive
set(RUST_STATICLIB "${CMAKE_SOURCE_DIR}/target/${CARGO_PROFILE}/liba_tree.a")
if(NOT EXISTS "${RUST_STATICLIB}")
  message(FATAL_ERROR "Rust static lib not found at ${RUST_STATICLIB}; run cargo build --${CARGO_PROFILE}")
endif()

# Link by full path so CMake doesn't try to interpret the archive name
target_link_libraries(example_main PRIVATE
  "${RUST_STATICLIB}"
  "${CXXBRIDGE_ARCHIVE}"
  Threads::Threads
  dl
)

set_target_properties(example_main PROPERTIES CXX_STANDARD 17 CXX_STANDARD_REQUIRED YES)
message(STATUS "Configured example_main. Run: cmake --build . to build")
