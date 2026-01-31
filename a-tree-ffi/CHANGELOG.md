# Changelog - a-tree-ffi

## [0.1.0] - 2026-01-29

### Added
- Initial release of C/C++ FFI bindings for a-tree
- Clean C API with proper memory management
- Auto-generated C header using cbindgen
- Support for all core a-tree operations:
  - Tree creation with attribute definitions
  - Expression insertion
  - Event building with boolean, integer, and string attributes
  - Searching for matching expressions
- Working C example demonstrating basic usage
- Working C++ example with RAII wrapper classes
- Both static (.a) and dynamic (.so) library builds
- Comprehensive README with usage examples
- Build script for compiling examples

### Design Decisions
- Separate crate to avoid modifying core a-tree library
- Uses manual C FFI (extern "C") instead of cxx crate
- Opaque pointers (void*) for event builders to avoid lifetime issues
- Explicit memory management functions for safety
- Result types with error messages for better error handling
