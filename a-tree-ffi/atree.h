#ifndef ATREE_H
#define ATREE_H

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

/**
 * Attribute types supported by the A-Tree
 */
typedef enum AtreeAttributeType {
  Boolean = 0,
  Integer = 1,
  Float = 2,
  String = 3,
  StringList = 4,
  IntegerList = 5,
} AtreeAttributeType;

/**
 * Opaque handle to an ATree instance
 */
typedef struct ATreeHandle ATreeHandle;

/**
 * Attribute definition for creating an A-Tree
 */
typedef struct AtreeAttributeDef {
  const char *name;
  enum AtreeAttributeType attr_type;
} AtreeAttributeDef;

/**
 * Result type for operations that can fail
 */
typedef struct AtreeResult {
  bool success;
  char *error_message;
} AtreeResult;

/**
 * Search result containing matching subscription IDs
 */
typedef struct AtreeSearchResult {
  uint64_t *ids;
  uintptr_t count;
} AtreeSearchResult;

/**
 * Create a new A-Tree with the given attribute definitions.
 *
 * # Arguments
 * * `defs` - Array of attribute definitions
 * * `count` - Number of definitions in the array
 *
 * # Returns
 * Pointer to ATreeHandle on success, null on failure
 *
 * # Safety
 * - `defs` must point to valid memory containing `count` AtreeAttributeDef structs
 * - Each `name` field must be a valid null-terminated C string
 * - Caller must free the returned handle with `atree_free()`
 */
struct ATreeHandle *atree_new(const struct AtreeAttributeDef *defs, uintptr_t count);

/**
 * Free an A-Tree handle.
 *
 * # Safety
 * - `handle` must be a valid pointer returned by `atree_new()`
 * - `handle` must not be used after this call
 */
void atree_free(struct ATreeHandle *handle);

/**
 * Insert a boolean expression associated with a subscription ID.
 *
 * # Arguments
 * * `handle` - Valid ATree handle
 * * `subscription_id` - Unique ID for this subscription
 * * `expression` - Null-terminated boolean expression string
 *
 * # Returns
 * Result indicating success or failure
 *
 * # Safety
 * - `handle` must be a valid pointer returned by `atree_new()`
 * - `expression` must be a valid null-terminated C string
 * - Caller must free result.error_message with `atree_free_error()` if !success
 */
struct AtreeResult atree_insert(struct ATreeHandle *handle,
                                uint64_t subscription_id,
                                const char *expression);

/**
 * Delete a subscription by ID.
 *
 * # Arguments
 * * `handle` - Valid ATree handle
 * * `subscription_id` - ID of the subscription to delete
 *
 * # Safety
 * - `handle` must be a valid pointer returned by `atree_new()`
 */
void atree_delete(struct ATreeHandle *handle, uint64_t subscription_id);

/**
 * Export the tree structure as a Graphviz DOT format string.
 *
 * # Returns
 * Null-terminated string containing DOT format, or null on failure
 *
 * # Safety
 * - `handle` must be a valid pointer returned by `atree_new()`
 * - Caller must free the returned string with `atree_free_string()`
 */
char *atree_to_graphviz(const struct ATreeHandle *handle);

/**
 * Free a string returned by the library.
 *
 * # Safety
 * - `string` must be a valid pointer from a function that returns *mut c_char
 */
void atree_free_string(char *string);

/**
 * Start building an event for searching.
 *
 * # Safety
 * - `handle` must be a valid pointer returned by `atree_new()`
 * - Returned pointer must be freed with `atree_event_builder_free()`
 */
void *atree_event_builder_new(const struct ATreeHandle *handle);

/**
 * Add a boolean attribute to the event.
 *
 * # Safety
 * - `builder` must be a valid pointer returned by `atree_event_builder_new()`
 * - `name` must be a valid null-terminated C string
 */
struct AtreeResult atree_event_builder_with_boolean(void *builder, const char *name, bool value);

/**
 * Add an integer attribute to the event.
 *
 * # Safety
 * - `builder` must be a valid pointer returned by `atree_event_builder_new()`
 * - `name` must be a valid null-terminated C string
 */
struct AtreeResult atree_event_builder_with_integer(void *builder, const char *name, int64_t value);

/**
 * Add a string attribute to the event.
 *
 * # Safety
 * - `builder` must be a valid pointer returned by `atree_event_builder_new()`
 * - `name` and `value` must be valid null-terminated C strings
 */
struct AtreeResult atree_event_builder_with_string(void *builder,
                                                   const char *name,
                                                   const char *value);

/**
 * Add a float attribute to the event.
 *
 * The float is represented as a decimal with a mantissa and scale.
 * For example, 123.45 would be represented as number=12345, scale=2.
 *
 * # Safety
 * - `builder` must be a valid pointer returned by `atree_event_builder_new()`
 * - `name` must be a valid null-terminated C string
 */
struct AtreeResult atree_event_builder_with_float(void *builder,
                                                  const char *name,
                                                  int64_t number,
                                                  uint32_t scale);

/**
 * Add a string list attribute to the event.
 *
 * # Safety
 * - `builder` must be a valid pointer returned by `atree_event_builder_new()`
 * - `name` must be a valid null-terminated C string
 * - `values` must point to an array of `count` valid null-terminated C strings
 */
struct AtreeResult atree_event_builder_with_string_list(void *builder,
                                                        const char *name,
                                                        const char *const *values,
                                                        uintptr_t count);

/**
 * Add an integer list attribute to the event.
 *
 * # Safety
 * - `builder` must be a valid pointer returned by `atree_event_builder_new()`
 * - `name` must be a valid null-terminated C string
 * - `values` must point to an array of `count` i64 values
 */
struct AtreeResult atree_event_builder_with_integer_list(void *builder,
                                                         const char *name,
                                                         const int64_t *values,
                                                         uintptr_t count);

/**
 * Add an undefined attribute to the event.
 *
 * # Safety
 * - `builder` must be a valid pointer returned by `atree_event_builder_new()`
 * - `name` must be a valid null-terminated C string
 */
struct AtreeResult atree_event_builder_with_undefined(void *builder, const char *name);

/**
 * Search the A-Tree for matching expressions.
 *
 * # Safety
 * - `handle` must be a valid pointer returned by `atree_new()`
 * - `builder` must be a valid pointer returned by `atree_event_builder_new()`
 * - `builder` will be consumed by this call and must not be used after
 * - Caller must free the returned result with `atree_search_result_free()`
 */
struct AtreeSearchResult atree_search(const struct ATreeHandle *handle, void *builder);

/**
 * Free a search result.
 *
 * # Safety
 * - `result` must be a valid search result returned by `atree_search()`
 * - `result` must not be used after this call
 */
void atree_search_result_free(struct AtreeSearchResult result);

/**
 * Free an error message string.
 *
 * # Safety
 * - `error` must be a valid pointer from AtreeResult.error_message
 */
void atree_free_error(char *error);

/**
 * Free an event builder without using it.
 *
 * # Safety
 * - `builder` must be a valid pointer returned by `atree_event_builder_new()`
 */
void atree_event_builder_free(void *builder);

#endif  /* ATREE_H */
