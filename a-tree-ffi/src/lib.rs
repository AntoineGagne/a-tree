//! C FFI bindings for the a-tree library.
//!
//! This crate provides a C-compatible API for using the a-tree library from C/C++ code.

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_void};
use std::ptr;
use std::slice;

use a_tree::{ATree, AttributeDefinition};

/// Opaque handle to an ATree instance
pub struct ATreeHandle {
    tree: ATree<u64>,
}

/// Attribute types supported by the A-Tree
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub enum AtreeAttributeType {
    Boolean = 0,
    Integer = 1,
    Float = 2,
    String = 3,
    StringList = 4,
    IntegerList = 5,
}

/// Attribute definition for creating an A-Tree
#[repr(C)]
pub struct AtreeAttributeDef {
    pub name: *const c_char,
    pub attr_type: AtreeAttributeType,
}

/// Result type for operations that can fail
#[repr(C)]
pub struct AtreeResult {
    pub success: bool,
    pub error_message: *mut c_char,
}

/// Search result containing matching subscription IDs
#[repr(C)]
pub struct AtreeSearchResult {
    pub ids: *mut u64,
    pub count: usize,
}

impl AtreeResult {
    fn ok() -> Self {
        Self {
            success: true,
            error_message: ptr::null_mut(),
        }
    }

    fn err(msg: &str) -> Self {
        let c_msg = CString::new(msg).unwrap_or_else(|_| CString::new("Invalid error message").unwrap());
        Self {
            success: false,
            error_message: c_msg.into_raw(),
        }
    }
}

/// Create a new A-Tree with the given attribute definitions.
///
/// # Arguments
/// * `defs` - Array of attribute definitions
/// * `count` - Number of definitions in the array
///
/// # Returns
/// Pointer to ATreeHandle on success, null on failure
///
/// # Safety
/// - `defs` must point to valid memory containing `count` AtreeAttributeDef structs
/// - Each `name` field must be a valid null-terminated C string
/// - Caller must free the returned handle with `atree_free()`
#[no_mangle]
pub unsafe extern "C" fn atree_new(defs: *const AtreeAttributeDef, count: usize) -> *mut ATreeHandle {
    if defs.is_null() || count == 0 {
        return ptr::null_mut();
    }

    let defs_slice = slice::from_raw_parts(defs, count);
    let mut attr_defs = Vec::with_capacity(count);

    for def in defs_slice {
        if def.name.is_null() {
            return ptr::null_mut();
        }

        let name = match CStr::from_ptr(def.name).to_str() {
            Ok(s) => s,
            Err(_) => return ptr::null_mut(),
        };

        let attr_def = match def.attr_type {
            AtreeAttributeType::Boolean => AttributeDefinition::boolean(name),
            AtreeAttributeType::Integer => AttributeDefinition::integer(name),
            AtreeAttributeType::Float => AttributeDefinition::float(name),
            AtreeAttributeType::String => AttributeDefinition::string(name),
            AtreeAttributeType::StringList => AttributeDefinition::string_list(name),
            AtreeAttributeType::IntegerList => AttributeDefinition::integer_list(name),
        };

        attr_defs.push(attr_def);
    }

    match ATree::<u64>::new(&attr_defs) {
        Ok(tree) => Box::into_raw(Box::new(ATreeHandle { tree })),
        Err(_) => ptr::null_mut(),
    }
}

/// Free an A-Tree handle.
///
/// # Safety
/// - `handle` must be a valid pointer returned by `atree_new()`
/// - `handle` must not be used after this call
#[no_mangle]
pub unsafe extern "C" fn atree_free(handle: *mut ATreeHandle) {
    if !handle.is_null() {
        drop(Box::from_raw(handle));
    }
}

/// Insert a boolean expression associated with a subscription ID.
///
/// # Arguments
/// * `handle` - Valid ATree handle
/// * `subscription_id` - Unique ID for this subscription
/// * `expression` - Null-terminated boolean expression string
///
/// # Returns
/// Result indicating success or failure
///
/// # Safety
/// - `handle` must be a valid pointer returned by `atree_new()`
/// - `expression` must be a valid null-terminated C string
/// - Caller must free result.error_message with `atree_free_error()` if !success
#[no_mangle]
pub unsafe extern "C" fn atree_insert(
    handle: *mut ATreeHandle,
    subscription_id: u64,
    expression: *const c_char,
) -> AtreeResult {
    if handle.is_null() || expression.is_null() {
        return AtreeResult::err("Invalid arguments");
    }

    let expr_str = match CStr::from_ptr(expression).to_str() {
        Ok(s) => s,
        Err(_) => return AtreeResult::err("Invalid UTF-8 in expression"),
    };

    let handle_ref = &mut *handle;
    match handle_ref.tree.insert(&subscription_id, expr_str) {
        Ok(_) => AtreeResult::ok(),
        Err(e) => AtreeResult::err(&format!("{:?}", e)),
    }
}

/// Delete a subscription by ID.
///
/// # Arguments
/// * `handle` - Valid ATree handle
/// * `subscription_id` - ID of the subscription to delete
///
/// # Safety
/// - `handle` must be a valid pointer returned by `atree_new()`
#[no_mangle]
pub unsafe extern "C" fn atree_delete(
    handle: *mut ATreeHandle,
    subscription_id: u64,
) {
    if handle.is_null() {
        return;
    }

    let handle_ref = &mut *handle;
    handle_ref.tree.delete(&subscription_id);
}

/// Export the tree structure as a Graphviz DOT format string.
///
/// # Returns
/// Null-terminated string containing DOT format, or null on failure
///
/// # Safety
/// - `handle` must be a valid pointer returned by `atree_new()`
/// - Caller must free the returned string with `atree_free_string()`
#[no_mangle]
pub unsafe extern "C" fn atree_to_graphviz(handle: *const ATreeHandle) -> *mut c_char {
    if handle.is_null() {
        return ptr::null_mut();
    }

    let handle_ref = &*handle;
    let dot = handle_ref.tree.to_graphviz();

    match CString::new(dot) {
        Ok(c_str) => c_str.into_raw(),
        Err(_) => ptr::null_mut(),
    }
}

/// Free a string returned by the library.
///
/// # Safety
/// - `string` must be a valid pointer from a function that returns *mut c_char
#[no_mangle]
pub unsafe extern "C" fn atree_free_string(string: *mut c_char) {
    if !string.is_null() {
        drop(CString::from_raw(string));
    }
}

/// Start building an event for searching.
///
/// # Safety
/// - `handle` must be a valid pointer returned by `atree_new()`
/// - Returned pointer must be freed with `atree_event_builder_free()`
#[no_mangle]
pub unsafe extern "C" fn atree_event_builder_new(handle: *const ATreeHandle) -> *mut c_void {
    if handle.is_null() {
        return ptr::null_mut();
    }

    let handle_ref = &*handle;
    let builder = handle_ref.tree.make_event();
    Box::into_raw(Box::new(builder)) as *mut c_void
}

/// Add a boolean attribute to the event.
///
/// # Safety
/// - `builder` must be a valid pointer returned by `atree_event_builder_new()`
/// - `name` must be a valid null-terminated C string
#[no_mangle]
pub unsafe extern "C" fn atree_event_builder_with_boolean(
    builder: *mut c_void,
    name: *const c_char,
    value: bool,
) -> AtreeResult {
    if builder.is_null() || name.is_null() {
        return AtreeResult::err("Invalid arguments");
    }

    let name_str = match CStr::from_ptr(name).to_str() {
        Ok(s) => s,
        Err(_) => return AtreeResult::err("Invalid UTF-8 in name"),
    };

    let builder_ref = &mut *(builder as *mut a_tree::EventBuilder);
    match builder_ref.with_boolean(name_str, value) {
        Ok(_) => AtreeResult::ok(),
        Err(e) => AtreeResult::err(&format!("{:?}", e)),
    }
}

/// Add an integer attribute to the event.
///
/// # Safety
/// - `builder` must be a valid pointer returned by `atree_event_builder_new()`
/// - `name` must be a valid null-terminated C string
#[no_mangle]
pub unsafe extern "C" fn atree_event_builder_with_integer(
    builder: *mut c_void,
    name: *const c_char,
    value: i64,
) -> AtreeResult {
    if builder.is_null() || name.is_null() {
        return AtreeResult::err("Invalid arguments");
    }

    let name_str = match CStr::from_ptr(name).to_str() {
        Ok(s) => s,
        Err(_) => return AtreeResult::err("Invalid UTF-8 in name"),
    };

    let builder_ref = &mut *(builder as *mut a_tree::EventBuilder);
    match builder_ref.with_integer(name_str, value) {
        Ok(_) => AtreeResult::ok(),
        Err(e) => AtreeResult::err(&format!("{:?}", e)),
    }
}

/// Add a string attribute to the event.
///
/// # Safety
/// - `builder` must be a valid pointer returned by `atree_event_builder_new()`
/// - `name` and `value` must be valid null-terminated C strings
#[no_mangle]
pub unsafe extern "C" fn atree_event_builder_with_string(
    builder: *mut c_void,
    name: *const c_char,
    value: *const c_char,
) -> AtreeResult {
    if builder.is_null() || name.is_null() || value.is_null() {
        return AtreeResult::err("Invalid arguments");
    }

    let name_str = match CStr::from_ptr(name).to_str() {
        Ok(s) => s,
        Err(_) => return AtreeResult::err("Invalid UTF-8 in name"),
    };

    let value_str = match CStr::from_ptr(value).to_str() {
        Ok(s) => s,
        Err(_) => return AtreeResult::err("Invalid UTF-8 in value"),
    };

    let builder_ref = &mut *(builder as *mut a_tree::EventBuilder);
    match builder_ref.with_string(name_str, value_str) {
        Ok(_) => AtreeResult::ok(),
        Err(e) => AtreeResult::err(&format!("{:?}", e)),
    }
}

/// Add a float attribute to the event.
///
/// The float is represented as a decimal with a mantissa and scale.
/// For example, 123.45 would be represented as number=12345, scale=2.
///
/// # Safety
/// - `builder` must be a valid pointer returned by `atree_event_builder_new()`
/// - `name` must be a valid null-terminated C string
#[no_mangle]
pub unsafe extern "C" fn atree_event_builder_with_float(
    builder: *mut c_void,
    name: *const c_char,
    number: i64,
    scale: u32,
) -> AtreeResult {
    if builder.is_null() || name.is_null() {
        return AtreeResult::err("Invalid arguments");
    }

    let name_str = match CStr::from_ptr(name).to_str() {
        Ok(s) => s,
        Err(_) => return AtreeResult::err("Invalid UTF-8 in name"),
    };

    let builder_ref = &mut *(builder as *mut a_tree::EventBuilder);
    match builder_ref.with_float(name_str, number, scale) {
        Ok(_) => AtreeResult::ok(),
        Err(e) => AtreeResult::err(&format!("{:?}", e)),
    }
}

/// Add a string list attribute to the event.
///
/// # Safety
/// - `builder` must be a valid pointer returned by `atree_event_builder_new()`
/// - `name` must be a valid null-terminated C string
/// - `values` must point to an array of `count` valid null-terminated C strings
#[no_mangle]
pub unsafe extern "C" fn atree_event_builder_with_string_list(
    builder: *mut c_void,
    name: *const c_char,
    values: *const *const c_char,
    count: usize,
) -> AtreeResult {
    if builder.is_null() || name.is_null() || values.is_null() {
        return AtreeResult::err("Invalid arguments");
    }

    let name_str = match CStr::from_ptr(name).to_str() {
        Ok(s) => s,
        Err(_) => return AtreeResult::err("Invalid UTF-8 in name"),
    };

    let values_slice = slice::from_raw_parts(values, count);
    let mut string_vec = Vec::with_capacity(count);

    for &value_ptr in values_slice {
        if value_ptr.is_null() {
            return AtreeResult::err("Null pointer in string list");
        }
        let value_str = match CStr::from_ptr(value_ptr).to_str() {
            Ok(s) => s,
            Err(_) => return AtreeResult::err("Invalid UTF-8 in string list"),
        };
        string_vec.push(value_str);
    }

    let builder_ref = &mut *(builder as *mut a_tree::EventBuilder);
    match builder_ref.with_string_list(name_str, &string_vec) {
        Ok(_) => AtreeResult::ok(),
        Err(e) => AtreeResult::err(&format!("{:?}", e)),
    }
}

/// Add an integer list attribute to the event.
///
/// # Safety
/// - `builder` must be a valid pointer returned by `atree_event_builder_new()`
/// - `name` must be a valid null-terminated C string
/// - `values` must point to an array of `count` i64 values
#[no_mangle]
pub unsafe extern "C" fn atree_event_builder_with_integer_list(
    builder: *mut c_void,
    name: *const c_char,
    values: *const i64,
    count: usize,
) -> AtreeResult {
    if builder.is_null() || name.is_null() || values.is_null() {
        return AtreeResult::err("Invalid arguments");
    }

    let name_str = match CStr::from_ptr(name).to_str() {
        Ok(s) => s,
        Err(_) => return AtreeResult::err("Invalid UTF-8 in name"),
    };

    let values_slice = slice::from_raw_parts(values, count);

    let builder_ref = &mut *(builder as *mut a_tree::EventBuilder);
    match builder_ref.with_integer_list(name_str, values_slice) {
        Ok(_) => AtreeResult::ok(),
        Err(e) => AtreeResult::err(&format!("{:?}", e)),
    }
}

/// Add an undefined attribute to the event.
///
/// # Safety
/// - `builder` must be a valid pointer returned by `atree_event_builder_new()`
/// - `name` must be a valid null-terminated C string
#[no_mangle]
pub unsafe extern "C" fn atree_event_builder_with_undefined(
    builder: *mut c_void,
    name: *const c_char,
) -> AtreeResult {
    if builder.is_null() || name.is_null() {
        return AtreeResult::err("Invalid arguments");
    }

    let name_str = match CStr::from_ptr(name).to_str() {
        Ok(s) => s,
        Err(_) => return AtreeResult::err("Invalid UTF-8 in name"),
    };

    let builder_ref = &mut *(builder as *mut a_tree::EventBuilder);
    match builder_ref.with_undefined(name_str) {
        Ok(_) => AtreeResult::ok(),
        Err(e) => AtreeResult::err(&format!("{:?}", e)),
    }
}

/// Search the A-Tree for matching expressions.
///
/// # Safety
/// - `handle` must be a valid pointer returned by `atree_new()`
/// - `builder` must be a valid pointer returned by `atree_event_builder_new()`
/// - `builder` will be consumed by this call and must not be used after
/// - Caller must free the returned result with `atree_search_result_free()`
#[no_mangle]
pub unsafe extern "C" fn atree_search(
    handle: *const ATreeHandle,
    builder: *mut c_void,
) -> AtreeSearchResult {
    if handle.is_null() || builder.is_null() {
        return AtreeSearchResult {
            ids: ptr::null_mut(),
            count: 0,
        };
    }

    let handle_ref = &*handle;
    let builder_owned = Box::from_raw(builder as *mut a_tree::EventBuilder);

    let event = match builder_owned.build() {
        Ok(e) => e,
        Err(_) => {
            return AtreeSearchResult {
                ids: ptr::null_mut(),
                count: 0,
            }
        }
    };

    let report = match handle_ref.tree.search(&event) {
        Ok(r) => r,
        Err(_) => {
            return AtreeSearchResult {
                ids: ptr::null_mut(),
                count: 0,
            }
        }
    };

    let matches: Vec<u64> = report.matches().iter().map(|&&id| id).collect();
    let count = matches.len();

    if count == 0 {
        AtreeSearchResult {
            ids: ptr::null_mut(),
            count: 0,
        }
    } else {
        let boxed = matches.into_boxed_slice();
        let ptr = Box::into_raw(boxed) as *mut u64;
        AtreeSearchResult { ids: ptr, count }
    }
}

/// Free a search result.
///
/// # Safety
/// - `result` must be a valid search result returned by `atree_search()`
/// - `result` must not be used after this call
#[no_mangle]
pub unsafe extern "C" fn atree_search_result_free(result: AtreeSearchResult) {
    if !result.ids.is_null() && result.count > 0 {
        drop(Box::from_raw(slice::from_raw_parts_mut(
            result.ids,
            result.count,
        )));
    }
}

/// Free an error message string.
///
/// # Safety
/// - `error` must be a valid pointer from AtreeResult.error_message
#[no_mangle]
pub unsafe extern "C" fn atree_free_error(error: *mut c_char) {
    if !error.is_null() {
        drop(CString::from_raw(error));
    }
}

/// Free an event builder without using it.
///
/// # Safety
/// - `builder` must be a valid pointer returned by `atree_event_builder_new()`
#[no_mangle]
pub unsafe extern "C" fn atree_event_builder_free(builder: *mut c_void) {
    if !builder.is_null() {
        drop(Box::from_raw(builder as *mut a_tree::EventBuilder));
    }
}
