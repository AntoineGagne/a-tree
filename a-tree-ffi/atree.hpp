// Copyright (c) 2026 A-Tree Contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

/// @file atree.hpp
/// @brief Modern C++ wrapper for the A-Tree FFI library
///
/// This header provides a clean, type-safe C++ API with RAII semantics,
/// strong error handling, and zero-overhead abstractions over the C FFI.

#ifndef ATREE_HPP
#define ATREE_HPP

#include <algorithm>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

extern "C" {
#include "atree.h"
}

namespace atree {

// ============================================================================
// Error Handling
// ============================================================================

/// @brief Exception thrown when an A-Tree operation fails
class Error : public std::runtime_error {
public:
    explicit Error(const std::string& message) : std::runtime_error(message) {}
    explicit Error(const char* message) : std::runtime_error(message) {}
};

/// @brief Result type for operations that may fail
/// @tparam T The success value type
template <typename T>
class Result {
private:
    std::optional<T> value_;
    std::optional<std::string> error_;

public:
    /// @brief Construct a successful result
    static Result ok(T value) {
        Result r;
        r.value_ = std::move(value);
        return r;
    }

    /// @brief Construct an error result
    static Result err(std::string error) {
        Result r;
        r.error_ = std::move(error);
        return r;
    }

    /// @brief Check if the result is successful
    bool is_ok() const { return value_.has_value(); }

    /// @brief Check if the result is an error
    bool is_err() const { return error_.has_value(); }

    /// @brief Get the value, throwing if error
    T& unwrap() & {
        if (is_err()) {
            throw Error(*error_);
        }
        return *value_;
    }

    /// @brief Get the value, throwing if error
    const T& unwrap() const& {
        if (is_err()) {
            throw Error(*error_);
        }
        return *value_;
    }

    /// @brief Get the value, throwing if error
    T&& unwrap() && {
        if (is_err()) {
            throw Error(*error_);
        }
        return std::move(*value_);
    }

    /// @brief Get the value or a default
    T unwrap_or(T default_value) && {
        if (is_ok()) {
            return std::move(*value_);
        }
        return default_value;
    }

    /// @brief Get the error message
    const std::string& error() const {
        if (!error_) {
            throw Error("Called error() on Ok result");
        }
        return *error_;
    }

    /// @brief Implicit conversion to bool (true if Ok)
    explicit operator bool() const { return is_ok(); }
};

/// @brief Specialization for void results
template <>
class Result<void> {
private:
    std::optional<std::string> error_;

public:
    /// @brief Construct a successful result
    static Result ok() {
        Result r;
        return r;
    }

    /// @brief Construct an error result
    static Result err(std::string error) {
        Result r;
        r.error_ = std::move(error);
        return r;
    }

    /// @brief Check if the result is successful
    bool is_ok() const { return !error_.has_value(); }

    /// @brief Check if the result is an error
    bool is_err() const { return error_.has_value(); }

    /// @brief Throw if error
    void unwrap() const {
        if (is_err()) {
            throw Error(*error_);
        }
    }

    /// @brief Get the error message
    const std::string& error() const {
        if (!error_) {
            throw Error("Called error() on Ok result");
        }
        return *error_;
    }

    /// @brief Implicit conversion to bool (true if Ok)
    explicit operator bool() const { return is_ok(); }
};

// ============================================================================
// Attribute Types
// ============================================================================

/// @brief Attribute type enumeration
enum class AttributeType {
    Boolean = 0,
    Integer = 1,
    Float = 2,
    String = 3,
    StringList = 4,
    IntegerList = 5,
};

/// @brief Attribute definition
struct AttributeDefinition {
    std::string name;
    AttributeType type;

    /// @brief Create a boolean attribute definition
    static AttributeDefinition boolean(std::string name) {
        return {std::move(name), AttributeType::Boolean};
    }

    /// @brief Create an integer attribute definition
    static AttributeDefinition integer(std::string name) {
        return {std::move(name), AttributeType::Integer};
    }

    /// @brief Create a float attribute definition
    static AttributeDefinition float_attr(std::string name) {
        return {std::move(name), AttributeType::Float};
    }

    /// @brief Create a string attribute definition
    static AttributeDefinition string(std::string name) {
        return {std::move(name), AttributeType::String};
    }

    /// @brief Create a string list attribute definition
    static AttributeDefinition string_list(std::string name) {
        return {std::move(name), AttributeType::StringList};
    }

    /// @brief Create an integer list attribute definition
    static AttributeDefinition integer_list(std::string name) {
        return {std::move(name), AttributeType::IntegerList};
    }
};

// ============================================================================
// Forward Declarations
// ============================================================================

class Tree;
class EventBuilder;

// ============================================================================
// EventBuilder - Fluent API for building events
// ============================================================================

/// @brief Builder for constructing events to search against
class EventBuilder {
private:
    void* builder_;
    bool consumed_;

    friend class Tree;

    // Private constructor - only Tree can create builders
    explicit EventBuilder(void* builder) : builder_(builder), consumed_(false) {
        if (!builder_) {
            throw Error("Failed to create event builder");
        }
    }

public:
    /// @brief Destructor - frees builder if not consumed
    ~EventBuilder() {
        if (builder_ && !consumed_) {
            atree_event_builder_free(builder_);
        }
    }

    // Disable copying
    EventBuilder(const EventBuilder&) = delete;
    EventBuilder& operator=(const EventBuilder&) = delete;

    // Enable moving
    EventBuilder(EventBuilder&& other) noexcept
        : builder_(other.builder_), consumed_(other.consumed_) {
        other.builder_ = nullptr;
        other.consumed_ = true;
    }

    EventBuilder& operator=(EventBuilder&& other) noexcept {
        if (this != &other) {
            if (builder_ && !consumed_) {
                atree_event_builder_free(builder_);
            }
            builder_ = other.builder_;
            consumed_ = other.consumed_;
            other.builder_ = nullptr;
            other.consumed_ = true;
        }
        return *this;
    }

    /// @brief Add a boolean attribute
    EventBuilder& with_boolean(std::string_view name, bool value) {
        check_not_consumed();
        AtreeResult result = atree_event_builder_with_boolean(
            builder_, std::string(name).c_str(), value);
        handle_result(result);
        return *this;
    }

    /// @brief Add an integer attribute
    EventBuilder& with_integer(std::string_view name, int64_t value) {
        check_not_consumed();
        AtreeResult result = atree_event_builder_with_integer(
            builder_, std::string(name).c_str(), value);
        handle_result(result);
        return *this;
    }

    /// @brief Add a string attribute
    EventBuilder& with_string(std::string_view name, std::string_view value) {
        check_not_consumed();
        AtreeResult result = atree_event_builder_with_string(
            builder_, std::string(name).c_str(), std::string(value).c_str());
        handle_result(result);
        return *this;
    }

    /// @brief Add a float attribute (using decimal representation)
    /// @param name Attribute name
    /// @param number Mantissa of the decimal number
    /// @param scale Number of decimal places (e.g., 123.45 = number:12345, scale:2)
    EventBuilder& with_float(std::string_view name, int64_t number, uint32_t scale) {
        check_not_consumed();
        AtreeResult result = atree_event_builder_with_float(
            builder_, std::string(name).c_str(), number, scale);
        handle_result(result);
        return *this;
    }

    /// @brief Add a float attribute from a double
    /// @param name Attribute name
    /// @param value Double value (converted to decimal with 6 decimal places)
    EventBuilder& with_float(std::string_view name, double value) {
        // Convert double to decimal with 6 decimal places
        int64_t number = static_cast<int64_t>(value * 1000000);
        return with_float(name, number, 6);
    }

    /// @brief Add a string list attribute
    EventBuilder& with_string_list(std::string_view name,
                                   const std::vector<std::string>& values) {
        check_not_consumed();
        std::vector<const char*> c_strs;
        c_strs.reserve(values.size());
        for (const auto& s : values) {
            c_strs.push_back(s.c_str());
        }
        AtreeResult result = atree_event_builder_with_string_list(
            builder_, std::string(name).c_str(), c_strs.data(), c_strs.size());
        handle_result(result);
        return *this;
    }

    /// @brief Add an integer list attribute
    EventBuilder& with_integer_list(std::string_view name,
                                    const std::vector<int64_t>& values) {
        check_not_consumed();
        AtreeResult result = atree_event_builder_with_integer_list(
            builder_, std::string(name).c_str(), values.data(), values.size());
        handle_result(result);
        return *this;
    }

    /// @brief Add an undefined/null attribute
    EventBuilder& with_undefined(std::string_view name) {
        check_not_consumed();
        AtreeResult result = atree_event_builder_with_undefined(
            builder_, std::string(name).c_str());
        handle_result(result);
        return *this;
    }

private:
    void check_not_consumed() const {
        if (consumed_) {
            throw Error("EventBuilder has already been consumed by search()");
        }
    }

    void handle_result(const AtreeResult& result) {
        if (!result.success) {
            std::string error_msg = result.error_message;
            atree_free_error(result.error_message);
            throw Error(error_msg);
        }
    }

    // Allow Tree to consume the builder
    void* release() {
        consumed_ = true;
        return builder_;
    }
};

// ============================================================================
// Tree - Main A-Tree container
// ============================================================================

/// @brief A-Tree container for boolean expression matching
class Tree {
private:
    ATreeHandle* handle_;

public:
    /// @brief Create a new A-Tree with the given attribute definitions
    /// @param definitions Vector of attribute definitions
    /// @throws Error if creation fails
    explicit Tree(const std::vector<AttributeDefinition>& definitions) {
        std::vector<AtreeAttributeDef> c_defs;
        c_defs.reserve(definitions.size());

        for (const auto& def : definitions) {
            c_defs.push_back({
                def.name.c_str(),
                static_cast<AtreeAttributeType>(def.type)
            });
        }

        handle_ = atree_new(c_defs.data(), c_defs.size());
        if (!handle_) {
            throw Error("Failed to create A-Tree");
        }
    }

    /// @brief Destructor - frees the tree
    ~Tree() {
        if (handle_) {
            atree_free(handle_);
        }
    }

    // Disable copying
    Tree(const Tree&) = delete;
    Tree& operator=(const Tree&) = delete;

    // Enable moving
    Tree(Tree&& other) noexcept : handle_(other.handle_) {
        other.handle_ = nullptr;
    }

    Tree& operator=(Tree&& other) noexcept {
        if (this != &other) {
            if (handle_) {
                atree_free(handle_);
            }
            handle_ = other.handle_;
            other.handle_ = nullptr;
        }
        return *this;
    }

    /// @brief Insert a boolean expression with an associated subscription ID
    /// @param subscription_id Unique identifier for this subscription
    /// @param expression Boolean expression string
    /// @return Result indicating success or failure
    Result<void> insert(uint64_t subscription_id, std::string_view expression) {
        AtreeResult result = atree_insert(
            handle_, subscription_id, std::string(expression).c_str());

        if (result.success) {
            return Result<void>::ok();
        } else {
            std::string error_msg = result.error_message;
            atree_free_error(result.error_message);
            return Result<void>::err(std::move(error_msg));
        }
    }

    /// @brief Delete a subscription by ID
    /// @param subscription_id ID of the subscription to remove
    void delete_subscription(uint64_t subscription_id) {
        atree_delete(handle_, subscription_id);
    }

    /// @brief Create a new event builder
    /// @return EventBuilder for constructing an event
    EventBuilder make_event() const {
        void* builder = atree_event_builder_new(handle_);
        return EventBuilder(builder);
    }

    /// @brief Search for expressions that match the given event
    /// @param builder EventBuilder containing the event data (consumed by this call)
    /// @return Result containing vector of matching subscription IDs
    Result<std::vector<uint64_t>> search(EventBuilder& builder) const {
        AtreeSearchResult result = atree_search(handle_, builder.release());

        std::vector<uint64_t> matches;
        if (result.ids != nullptr && result.count > 0) {
            matches.assign(result.ids, result.ids + result.count);
            atree_search_result_free(result);
        }

        return Result<std::vector<uint64_t>>::ok(std::move(matches));
    }

    /// @brief Search for expressions that match the given event (rvalue overload)
    /// @param builder EventBuilder containing the event data (consumed by this call)
    /// @return Result containing vector of matching subscription IDs
    Result<std::vector<uint64_t>> search(EventBuilder&& builder) const {
        return search(builder);
    }

    /// @brief Export the tree structure as Graphviz DOT format
    /// @return Result containing DOT format string
    Result<std::string> to_graphviz() const {
        char* dot = atree_to_graphviz(handle_);
        if (!dot) {
            return Result<std::string>::err("Failed to generate Graphviz output");
        }

        std::string result(dot);
        atree_free_string(dot);
        return Result<std::string>::ok(std::move(result));
    }
};

// ============================================================================
// Convenience Functions
// ============================================================================

/// @brief Create an A-Tree with attribute definitions using initializer list
/// @param definitions Initializer list of attribute definitions
/// @return Result containing the created Tree
inline Result<Tree> create_tree(
    std::initializer_list<AttributeDefinition> definitions) {
    try {
        return Result<Tree>::ok(Tree(std::vector<AttributeDefinition>(definitions)));
    } catch (const Error& e) {
        return Result<Tree>::err(e.what());
    }
}

} // namespace atree

#endif // ATREE_HPP
