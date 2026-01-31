#include <iostream>
#include <string>
#include <vector>
#include <memory>
#include <stdexcept>

extern "C" {
    #include "../atree.h"
}

// RAII wrapper for A-Tree
class ATreeWrapper {
private:
    ATreeHandle* handle_;

public:
    ATreeWrapper(const std::vector<std::pair<std::string, AtreeAttributeType>>& attrs) {
        std::vector<AtreeAttributeDef> defs;
        defs.reserve(attrs.size());

        for (const auto& [name, type] : attrs) {
            defs.push_back({name.c_str(), type});
        }

        handle_ = atree_new(defs.data(), defs.size());
        if (!handle_) {
            throw std::runtime_error("Failed to create A-Tree");
        }
    }

    ~ATreeWrapper() {
        if (handle_) {
            atree_free(handle_);
        }
    }

    // Disable copying
    ATreeWrapper(const ATreeWrapper&) = delete;
    ATreeWrapper& operator=(const ATreeWrapper&) = delete;

    // Enable moving
    ATreeWrapper(ATreeWrapper&& other) noexcept : handle_(other.handle_) {
        other.handle_ = nullptr;
    }

    ATreeWrapper& operator=(ATreeWrapper&& other) noexcept {
        if (this != &other) {
            if (handle_) {
                atree_free(handle_);
            }
            handle_ = other.handle_;
            other.handle_ = nullptr;
        }
        return *this;
    }

    void insert(uint64_t subscription_id, const std::string& expression) {
        AtreeResult result = atree_insert(handle_, subscription_id, expression.c_str());
        if (!result.success) {
            std::string error_msg = result.error_message;
            atree_free_error(result.error_message);
            throw std::runtime_error("Failed to insert: " + error_msg);
        }
    }

    void* create_event_builder() const {
        void* builder = atree_event_builder_new(handle_);
        if (!builder) {
            throw std::runtime_error("Failed to create event builder");
        }
        return builder;
    }

    std::vector<uint64_t> search(void* builder) const {
        // Note: builder is consumed by this call
        AtreeSearchResult result = atree_search(handle_, builder);

        std::vector<uint64_t> matches;
        if (result.ids != nullptr) {
            matches.assign(result.ids, result.ids + result.count);
            atree_search_result_free(result);
        }

        return matches;
    }
};

// RAII wrapper for EventBuilder
class EventBuilderWrapper {
private:
    void* builder_;

public:
    explicit EventBuilderWrapper(void* builder) : builder_(builder) {
        if (!builder_) {
            throw std::runtime_error("Invalid event builder");
        }
    }

    ~EventBuilderWrapper() {
        if (builder_) {
            atree_event_builder_free(builder_);
        }
    }

    // Disable copying
    EventBuilderWrapper(const EventBuilderWrapper&) = delete;
    EventBuilderWrapper& operator=(const EventBuilderWrapper&) = delete;

    EventBuilderWrapper& with_boolean(const std::string& name, bool value) {
        AtreeResult result = atree_event_builder_with_boolean(builder_, name.c_str(), value);
        if (!result.success) {
            std::string error_msg = result.error_message;
            atree_free_error(result.error_message);
            throw std::runtime_error("Failed to add boolean: " + error_msg);
        }
        return *this;
    }

    EventBuilderWrapper& with_integer(const std::string& name, int64_t value) {
        AtreeResult result = atree_event_builder_with_integer(builder_, name.c_str(), value);
        if (!result.success) {
            std::string error_msg = result.error_message;
            atree_free_error(result.error_message);
            throw std::runtime_error("Failed to add integer: " + error_msg);
        }
        return *this;
    }

    EventBuilderWrapper& with_string(const std::string& name, const std::string& value) {
        AtreeResult result = atree_event_builder_with_string(builder_, name.c_str(), value.c_str());
        if (!result.success) {
            std::string error_msg = result.error_message;
            atree_free_error(result.error_message);
            throw std::runtime_error("Failed to add string: " + error_msg);
        }
        return *this;
    }

    // Release ownership (for passing to search)
    void* release() {
        void* temp = builder_;
        builder_ = nullptr;
        return temp;
    }
};

int main() {
    try {
        // Create A-Tree with attribute definitions
        std::cout << "Creating A-Tree..." << std::endl;
        ATreeWrapper tree({
            {"private", Boolean},
            {"exchange_id", Integer},
            {"deal_ids", StringList}
        });
        std::cout << "A-Tree created successfully" << std::endl;

        // Insert expression
        std::cout << "\nInserting expression..." << std::endl;
        std::string expression = "exchange_id = 1 and private";
        tree.insert(42, expression);
        std::cout << "Expression inserted: '" << expression << "' with ID: 42" << std::endl;

        // Build event
        std::cout << "\nBuilding event..." << std::endl;
        EventBuilderWrapper builder(tree.create_event_builder());
        builder.with_boolean("private", true)
               .with_integer("exchange_id", 1);
        std::cout << "Event built with attributes: private=true, exchange_id=1" << std::endl;

        // Search
        std::cout << "\nSearching for matches..." << std::endl;
        auto matches = tree.search(builder.release());

        if (matches.empty()) {
            std::cout << "No matches found" << std::endl;
        } else {
            std::cout << "Found " << matches.size() << " match(es):" << std::endl;
            for (uint64_t id : matches) {
                std::cout << "  - Subscription ID: " << id << std::endl;
            }
        }

        std::cout << "\nDone!" << std::endl;
        return 0;

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}
