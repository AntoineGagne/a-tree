#include <iostream>
#include <memory>
#include <vector>
#include <string>

// Include the generated header from cxx. The exact include path depends on your build setup.
// cxx generates a header for the bridge; adjust include path as appropriate for your build.
#include "ffi.rs.h" // the generated header for the bridge module (may be in "cxxbridge/..." depending on build)

using namespace a_tree_ffi;

int main() {
    // Build attribute definitions
    std::vector<AttributeDefinition> defs;
    defs.push_back(AttributeDefinition{"private", AttributeKind::Boolean});
    defs.push_back(AttributeDefinition{"exchange_id", AttributeKind::Integer});
    defs.push_back(AttributeDefinition{"deal_ids", AttributeKind::StringList});
    // Create tree
    auto maybe_tree = atree_new(defs);
    if (!maybe_tree.has_value()) {
        std::cerr << "Failed to create tree: " << maybe_tree.error() << std::endl;
        return 1;
    }
    std::unique_ptr<ATreeU64> tree = std::move(maybe_tree.value());

    // Insert a subscription
    auto res = atree_insert(*tree, 42ull, "exchange_id = 1 and private");
    if (!res.has_value()) {
        std::cerr << "Insert error: " << res.error() << std::endl;
        return 1;
    }

    // Build an Event
    EventValue v;
    v.tag = EventValueTag::Boolean;
    v.boolean = false;

    EventAttribute a;
    a.name = "private";
    a.value = v;

    Event ev;
    ev.attributes.push_back(a);

    // Search
    auto maybe_matches = atree_search(*tree, ev);
    if (!maybe_matches.has_value()) {
        std::cerr << "Search error: " << maybe_matches.error() << std::endl;
        return 1;
    }
    std::vector<uint64_t> matches = maybe_matches.value();
    std::cout << "Matches:";
    for (auto id : matches) std::cout << " " << id;
    std::cout << std::endl;
    return 0;
}
