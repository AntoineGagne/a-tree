#include <iostream>
#include <string>

// Include the cxx-generated header. Ensure -I points to the directory containing ffi.rs.h.
#include "ffi.rs.h"

using namespace a_tree_ffi;
using ::rust::Vec;
using ::rust::String;
using ::rust::Box;

int main() {
    // Build attribute definitions using rust::Vec and rust::String
    Vec<AttributeDefinition> defs;
    defs.push_back(AttributeDefinition{ String("private"), AttributeKind::Boolean });
    defs.push_back(AttributeDefinition{ String("exchange_id"), AttributeKind::Integer });
    defs.push_back(AttributeDefinition{ String("deal_ids"), AttributeKind::StringList });

    // Create the tree (returns rust::Box<ATreeU64>)
    Box<ATreeU64> tree = atree_new(std::move(defs));

    // Insert a subscription (atree_insert returns void)
    atree_insert(*tree, 42ull, "exchange_id = 1 and private");

    // Build an Event with one boolean attribute
    Vec<EventAttribute> attributes;
    EventValue val;
    val.tag = EventValueTag::Boolean;
    val.boolean = false;

    EventAttribute attr;
    attr.name = String("private");
    attr.value = std::move(val);

    attributes.push_back(std::move(attr));

    Event ev;
    ev.attributes = std::move(attributes);

    // Search (returns rust::Vec<uint64_t>)
    Vec<uint64_t> matches = atree_search(*tree, std::move(ev));

    std::cout << "Matches:";
    for (auto id : matches) {
        std::cout << " " << id;
    }
    std::cout << std::endl;

    // tree (rust::Box) will be dropped automatically when it goes out of scope,
    // which calls back into Rust to free the object.
    return 0;
}
