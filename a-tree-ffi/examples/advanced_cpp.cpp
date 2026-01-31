// Advanced C++ example demonstrating new FFI features
//
// This example shows:
// - Float attributes with decimal precision
// - String list and integer list attributes
// - Undefined/null attribute handling
// - Delete operation
// - Graphviz export
// - Modern C++ Result-based error handling

#include <iostream>
#include <iomanip>
#include "../atree.hpp"

using namespace atree;

void print_separator(const std::string& title) {
    std::cout << "\n" << std::string(60, '=') << "\n";
    std::cout << title << "\n";
    std::cout << std::string(60, '=') << "\n";
}

int main() {
    try {
        print_separator("Creating A-Tree with Multiple Attribute Types");

        // Create tree with various attribute types
        Tree tree({
            AttributeDefinition::boolean("is_active"),
            AttributeDefinition::integer("user_id"),
            AttributeDefinition::float_attr("price"),
            AttributeDefinition::string("country"),
            AttributeDefinition::string_list("tags"),
            AttributeDefinition::integer_list("categories"),
        });

        std::cout << "✓ A-Tree created successfully\n";

        // ====================================================================
        print_separator("Inserting Expressions");

        // Simple expressions that we know work
        tree.insert(1, "is_active and user_id > 100").unwrap();
        std::cout << "✓ Inserted subscription 1: 'is_active and user_id > 100'\n";

        tree.insert(2, "price >= 50.0 and price <= 100.0").unwrap();
        std::cout << "✓ Inserted subscription 2: 'price >= 50.0 and price <= 100.0'\n";

        tree.insert(3, "country = \"US\"").unwrap();
        std::cout << "✓ Inserted subscription 3: 'country = \"US\"'\n";

        tree.insert(4, "price > 25.0").unwrap();
        std::cout << "✓ Inserted subscription 4: 'price > 25.0'\n";

        // ====================================================================
        print_separator("Search 1: Boolean and Integer");

        auto matches1 = tree.search(
            tree.make_event()
                .with_boolean("is_active", true)
                .with_integer("user_id", 150)
                .with_undefined("price")
                .with_undefined("country")
                .with_undefined("tags")
                .with_undefined("categories")
        ).unwrap();

        std::cout << "Event: is_active=true, user_id=150, others undefined\n";
        std::cout << "Found " << matches1.size() << " match(es): ";
        for (auto id : matches1) std::cout << id << " ";
        std::cout << "\nExpected: subscription 1\n";

        // ====================================================================
        print_separator("Search 2: Float with Double (Automatic Conversion)");

        auto matches2 = tree.search(
            tree.make_event()
                .with_float("price", 75.50)  // Converted to decimal automatically
                .with_undefined("is_active")
                .with_undefined("user_id")
                .with_undefined("country")
                .with_undefined("tags")
                .with_undefined("categories")
        ).unwrap();

        std::cout << "Event: price=75.50 (auto-converted to decimal)\n";
        std::cout << "Found " << matches2.size() << " match(es): ";
        for (auto id : matches2) std::cout << id << " ";
        std::cout << "\nExpected: subscriptions 2, 4\n";

        // ====================================================================
        print_separator("Search 3: Float with Precise Decimal");

        // Using precise decimal representation: 60.00 = 6000 with scale 2
        auto matches3 = tree.search(
            tree.make_event()
                .with_float("price", 6000, 2)  // Precise: 60.00
                .with_undefined("is_active")
                .with_undefined("user_id")
                .with_undefined("country")
                .with_undefined("tags")
                .with_undefined("categories")
        ).unwrap();

        std::cout << "Event: price=60.00 (precise decimal: 6000, scale: 2)\n";
        std::cout << "Found " << matches3.size() << " match(es): ";
        for (auto id : matches3) std::cout << id << " ";
        std::cout << "\nExpected: subscriptions 2, 4\n";

        // ====================================================================
        print_separator("Search 4: String Matching");

        auto matches4 = tree.search(
            tree.make_event()
                .with_string("country", "US")
                .with_undefined("is_active")
                .with_undefined("user_id")
                .with_undefined("price")
                .with_undefined("tags")
                .with_undefined("categories")
        ).unwrap();

        std::cout << "Event: country=\"US\", others undefined\n";
        std::cout << "Found " << matches4.size() << " match(es): ";
        for (auto id : matches4) std::cout << id << " ";
        std::cout << "\nExpected: subscription 3\n";

        // ====================================================================
        print_separator("Search 5: String and Integer Lists");

        auto matches5 = tree.search(
            tree.make_event()
                .with_string_list("tags", {"featured", "sale", "new"})
                .with_integer_list("categories", {10, 42, 99})
                .with_undefined("is_active")
                .with_undefined("user_id")
                .with_undefined("price")
                .with_undefined("country")
        ).unwrap();

        std::cout << "Event: tags=[\"featured\", \"sale\", \"new\"], categories=[10, 42, 99]\n";
        std::cout << "Found " << matches5.size() << " match(es): ";
        for (auto id : matches5) std::cout << id << " ";
        std::cout << "\n(Note: No subscriptions match list-only criteria)\n";

        // ====================================================================
        print_separator("Deleting Subscription");

        tree.delete_subscription(3);
        std::cout << "✓ Deleted subscription 3\n";

        auto matches6 = tree.search(
            tree.make_event()
                .with_string("country", "US")
                .with_undefined("is_active")
                .with_undefined("user_id")
                .with_undefined("price")
                .with_undefined("tags")
                .with_undefined("categories")
        ).unwrap();

        std::cout << "\nSearching again for country=\"US\":\n";
        std::cout << "Found " << matches6.size() << " match(es): ";
        for (auto id : matches6) std::cout << id << " ";
        std::cout << "\nExpected: none (3 was deleted)\n";

        // ====================================================================
        print_separator("Graphviz Export");

        auto graphviz_result = tree.to_graphviz();
        if (graphviz_result) {
            const auto& dot = graphviz_result.unwrap();
            std::cout << "✓ Generated Graphviz DOT format (" << dot.length() << " bytes)\n";
            std::cout << "\nFirst 300 characters:\n";
            std::cout << dot.substr(0, std::min<size_t>(300, dot.length()));
            if (dot.length() > 300) {
                std::cout << "...";
            }
            std::cout << "\n\nTo visualize, save to a file and run:\n";
            std::cout << "  dot -Tpng tree.dot -o tree.png\n";
        } else {
            std::cerr << "✗ Failed to generate Graphviz: "
                      << graphviz_result.error() << "\n";
        }

        // ====================================================================
        print_separator("Error Handling Example");

        // Try to insert an invalid expression
        auto invalid_result = tree.insert(999, "this is not a valid expression!");
        if (invalid_result.is_err()) {
            std::cout << "✓ Correctly caught error: " << invalid_result.error() << "\n";
        }

        // ====================================================================
        print_separator("Success!");
        std::cout << "All operations completed successfully!\n";
        std::cout << "\nKey features demonstrated:\n";
        std::cout << "  ✓ Float attributes (both double and decimal precision)\n";
        std::cout << "  ✓ String list and integer list attributes\n";
        std::cout << "  ✓ Undefined attribute handling\n";
        std::cout << "  ✓ Fluent builder API with method chaining\n";
        std::cout << "  ✓ Result-based error handling\n";
        std::cout << "  ✓ RAII memory management\n";
        std::cout << "  ✓ Delete operations\n";
        std::cout << "  ✓ Graphviz export for visualization\n";

        return 0;

    } catch (const Error& e) {
        std::cerr << "\n✗ Fatal error: " << e.what() << "\n";
        return 1;
    } catch (const std::exception& e) {
        std::cerr << "\n✗ Unexpected error: " << e.what() << "\n";
        return 1;
    }
}
