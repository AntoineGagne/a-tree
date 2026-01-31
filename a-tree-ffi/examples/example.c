#include <stdio.h>
#include <stdlib.h>
#include "../atree.h"

int main(void) {
    // Define attributes for the A-Tree
    AtreeAttributeDef defs[] = {
        { .name = "private", .attr_type = Boolean },
        { .name = "exchange_id", .attr_type = Integer },
        { .name = "deal_ids", .attr_type = StringList }
    };

    // Create the A-Tree
    printf("Creating A-Tree...\n");
    ATreeHandle *tree = atree_new(defs, 3);
    if (tree == NULL) {
        fprintf(stderr, "Failed to create A-Tree\n");
        return 1;
    }
    printf("A-Tree created successfully\n");

    // Insert a boolean expression
    printf("\nInserting expression...\n");
    const char *expression = "exchange_id = 1 and private";
    AtreeResult result = atree_insert(tree, 42, expression);
    if (!result.success) {
        fprintf(stderr, "Failed to insert: %s\n", result.error_message);
        atree_free_error(result.error_message);
        atree_free(tree);
        return 1;
    }
    printf("Expression inserted: '%s' with ID: 42\n", expression);

    // Build an event
    printf("\nBuilding event...\n");
    void *builder = atree_event_builder_new(tree);
    if (builder == NULL) {
        fprintf(stderr, "Failed to create event builder\n");
        atree_free(tree);
        return 1;
    }

    // Add attributes to the event
    result = atree_event_builder_with_boolean(builder, "private", true);
    if (!result.success) {
        fprintf(stderr, "Failed to add boolean: %s\n", result.error_message);
        atree_free_error(result.error_message);
        atree_event_builder_free(builder);
        atree_free(tree);
        return 1;
    }

    result = atree_event_builder_with_integer(builder, "exchange_id", 1);
    if (!result.success) {
        fprintf(stderr, "Failed to add integer: %s\n", result.error_message);
        atree_free_error(result.error_message);
        atree_event_builder_free(builder);
        atree_free(tree);
        return 1;
    }

    printf("Event built with attributes: private=true, exchange_id=1\n");

    // Search for matching expressions
    printf("\nSearching for matches...\n");
    AtreeSearchResult search_result = atree_search(tree, builder);
    // Note: builder is consumed by atree_search and should not be used after

    if (search_result.ids == NULL) {
        printf("No matches found\n");
    } else {
        printf("Found %zu match(es):\n", search_result.count);
        for (size_t i = 0; i < search_result.count; i++) {
            printf("  - Subscription ID: %lu\n", search_result.ids[i]);
        }
        atree_search_result_free(search_result);
    }

    // Clean up
    printf("\nCleaning up...\n");
    atree_free(tree);
    printf("Done!\n");

    return 0;
}
