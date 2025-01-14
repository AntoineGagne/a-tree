use a_tree::{ATree, AttributeDefinition};
use std::collections::HashMap;

const FIRST_EXPRESSION: &str = r#"exchange_id = 1 and deal_ids one of ['deal-1', 'deal-2'] and segment_ids one of [1, 2, 3] and country in ['FR', 'GB']"#;
const SECOND_EXPRESSION: &str = r#"exchange_id = 1 and deal_ids one of ['deal-1', 'deal-2'] and segment_ids one of [1, 2, 3] and country = 'CA' and city in ['QC'] or country = 'US' and city in ['AZ']"#;
const THIRD_EXPRESSION: &str = r#"exchange_id = 1 and deal_ids one of ['deal-1', 'deal-2'] and segment_ids one of [1, 2, 3] and country = 'CA' and city in ['QC'] or country = 'US'"#;

fn main() {
    // Create the A-Tree
    let attributes = [
        AttributeDefinition::integer("exchange_id"),
        AttributeDefinition::string_list("deal_ids"),
        AttributeDefinition::integer_list("segment_ids"),
        AttributeDefinition::string("country"),
        AttributeDefinition::string("city"),
    ];
    let mut atree = ATree::new(&attributes).unwrap();

    // Insert the arbitrary boolean expressions
    let expressions_by_ids = [
        (1, FIRST_EXPRESSION),
        (2, SECOND_EXPRESSION),
        (3, THIRD_EXPRESSION),
    ];
    let mappings: HashMap<u64, &str> = HashMap::from_iter(expressions_by_ids);
    for (id, expression) in &expressions_by_ids {
        atree.insert(id, expression).unwrap();
    }

    // Create the matching event
    let mut builder = atree.make_event();
    builder.with_integer("exchange_id", 5).unwrap();
    builder
        .with_string_list("deal_ids", &["deal-3", "deal-1"])
        .unwrap();
    builder
        .with_integer_list("segment_ids", &[3, 4, 5])
        .unwrap();
    builder.with_string("country", "US").unwrap();
    builder.with_string("city", "AZ").unwrap();
    let event = builder.build().unwrap();

    // Search inside the A-Tree for matching events
    let report = atree.search(&event).unwrap();
    report.matches().iter().for_each(|id| {
        println!(r#"Found ID: {id}, Expression: "{}""#, mappings[id]);
    });
}
