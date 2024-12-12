use a_tree::{ATree, AttributeDefinition};

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
    let id = atree
        .insert(r#"exchange_id = 1 and deal_ids one of ["deal-1", "deal-2"] and segment_ids one of [1, 2, 3] and country in ['FR', 'GB']"#)
        .unwrap();
    let other_id = atree.insert(r#"exchange_id = 1 and deal_ids one of ["deal-1", "deal-2"] and segment_ids one of [1, 2, 3] and country = 'CA' and city in ['QC'] or country = 'US' and city in ['AZ']"#).unwrap();

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

    let report = atree.search(event).unwrap();
}
