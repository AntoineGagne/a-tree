use a_tree::{ATree, AttributeDefinition};

fn main() {
    let attributes = [
        AttributeDefinition::integer("exchange_id"),
        AttributeDefinition::string_list("deal_ids"),
    ];
    let mut atree = ATree::new(&attributes).unwrap();
    let id = atree
        .insert(r#"deal_ids one of ["deal-1", "deal-2"]"#)
        .unwrap();
    let other_id = atree.insert(r#"exchange_id = 5"#).unwrap();

    let mut builder = atree.make_event();
    builder.with_integer("exchange_id", 5).unwrap();
    builder
        .with_string_list("deal_ids", &["deal-3", "deal-1"])
        .unwrap();
    let event = builder.build().unwrap();

    let report = atree.search(event).unwrap();
}
