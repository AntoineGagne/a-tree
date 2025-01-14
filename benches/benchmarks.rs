use a_tree::{ATree, AttributeDefinition};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use itertools::Itertools;
use serde::Deserialize;
use std::collections::HashMap;

const AN_EXPRESSION: &str = r#"exchange_id = 1 and deal_ids one of ["deal-1", "deal-2"] and segment_ids one of [1, 2, 3] and country = 'CA' and city in ['QC'] or country = 'US' and city in ['AZ']"#;
const ID: u64 = 1;
const AN_ID: &u64 = &ID;

const SEARCH_FILE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/benches/data/search.json"
));

pub fn insert_expression(c: &mut Criterion) {
    c.bench_function("insert", |b| {
        b.iter_batched(
            || {
                let attributes = [
                    AttributeDefinition::integer("exchange_id"),
                    AttributeDefinition::string_list("deal_ids"),
                    AttributeDefinition::integer_list("segment_ids"),
                    AttributeDefinition::string("country"),
                    AttributeDefinition::string("city"),
                ];
                ATree::new(&attributes).unwrap()
            },
            |mut atree| {
                let _ = std::hint::black_box(atree.insert(AN_ID, AN_EXPRESSION));
            },
            BatchSize::SmallInput,
        )
    });
}

pub fn search(c: &mut Criterion) {
    let attributes = [
        AttributeDefinition::integer("exchange_id"),
        AttributeDefinition::string_list("deal_ids"),
        AttributeDefinition::integer_list("segment_ids"),
        AttributeDefinition::string("country"),
        AttributeDefinition::string("city"),
    ];
    let mut atree = ATree::new(&attributes).unwrap();
    atree.insert(AN_ID, AN_EXPRESSION).unwrap();
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
    c.bench_function("search", |b| {
        b.iter(|| {
            let _ = std::hint::black_box(atree.search(&event));
        })
    });
}

#[derive(Deserialize)]
struct SearchContent {
    attributes: HashMap<String, AttributeType>,
    events: Vec<HashMap<String, EventValue>>,
    expressions: Vec<Expression>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum AttributeType {
    Boolean,
    String,
    StringList,
    Integer,
    IntegerList,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum EventValue {
    Boolean(bool),
    String(String),
    StringList(Vec<String>),
    Integer(i64),
    IntegerList(Vec<i64>),
}

#[derive(Deserialize)]
struct Expression {
    id: u64,
    expression: String,
}

pub fn search_with_files(c: &mut Criterion) {
    let content: SearchContent = serde_json::from_str(SEARCH_FILE).unwrap();
    let attributes = content
        .attributes
        .iter()
        .map(|(name, kind)| match kind {
            AttributeType::String => AttributeDefinition::string(name),
            AttributeType::Boolean => AttributeDefinition::boolean(name),
            AttributeType::Integer => AttributeDefinition::integer(name),
            AttributeType::StringList => AttributeDefinition::string_list(name),
            AttributeType::IntegerList => AttributeDefinition::integer_list(name),
        })
        .collect_vec();
    let mut atree = ATree::new(&attributes).unwrap();
    content
        .expressions
        .iter()
        .for_each(|Expression { id, expression }| atree.insert(id, expression).unwrap());

    let events = content
        .events
        .iter()
        .map(|event| {
            let mut builder = atree.make_event();
            event.iter().for_each(|(name, value)| match value {
                EventValue::String(value) => {
                    builder.with_string(name, value).unwrap();
                }
                EventValue::Boolean(value) => {
                    builder.with_boolean(name, *value).unwrap();
                }
                EventValue::Integer(value) => {
                    builder.with_integer(name, *value).unwrap();
                }
                EventValue::StringList(value) => {
                    builder
                        .with_string_list(
                            name,
                            value.iter().map(|x| x.as_str()).collect_vec().as_slice(),
                        )
                        .unwrap();
                }
                EventValue::IntegerList(value) => {
                    builder.with_integer_list(name, value).unwrap();
                }
            });
            builder.build().unwrap()
        })
        .collect_vec();
    c.bench_function("search_with_files", |b| {
        b.iter(|| {
            for event in &events {
                let _ = std::hint::black_box(atree.search(event));
            }
        })
    });
}

criterion_group!(benches, insert_expression, search, search_with_files);
criterion_main!(benches);
