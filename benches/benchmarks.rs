use a_tree::{ATree, AttributeDefinition};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};

const AN_EXPRESSION: &str = r#"exchange_id = 1 and deal_ids one of ["deal-1", "deal-2"] and segment_ids one of [1, 2, 3] and country = 'CA' and city in ['QC'] or country = 'US' and city in ['AZ']"#;
const ID: u64 = 1;
const AN_ID: &u64 = &ID;

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
    c.bench_function("search", |b| {
        b.iter_batched(
            || {
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
                builder.build().unwrap()
            },
            |event| {
                let _ = std::hint::black_box(atree.search(event));
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(benches, insert_expression, search);
criterion_main!(benches);
