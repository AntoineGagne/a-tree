use a_tree::{ATree, AttributeDefinition};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};

const AN_EXPRESSION: &str = r#"exchange_id = 1 and deal_ids one of ["deal-1", "deal-2"] and segment_ids one of [1, 2, 3] and country = 'CA' and city in ['QC'] or country = 'US' and city in ['AZ']"#;

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
                let _ = std::hint::black_box(atree.insert(AN_EXPRESSION));
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(benches, insert_expression);
criterion_main!(benches);
