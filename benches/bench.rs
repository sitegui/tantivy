mod facet_collectors;

use criterion::{criterion_group, criterion_main};

criterion_group!(benches, facet_collectors::bench_facet_collector);
criterion_main!(benches);
