use criterion::Criterion;
use rand::seq::SliceRandom;
use rand::thread_rng;
use tantivy::collector::FacetCollector;
use tantivy::doc;
use tantivy::query::AllQuery;
use tantivy::schema::{Facet, Schema};
use tantivy::Index;

pub fn bench_facet_collector(b: &mut Criterion) {
    b.bench_function("facet_collectors", |b| {
        let mut schema_builder = Schema::builder();
        let facet_field = schema_builder.add_facet_field("facet");
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);

        let mut docs = vec![];
        for val in 0..50 {
            let facet = Facet::from(&format!("/facet_{}", val));
            for _ in 0..val * val {
                docs.push(doc!(facet_field=>facet.clone()));
            }
        }
        // 40425 docs
        docs[..].shuffle(&mut thread_rng());

        let mut index_writer = index.writer_with_num_threads(1, 3_000_000).unwrap();
        for doc in docs {
            index_writer.add_document(doc);
        }
        index_writer.commit().unwrap();
        let reader = index.reader().unwrap();
        b.iter(|| {
            let searcher = reader.searcher();
            let facet_collector = FacetCollector::for_field(facet_field);
            searcher.search(&AllQuery, &facet_collector).unwrap();
        });
    });
}
