use arklib::index::ResourceIndex;
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn index_build_benchmark(c: &mut Criterion) {
    let path = "tests/"; // Set the path to the directory containing the resources

    c.bench_function("index_build", move |b| {
        b.iter(|| {
            let _index = ResourceIndex::build(black_box(path.to_string()));
        });
    });
}

criterion_group!(benches, index_build_benchmark);
criterion_main!(benches);
