use arklib::index::ResourceIndex;
use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion,
};

fn index_build_benchmark(c: &mut Criterion) {
    let path = "tests/"; // Set the path to the directory containing the resources here

    // assert the path exists and is a directory
    assert!(
        std::path::Path::new(path).is_dir(),
        "The path: {} does not exist or is not a directory",
        path
    );

    let mut group = c.benchmark_group("index_build");
    group.measurement_time(std::time::Duration::from_secs(20)); // Set the measurement time here

    group.bench_with_input(
        BenchmarkId::new("index_build", path),
        &path,
        |b, path| {
            b.iter(|| {
                ResourceIndex::build(black_box(path.to_string()));
            });
        },
    );

    group.finish();

    // Print the number of collisions (i.e. resources with the same hash)
    let index = ResourceIndex::build(path.to_string());
    let collisions_size = index.collisions.len();
    println!("Collisions: {}", collisions_size);
}

criterion_group!(benches, index_build_benchmark);
criterion_main!(benches);
