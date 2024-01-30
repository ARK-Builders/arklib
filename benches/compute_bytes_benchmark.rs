use arklib::id::ResourceId;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::prelude::*;
use std::fs;

fn generate_random_data(size: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    (0..size).map(|_| rng.gen()).collect()
}

fn compute_bytes_on_raw_data(c: &mut Criterion) {
    let inputs = [
        ("compute_bytes_small", 1024),
        ("compute_bytes_medium", 8192),
        ("compute_bytes_large", 65536),
    ];

    for (name, size) in inputs.iter() {
        let input_data = generate_random_data(*size);
        c.bench_function(name, move |b| {
            b.iter(|| {
                if let Ok(result) =
                    ResourceId::compute_bytes(black_box(&input_data))
                {
                    black_box(result);
                } else {
                    panic!("compute_bytes returned an error");
                }
            });
        });
    }
}

fn compute_bytes_on_files_benchmark(c: &mut Criterion) {
    let file_paths = ["tests/lena.jpg", "tests/test.pdf"]; // Add files to benchmark here

    for file_path in file_paths.iter() {
        let raw_bytes = fs::read(file_path).unwrap();
        c.bench_function(file_path, move |b| {
            b.iter(|| {
                if let Ok(result) =
                    ResourceId::compute_bytes(black_box(&raw_bytes))
                {
                    black_box(result);
                } else {
                    panic!("compute_bytes returned an error");
                }
            });
        });
    }
}

criterion_group!(
    benches,
    compute_bytes_on_raw_data,
    compute_bytes_on_files_benchmark
);
criterion_main!(benches);
