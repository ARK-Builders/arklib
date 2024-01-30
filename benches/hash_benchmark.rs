use arklib::id::ResourceId;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::prelude::*;

fn generate_random_data(size: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    (0..size).map(|_| rng.gen()).collect()
}

fn compute_bytes_benchmark(c: &mut Criterion) {
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

criterion_group!(benches, compute_bytes_benchmark);
criterion_main!(benches);
