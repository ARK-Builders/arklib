use arklib::id::ResourceId;
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn compute_bytes_benchmark(c: &mut Criterion) {
    let inputs = [
        ("compute_bytes_small", vec![0u8; 64]),
        ("compute_bytes_medium", vec![1u8; 512]),
        ("compute_bytes_large", vec![2u8; 4096]),
    ];

    for (name, input_data) in inputs.iter() {
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
