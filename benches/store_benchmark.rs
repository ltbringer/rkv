use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rkv::store::lsm_store::KVStore;
use tempfile::TempDir;

fn as_bytes(x: Vec<u32>) -> Vec<Vec<u8>> {
    x.iter()
        .map(|k| k.to_be_bytes().to_vec())
        .collect::<Vec<Vec<u8>>>()
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let temp_dir = match TempDir::new() {
        Ok(dir) => dir,
        Err(_) => panic!("Failed creating tempdir."),
    };
    let mut store = KVStore::new(100, temp_dir.into_path());
    let n_keys = 1_000_000;
    let keys: Vec<u32> = (0..n_keys).collect();
    let values: Vec<u32> = (0..n_keys).collect();
    let key_bytes = as_bytes(keys);
    let value_bytes = as_bytes(values);
    let step = (n_keys / 10) as usize;

    let mut group = c.benchmark_group(format!("store.get for {} keys", n_keys));
    group.significance_level(0.1).sample_size(10);
    for (k, v) in key_bytes.iter().zip(value_bytes.iter()) {
        store.set(k, v);
    }

    for (i, (k, v)) in key_bytes
        .iter()
        .zip(value_bytes.iter())
        .step_by(step)
        .enumerate()
    {
        let size = k.len() + v.len();
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(i), k, |b, k| {
            b.iter(|| store.get(k))
        });
    }
    group.finish();
    drop(store);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
