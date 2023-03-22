use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rkv::store::lsm_store::KVStore;
use tempfile::TempDir;


fn as_bytes(x: Vec<u32>) -> Vec<Vec<u8>> {
    x.iter().map(|k| k.to_be_bytes().to_vec()).collect::<Vec<Vec<u8>>>()
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let temp_dir = match TempDir::new() {
        Ok(dir) => dir,
        Err(_) => panic!("Failed creating tempdir."),
    };
    let mut store = KVStore::new(100, temp_dir.into_path());
    let max_range = 10_000_000;
    let keys: Vec<u32> = (0..max_range).collect();
    let values: Vec<u32> = (0..max_range).collect();
    let key_bytes = as_bytes(keys);
    let value_bytes = as_bytes(values);

    for (k, v) in key_bytes.iter().zip(value_bytes.iter()) {
        store.set(k, v);
    }

    c.bench_function("store_get", |b| {
        b.iter(|| {
            store.get(black_box(&key_bytes[(max_range / 2) as usize]))
        });
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);