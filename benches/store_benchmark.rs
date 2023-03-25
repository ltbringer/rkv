use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rkv::store::lsm_store::KVStore;
use tempfile::TempDir;

fn as_bytes(x: Vec<u64>) -> Vec<Vec<u8>> {
    x.iter()
        .map(|k| k.to_be_bytes().to_vec())
        .collect::<Vec<Vec<u8>>>()
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let temp_dir = match TempDir::new() {
        Ok(dir) => dir,
        Err(_) => panic!("Failed creating tempdir."),
    };
    let n_keys = 1_000_000;
    let size = 16;
    let key_per_table = 100_000 * size;
    let mut store = KVStore::new(key_per_table, temp_dir.into_path());
    let step = (n_keys / 5) as usize;
    let keys = (0..n_keys).collect::<Vec<u64>>();
    let key_bytes = as_bytes(keys);

    for k in key_bytes.iter() {
        store.set(k, k);
    }

    let mut group = c.benchmark_group(format!(
        "store.get-for-{}-keys-in-{}-sstable(s)",
        n_keys,
        (n_keys * size) / key_per_table
    ));
    group.significance_level(0.1).sample_size(10);

    for (i, k) in key_bytes.iter().step_by(step).enumerate() {
        let size = k.len() + k.len();
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
