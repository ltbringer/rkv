use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rkv::store::lsm_store::KVStore;
use rand::{distributions::Alphanumeric, Rng};
use std::env;
use std::thread;
use std::sync::Arc;
use tempfile::tempdir;

fn rand_string(l: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(l)
        .map(char::from)
        .collect()
}

pub fn criterion_benchmark(c: &mut Criterion) {
    println!("Creating temp dir ...");
    let temp_dir = tempdir().unwrap();
    let path = temp_dir.path().to_path_buf();

    let default_n_keys = 100_000;
    let n_keys = match env::var("N_KEYS") {
        Ok(env_n_keys) => env_n_keys.parse().unwrap_or(default_n_keys),
        Err(_) => default_n_keys,
    };

    let key_length = 6000; // Max 65535
    let key_per_table = 100_000;
    let mut store = KVStore::new("benchmark".to_owned(), key_per_table, path.clone());
    let step = (n_keys / 5) as usize;

    println!("Creating {} random key strings.", n_keys);

    let keys = (0..n_keys).map(|_| rand_string(key_length)).collect::<Vec<String>>();
    let key_chunks: Vec<Vec<String>> = keys.chunks(8).map(|c| c.to_vec()).collect();

    println!("PARAMS:
        , n_keys={}
        , key_length={}
        , key_per_table={}
        , step={}", n_keys, key_length, key_per_table, step);

    println!("Inserting {} keys ...", n_keys);

    let mut thread_handlers = vec![];

    for k in key_chunks.iter() {
        let chunk_ref = Arc::new(k.clone());
        let mut store_ref = store.clone();
        let handle = thread::spawn(move || {
            for k in chunk_ref.iter() {
                store_ref.set(k.as_bytes(), k.as_bytes());
            }
        });
        thread_handlers.push(handle);
    }

    for handle in thread_handlers {
        handle.join().unwrap();
        println!("Joined thread ...");
    }

    println!("Finished inserting {} keys.", n_keys);

    let mut group = c.benchmark_group(format!(
        "get-perf_{}-keys-ofsize-{}-each",
        n_keys,
        key_length
    ));

    group.significance_level(0.1).sample_size(10);

    for (i, k) in keys.iter().step_by(step).enumerate() {
        let size = k.len() + k.len();
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(i), k, |b, k| {
            b.iter(|| store.get(k.as_bytes()))
        });
    }

    group.finish();
    drop(path);
    temp_dir.close().unwrap();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
