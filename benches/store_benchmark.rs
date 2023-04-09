use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::{distributions::Alphanumeric, Rng};
use rkv::store::lsm_store::KVStore;
use std::env;
use std::sync::{Arc, Mutex};
use std::thread;
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

    let key_length = match env::var("KEY_LENGTH") {
        Ok(key_length) => key_length.parse().unwrap_or(500),
        Err(_) => 500,
    }; // Max 65535

    let max_threads = num_cpus::get();

    let n_threads: usize = match env::var("THREADS") {
        Ok(n_threads) => n_threads.parse().unwrap_or(max_threads),
        Err(_) => max_threads,
    };

    let n_threads = n_threads.clamp(1, max_threads);
    let key_per_table = 50000;
    let mut store = KVStore::new("benchmark".to_owned(), key_per_table, path.clone());
    let step = n_keys / 5;

    println!(
        "PARAMS:
        , n_keys={n_keys}
        , n_threads={n_threads}
        , max_threads={max_threads}
        , key_length={key_length}
        , step={step}
        , key_per_table={key_per_table} (before compaction)",
        n_keys = n_keys,
        n_threads = n_threads,
        max_threads = max_threads,
        key_length = key_length,
        key_per_table = key_per_table,
        step = step
    );

    println!("Inserting {} keys ...", n_keys);

    let mut thread_handlers = vec![];
    let test_keys: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(vec![]));
    let ctr: Arc<Mutex<u64>> = Arc::new(Mutex::new(0));

    for chunk in 0..n_threads {
        let test_keys = test_keys.clone();
        let mut store = store.clone();
        let ctr = ctr.clone();
        let handle = thread::spawn(move || {
            let start = chunk * (n_keys / n_threads);
            let end = start + (n_keys / n_threads);

            for i in start..end {
                let k = rand_string(key_length);
                if i % step == 0  {
                    match test_keys.lock() {
                        Ok(mut test_keys) => test_keys.push(k.clone()),
                        Err(e) => panic!("Poisoned lock: {:?}", e),
                    }
                }
                store.set(k.as_bytes(), k.as_bytes());
                match ctr.lock() {
                    Ok(mut ctr) => {
                        *ctr += 1;
                        println!("Inserted key: ({}/{})", ctr, n_keys);
                    },
                    Err(e) => panic!("Poisoned lock: {:?}", e),
                }
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
        "store/get/{}-keys-ofsize-{}-each",
        n_keys, key_length
    ));

    group.significance_level(0.1).sample_size(50);

    match test_keys.lock() {
        Ok(test_keys) => {
            for (i, k) in test_keys.iter().enumerate() {
                let size = k.len() + k.len();
                group.throughput(Throughput::Bytes(size as u64));
                group.bench_with_input(BenchmarkId::from_parameter(i), k, |b, k| {
                    b.iter(|| store.get(k.as_bytes()))
                });
            }        
        },
        Err(e) => panic!("Poisoned lock: {:?}", e),
    }

    group.finish();
    drop(path);
    temp_dir.close().unwrap();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
