use log::{debug, error};
use std::io::Result;
use std::sync::{Arc, Mutex};
use std::thread;
use std::{collections::BTreeMap, path::PathBuf};

use glob::glob;

use crate::sstable::constants::{RKV, TOMBSTONE};
use crate::sstable::sst::{create_sstable, sstable_compaction, SSTable};

/// A key value store implemented as an LSM Tree.
///
/// # Example
/// ```
/// use std::path::PathBuf;
/// use rkv::store::lsm_store::KVStore;
///
/// let mut store = KVStore::new("database".to_owned(), 100, PathBuf::from("/tmp/.tmp20aefd00/book_ratings/"));
/// store.set(b"The Rust Programming language", b"5");
/// if let Some(v) = store.get(b"The Rust Programming language") {
///     assert_eq!(v.as_slice(), b"5");
/// }
/// ```
#[derive(Clone)]
pub struct KVStore {
    name: String,
    /// memtable is
    memtable: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
    mem_size: Arc<Mutex<u64>>,
    max_bytes: u64,
    sstables: Arc<Mutex<Vec<SSTable>>>,
    sstable_dir: PathBuf,
}

impl KVStore {
    pub fn new(name: String, size: u64, sstable_dir: PathBuf) -> Self {
        let mut store = KVStore {
            name,
            memtable: Arc::new(Mutex::new(BTreeMap::new())),
            mem_size: Arc::new(Mutex::new(0)),
            max_bytes: size,
            sstables: Arc::new(Mutex::new(vec![])),
            sstable_dir,
        };
        let discovered_tables = store.discover_sstables();
        store.sstables = Arc::new(Mutex::new(discovered_tables));
        store
    }

    fn is_overflow(&self) -> bool {
        match self.mem_size.lock() {
            Ok(mem_size) => *mem_size >= self.max_bytes,
            Err(e) => panic!("Failed to unlock. Reason: {}", e),
        }
    }

    /// Track the number of sstables.
    pub fn get_sstables_count(&self) -> usize {
        match self.sstables.lock() {
            Ok(sstables) => sstables.len(),
            Err(e) => panic!("Failed to lock. Reason: {}", e),
        }
    }

    /// Find sstables after restarts.
    ///
    /// As long as sstables (.rkv) files are present at the path,
    /// this method will load them before creating an instance of the `KVStore`.
    fn discover_sstables(&mut self) -> Vec<SSTable> {
        let mut sstables: Vec<SSTable> = vec![];
        let sstable_dir = self.sstable_dir.join(RKV).join("dat");
        let sstable_dir_str = sstable_dir.as_path().display().to_string();
        let glob_pattern = format!("{}/*.{}", sstable_dir_str, RKV);
        for entry in glob(&glob_pattern).expect("Failed to read glob pattern") {
            match entry {
                Ok(path) => match SSTable::new(path.clone(), true, true, false) {
                    Ok(sstable) => sstables.push(sstable),
                    Err(e) => error!(
                        "Failed to read sstable {} because {}",
                        path.as_path().display(),
                        e
                    ),
                },
                Err(e) => println!("{:?}", e),
            }
        }
        sstables
    }

    /// Reduce number of SSTables.
    ///
    /// To read K-V pairs from sstabls, we need to:
    /// 1. For each file:
    /// 1. Load the file contents into a buffer.
    /// 1. Search the key.
    ///
    /// This gets very slow as the number of sstables increase.
    /// 1. Keys that are updated frequently.
    /// 1. Keys that have been deleted.
    ///
    /// These will occupy extra space in multiple sstables. We can periodically clean up and
    /// combine sstables into single table. Since this process is also slow, we run it on a separate thread.
    pub fn compaction(&mut self) {
        self.sstables = sstable_compaction(self.sstables.clone(), &self.sstable_dir.join(&self.name));
    }

    /// Drain key-value pairs into an sstable.
    fn flush_memtable(&mut self) -> Result<()> {
        let mut sstable = create_sstable(
            self.get_sstables_count(),
            &self.sstable_dir.join(&self.name),
        );
        sstable.write(&self.memtable.lock().unwrap())?;
        match self.sstables.lock() {
            Ok(mut sstables) => sstables.push(sstable),
            Err(e) => panic!("Failed to lock. Reason: {}", e),
        }

        if self.get_sstables_count() > 1 {
            self.compaction();
        }
        self.memtable = Arc::new(Mutex::new(BTreeMap::new()));
        self.mem_size = Arc::new(Mutex::new(0));
        Ok(())
    }

    /// Set a key value pair in the store.
    pub fn set(&mut self, k: &[u8], v: &[u8]) {
        match self.mem_size.lock() {
            Ok(mut mem_size) => *mem_size += (k.len() + v.len()) as u64,
            Err(e) => panic!("Failed to lock. Reason: {}", e),
        }
        if self.is_overflow() {
            debug!("Memtable is full. Flushing to disk");

            if let Err(e) = self.flush_memtable() {
                panic!("Failed to flush memtable because {}", e);
            }
        }
        match self.memtable.lock() {
            Ok(mut memtable) => memtable.insert(k.to_vec(), v.to_vec()),
            Err(e) => panic!("Failed to lock. Reason: {}", e),
        };
    }

    /// Get the value for a key stored previously
    pub fn get(&mut self, k: &[u8]) -> Option<Vec<u8>> {
        match self.memtable.lock() {
            Ok(memtable) => {
                if let Some(v) = memtable.get(k) {
                    if v == TOMBSTONE {
                        return None;
                    }
                    return Some(v.to_vec());
                }
            }
            Err(e) => panic!("Failed to lock. Reason: {}", e),
        }
        parallel_search(self.sstables.clone(), k.to_vec())
    }

    /// Remove a key value pair.
    pub fn delete(&mut self, k: &[u8]) {
        match self.memtable.lock() {
            Ok(mut memtable) => memtable.insert(k.to_vec(), TOMBSTONE.to_vec()),
            Err(e) => panic!("Failed to lock. Reason: {}", e),
        };

        if parallel_search(self.sstables.clone(), k.to_vec()).is_some() {
            match self.mem_size.lock() {
                Ok(mut mem_size) => *mem_size += k.len() as u64,
                Err(e) => panic!("Failed to lock. Reason: {}", e),
            }
        }
    }

    /// Get the current size of memtable.
    pub fn size(&self) -> u64 {
        match self.mem_size.lock() {
            Ok(mem_size) => *mem_size,
            Err(e) => panic!("Failed to lock. Reason: {}", e),
        }
    }
}

/// Parallel search SSTables.
///
/// sstables=Vec<SSTables> is ordered such that the most recent table is at the end.
/// 1. We partition sstables so that multiple threads can search them in parallel.
/// 2. We use a channel to collect results from each thread.
fn parallel_search(shared_sstables: Arc<Mutex<Vec<SSTable>>>, k: Vec<u8>) -> Option<Vec<u8>> {
    let n_sstables = shared_sstables.lock().unwrap().len();
    let n_threads = std::cmp::min(n_sstables, 10);
    let chunk_size = (n_sstables + n_threads - 1) / n_threads;
    let key = Arc::new(k);
    let result: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
    let mut handles = vec![];
    let last_index: Arc<Mutex<Option<usize>>> = Arc::new(Mutex::new(None));

    for i in 0..n_threads {
        let sstables_locked = shared_sstables.clone();
        let key = key.clone();
        let result = result.clone();
        let last_index = last_index.clone();

        let start = i * chunk_size;
        let end = std::cmp::min(start + chunk_size, n_sstables);

        let handle = thread::spawn(move || {
            let sstables = sstables_locked.lock().unwrap();
            let sstable_chunk = &sstables[start..end];
            for (j, sstable) in sstable_chunk.iter().enumerate() {
                let mut current_last_index = last_index.lock().unwrap();
                if let Some(last_index) = *current_last_index {
                    if last_index >= start + j {
                        return;
                    }
                }

                let value = match sstable.search(&key) {
                    Ok(v) => v,
                    _ => None,
                };

                if let Some(v) = value {
                    if v == TOMBSTONE {
                        return;
                    }
                    let mut result = result.lock().unwrap();
                    *result = Some(v);
                    *current_last_index = Some(start + j);
                    return;
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Failed to join thread!");
    }

    let result = result.lock().unwrap();
    result.clone()
}
