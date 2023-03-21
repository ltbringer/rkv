use log::{debug, error};
use std::fs::create_dir_all;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;
use std::{collections::HashMap, path::PathBuf};

use glob::glob;
use uuid::Uuid;

use crate::sstable::constants::{RKV, TOMBSTONE};
use crate::sstable::sst::SSTable;

/// A key value store implemented as an LSM Tree.
///
/// # Example
/// ```
/// use std::path::PathBuf;
/// use rkv::store::lsm_store::KVStore;
///
/// let mut store = KVStore::new(100, PathBuf::from("/tmp/.tmp20aefd00/book_ratings/"));
/// store.set(b"The Rust Programming language", b"5");
/// if let Some(v) = store.get(b"The Rust Programming language") {
///     assert_eq!(v.as_slice(), b"5");
/// }
/// ```
pub struct KVStore {
    /// memtable is
    memtable: HashMap<Vec<u8>, Vec<u8>>,
    mem_size: u64,
    max_bytes: u64,
    sstables: Vec<SSTable>,
    sstable_dir: PathBuf,
}

impl KVStore {
    pub fn new(size: u64, sstable_dir: PathBuf) -> Self {
        let mut store = KVStore {
            memtable: HashMap::new(),
            mem_size: 0,
            max_bytes: size,
            sstables: vec![],
            sstable_dir,
        };
        store.discover_sstables();
        store
    }

    fn is_overflow(&self) -> bool {
        self.max_bytes < self.mem_size
    }

    /// Track the number of sstables.
    pub fn get_sstables_count(&self) -> usize {
        self.sstables.len()
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
        let sstables_ptr = Arc::new(Mutex::new(self.sstables.clone()));
        let sstable_dir = self.sstable_dir.clone();
        let combined_table = thread::spawn(move || {
            let locked_sstables = sstables_ptr.lock();
            match locked_sstables {
                Ok(mut sstables) => compaction(&mut sstables, &sstable_dir),
                Err(e) => panic!("oops {}", e),
            }
        })
        .join()
        .unwrap();
        self.sstables = vec![combined_table];
    }

    /// Drain key-value pairs into an sstable.
    fn flush_memtable(&mut self) -> Result<()>{
        let mut sstable = create_sstable(self.sstables.len(), &self.sstable_dir);
        sstable.write(&self.memtable)?;
        self.sstables.push(sstable);
        self.memtable = HashMap::new();
        self.mem_size = 0;
        Ok(())
    }

    /// Set a key value pair in the store. 
    pub fn set(&mut self, k: &[u8], v: &[u8]) {
        self.mem_size += (k.len() + v.len()) as u64;
        if self.is_overflow() && self.memtable.is_empty() {
            panic!("Store size ({} bytes) should be greater than \
                    {} bytes (size of key-value pair being inserted)!", 
                    self.max_bytes,
                    self.mem_size);
        }
        if self.is_overflow() {
            debug!(
                "{}",
                format!(
                    "Size overflow, max-size={}, current-size={}",
                    self.max_bytes, self.mem_size
                )
            );

            self.flush_memtable();
        }
        self.memtable.insert(k.to_vec(), v.to_vec());
    }

    /// Get the value for a key stored previously
    pub fn get(&mut self, k: &[u8]) -> Option<Vec<u8>> {
        if let Some(v) = self.memtable.get(k) {
            if v == TOMBSTONE {
                return None;
            }
            return Some(v.to_vec());
        }
        parallel_search(&mut self.sstables, k.to_vec())
    }

    /// Remove a key value pair.
    pub fn delete(&mut self, k: &[u8]) {
        if self.memtable.remove(k).is_some() {
            return;
        };
        if parallel_search(&mut self.sstables, k.to_vec()).is_some() {
            self.memtable.insert(k.to_vec(), TOMBSTONE.to_vec());
        }
    }

    /// Get the current size of memtable.
    pub fn size(&self) -> u64 {
        self.mem_size
    }
}

/// Parallel search SSTables.
/// 
/// sstables=Vec<SSTables> is ordered such that the most recent table is at the end.
/// 1. We partition sstables so that multiple threads can search them in parallel.
/// 2. We use a channel to collect results from each thread.
fn parallel_search(sstables: &mut Vec<SSTable>, k: Vec<u8>) -> Option<Vec<u8>> {
    let n_sstables = sstables.len();
    let n_threads = std::cmp::min(sstables.len(), 10);
    let chunk_size = (n_sstables + n_threads - 1) / n_threads;
    let sstables = Arc::new(sstables.clone());
    let key = Arc::new(k);
    let result: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
    let mut handles = vec![];
    let last_index: Arc<Mutex<Option<usize>>> = Arc::new(Mutex::new(None));    

    for i in 0..n_threads {
        let sstables = sstables.clone();
        let key = key.clone();
        let result = result.clone();
        let last_index = last_index.clone();

        let start = i * chunk_size;
        let end = std::cmp::min(start + chunk_size, n_sstables);

        let handle = thread::spawn(move || {
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

fn create_sstable(n_sstables: usize, sstable_dir: &Path) -> SSTable {
    let uuid = Uuid::new_v4();
    let idx = n_sstables + 1;
    let slug = format!("{}-{}.{}", idx, uuid, RKV);
    let dirname = sstable_dir.join(RKV).join("data");
    create_dir_all(dirname.clone()).unwrap();
    let filename = dirname.join(slug);
    SSTable::new(filename, true, true, true).unwrap()
}

fn compaction(sstables: &mut Vec<SSTable>, sstable_dir: &Path) -> SSTable {
    let mut store: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let n_sstables = sstables.len();
    for sstable in &mut *sstables {
        if let Ok(hashmap) = sstable.as_hashmap() {
            store.extend(hashmap)
        }
    }
    let mut keys: Vec<Vec<u8>> = store.clone().into_keys().collect();
    keys.sort();

    let mut sstable = create_sstable(n_sstables, sstable_dir);
    if let Err(e) = sstable.write(&store) {
        panic!("Failed to write to sstable because {}", e);
    }

    for i_sstable in sstables {
        i_sstable.delete();
    }

    sstable
}
