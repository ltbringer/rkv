use log::{debug, error};
use std::{collections::HashMap, path::PathBuf};
use std::fs::create_dir_all;

use uuid::Uuid;
use glob::glob;

use crate::sstable::sstable::SSTable;
use crate::sstable::constants::{TOMBSTONE, RKV};

pub struct KVStore {
    memtable: HashMap<Vec<u8>, Vec<u8>>,
    mem_size: u64,
    max_bytes: u64,
    sstables: Vec<SSTable>,
    sstable_dir: PathBuf
}

impl KVStore {
    pub fn new(size: u64, sstable_dir: PathBuf) -> Self {
        KVStore {
            memtable: HashMap::new(),
            mem_size: 0,
            max_bytes: size,
            sstables: vec![],
            sstable_dir
        }
    }

    fn is_overflow(&self) -> bool {
        self.max_bytes < self.mem_size
    }

    fn discover_sstables(&mut self) {
        let mut sstables: Vec<SSTable> = vec![];
        let sstable_dir = self.sstable_dir.join(RKV).join("dat");
        let sstable_dir_str = sstable_dir.as_path().display().to_string();
        let glob_pattern = format!("{}/*.sstable", sstable_dir_str);
        for entry in glob(&glob_pattern).expect("Failed to read glob pattern") {
            match entry {
                Ok(path) => {
                    match SSTable::new(path.clone()) {
                        Ok(sstable) => sstables.push(sstable),
                        Err(e) => error!("Failed to read sstable {} because {}", path.as_path().display(), e)
                    }
                },
                Err(e) => println!("{:?}", e),
            }
        }
        self.sstables = sstables;
    }

    fn create_sstable(&mut self) -> SSTable {
        let uuid = Uuid::new_v4();
        let idx = self.sstables.len() + 1;
        let slug = format!("{}-{}.{}", idx, uuid, RKV);
        let dirname = self.sstable_dir
            .join(RKV)
            .join("data");
        create_dir_all(dirname.clone()).unwrap();
        let filename = dirname.join(slug);
        SSTable::new(filename).unwrap()
    }

    fn compaction(&mut self) {
        let mut store: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        for sstable in &mut self.sstables {
            if let Ok(hashmap) = sstable.as_hashmap() {
                store.extend(hashmap)
            }
        }
        let mut keys: Vec<Vec<u8>> = store.clone().into_keys().collect();
        keys.sort();

        let mut sstable = self.create_sstable();
        keys.iter()
            .filter_map(|k| store.get(k).map(|v| (k, v)))
            .try_for_each(|(k, v)| sstable.write(k, v))
            .unwrap_or_else(|e| error!("{}", e));
        self.sstables = vec![sstable];

        for i_sstable in &self.sstables {
            i_sstable.delete();
        }
    }

    fn flush_memtable(&mut self) {
        let mut sstable = self.create_sstable();
        let mut keys: Vec<Vec<u8>> = self.memtable.clone().into_keys().collect();
        keys.sort();

        for k in keys {
            if let Some(v) = self.memtable.get(&k) {
                if let Err(e) = sstable.write(&k, &v) {
                    error!("{}", e);
                }
            };
        }
        self.sstables.push(sstable);

        self.memtable = HashMap::new();
        self.mem_size = 0;
    }

    pub fn set(&mut self, k: &[u8], v: &[u8]) {
        self.mem_size += (k.len() + v.len()) as u64;
        if self.is_overflow() && self.memtable.is_empty() {
            panic!("Store size ({} bytes) should be greater than {} bytes (size of key-value pair being inserted)!", self.max_bytes, self.mem_size);
        }
        if self.is_overflow() {
            debug!("{}", format!(
                "Size overflow, max-size={}, current-size={}", 
                self.max_bytes,
                self.mem_size
            ));

            self.flush_memtable();
        }
        self.memtable.insert(k.to_vec(), v.to_vec());
    }

    fn get_from_sstable(&mut self, k: &[u8]) -> Option<Vec<u8>> {
        for sstable in &mut self.sstables {
            let value = match sstable.scan(k) {
                Ok(v) => v,
                _ => None
            };
            if let Some(v) = value {
                return if v == TOMBSTONE { None } else { Some(v) }
            }
        }
        return None
    }

    pub fn get(&mut self, k: &[u8]) -> Option<Vec<u8>> {
        if let Some(v) = self.memtable.get(k) {
            if v == TOMBSTONE {
                return None
            }
            return Some(v.to_vec());
        }
        return self.get_from_sstable(k)
    }

    pub fn delete(&mut self, k: &[u8]) {
        if let Some(_) = self.memtable.remove(k) {
            return ()
        };
        if let Some(_) = self.get_from_sstable(k) {
            self.memtable.insert(k.to_vec(), TOMBSTONE.to_vec());
        }
    }

    pub fn size(&self) -> u64 {
        self.mem_size
    }
}