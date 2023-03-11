use log::{debug, error};
use std::{collections::HashMap, path::PathBuf};
use uuid::Uuid;

use crate::sstable::sstable::SSTable;

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
        let sstable_dir = self.sstable_dir.join(RKVDIR).join("dat");
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

        let uuid = Uuid::new_v4();
        let path = uuid.to_string();
        let filename = self.sstable_dir.join(path);
        match SSTable::new(filename) {
            Ok(sstable) => Some(sstable),
            Err(e) => {
                error!("{}", e);
                None
            }
        }
    }

    fn flush_memtable(&mut self) {
        if let Some(mut sstable) = self.create_sstable() {
            let mut keys: Vec<Vec<u8>> = self.memtable.clone().into_keys().collect();
            keys.sort();

            for k in keys {
                if let Some(v) = self.memtable.get(&k) {
                    if let Err(e) = sstable.write(&k, &v) {
                        error!("{}", e);
                    }
                };
            }
            self.memtable = HashMap::new();
            self.mem_size = 0;
        }
    }

    pub fn set(&mut self, k: &[u8], v: &[u8]) {
        self.mem_size += (k.len() + v.len()) as u64;
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

    pub fn get(&mut self, k: &[u8]) -> Option<Vec<u8>> {
        if let Some(v) = self.memtable.get(k) {
            return Some(v.to_vec());
        }

        for sstable in &mut self.sstables {
            let value = match sstable.read(k) {
                Ok(v) => v,
                _ => None
            };
            if value.is_some() {
                return value
            }
        }
        return None
    }

    pub fn delete(&mut self, k: &[u8]) {
        self.memtable.remove(k);
    }

    pub fn size(&self) -> u64 {
        self.mem_size
    }
}