use log::{info, trace};
use std::collections::HashMap;

pub struct KVStore {
    data: HashMap<Vec<u8>, Vec<u8>>,
    mem_size: u64,
    max_bytes: u64
}

impl KVStore {
    pub fn new(size: Option<u64>) -> Self {
        let max_bytes = match size {
            Some(s) => s,
            None => 100
        };
        KVStore {
            data: HashMap::new(),
            mem_size: 0,
            max_bytes
        }
    }

    fn is_overflow(&self) -> bool {
        self.max_bytes < self.mem_size
    }

    pub fn set(&mut self, k: &[u8], v: &[u8]) {
        self.mem_size += (k.len() + v.len()) as u64;
        if self.is_overflow() {
            panic!("{}", format!("Size overflow, max-size={}, current-size={}", self.max_bytes, self.mem_size));
        }
        self.data.insert(k.to_vec(), v.to_vec());
    }

    pub fn get(&self, k: &[u8]) -> Option<&Vec<u8>> {
        self.data.get(k)
    }

    pub fn delete(&mut self, k: &[u8]) {
        self.data.remove(k);
    }

    pub fn size(&self) -> u64 {
        self.mem_size
    }
}