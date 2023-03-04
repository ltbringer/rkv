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

    pub fn set(&mut self, k: &[u8], v: &[u8]) {
        self.data.insert(k.to_vec(), v.to_vec());
        self.mem_size += (k.len() + v.len()) as u64;
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