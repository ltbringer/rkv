use crate::sstable::constants::{KEY_WORD, RKV, TOMBSTONE, VALUE_WORD, WORD};
use crate::utils::futil;
use log::error;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::create_dir_all;
use std::fs::{remove_file, File, OpenOptions};
use std::io::{Read, Result, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use uuid::Uuid;

#[derive(Clone)]
pub struct SSTable {
    dat: PathBuf,
    index: PathBuf,
    read: bool,
    write: bool,
    create: bool,
}

impl SSTable {
    /**
     * The anatomy of an SSTable:
     *
     * |0|0|0|0|0|0|0|9|t|e|s|t|_|m|o|d|e|0|0|0|0|0|0|0|7|1|2|3|4|5|6|7|
     * |<- Key length->|<-key contents-->|<- Val length->|<-- Value -->|
     * |0|0|0|0|0|0|0|4|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_
     * |<- Key length->| ...
     *
     * Notice: the key `test_mode` is 9 characters long. That's what the
     * `Key length` is trying to specify. The same explains the following
     * `Val length`.
     */
    pub fn new(filename: PathBuf, read: bool, write: bool, create: bool) -> Result<SSTable> {
        Ok(SSTable {
            dat: filename.clone(),
            index: filename.with_extension("index"),
            read,
            write,
            create,
        })
    }

    pub fn delete(&self) {
        let filename = self.dat.clone();
        let display_name = filename.as_path().display().to_string();
        if let Err(e) = remove_file(filename) {
            error!("Failed deleting file {} {}", display_name, e);
        }
    }

    fn open(&self) -> Result<(File, File)> {
        let dat = OpenOptions::new()
            .read(self.read)
            .write(self.write)
            .create(self.create)
            .open(self.dat.clone())?;

        let index = OpenOptions::new()
            .read(self.read)
            .write(self.write)
            .create(self.create)
            .open(self.index.clone())?;

        Ok((dat, index))
    }

    /**
     * Write a key-value pair to an SSTable.
     *
     * - Both key length and value length are 8 bytes long because
     *   we are using u64 for keys and values.
     * - Writing the key (and value) length helps us at the time of reading.
     *   or else we would resort to delimiters and handle cases when the
     *   delimiter character is also an input.
     */
    pub fn write(&mut self, hashmap: &HashMap<Vec<u8>, Vec<u8>>) -> Result<()> {
        let (mut data, mut index) = self.open()?;
        data.seek(SeekFrom::End(0))?;
        index.seek(SeekFrom::End(0))?;

        let mut sorted_hashmap: Vec<(&Vec<u8>, &Vec<u8>)> = hashmap.iter().collect();
        sorted_hashmap.sort_by(|a, b| {
            let a_key = a.0;
            let other_key = b.0;
            a_key.cmp(other_key)
        });

        for (key, value) in sorted_hashmap {
            let mut buf = vec![];
            let seek_pos = data.stream_position()?;
            futil::set_index(&mut index, seek_pos)?;
            futil::set_key(&mut buf, key.len(), key)?;
            futil::set_value(&mut buf, value.len(), value)?;
            data.write_all(&buf)?;
        }

        Ok(())
    }

    pub fn as_hashmap(&mut self) -> Result<HashMap<Vec<u8>, Vec<u8>>> {
        let (mut data, _) = self.open()?;
        data.seek(SeekFrom::Start(0))?;
        let mut buf = Vec::new();
        data.read_to_end(&mut buf)?;
        let mut i: usize = 0;
        let mut hashmap: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

        while i < buf.len() {
            let key_len = futil::get_key_size(&buf, i);
            i += KEY_WORD;

            let key_ = &buf[i..i + key_len];
            i += key_len;

            let value_len = futil::get_value_size(&buf, i);
            i += VALUE_WORD;

            let value_ = &buf[i..i + value_len];
            i += value_len;

            if value_ != TOMBSTONE {
                hashmap.insert(key_.to_vec(), value_.to_vec());
            }
        }
        Ok(hashmap)
    }

    /**
     * Search for the latest value of a given key in an SSTable.
     */
    pub fn search(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let (mut data, mut index) = self.open()?;
        let mut start = index.seek(SeekFrom::Start(0))?;
        let mut end = index.seek(SeekFrom::End(0))? / WORD as u64;

        while start < end {
            let mid = start + (end - start) / 2;
            let (current_key, value) = futil::key_value_at(mid, &mut index, &mut data)?;

            match key.cmp(&current_key) {
                Ordering::Less => {
                    end = mid;
                }
                Ordering::Equal => {
                    if value != TOMBSTONE {
                        return Ok(Some(value.to_vec()));
                    }
                    if mid + 1 < end {
                        let (next_key, _) = futil::key_value_at(mid + 1, &mut index, &mut data)?;
                        if next_key != key {
                            return Ok(Some(value));
                        } else {
                            start = mid + 1;
                        }
                    }
                }
                Ordering::Greater => {
                    start = mid + 1;
                }
            }
        }

        Ok(None)
    }
}

pub fn create_sstable(n_sstables: usize, sstable_dir: &Path) -> SSTable {
    let uuid = Uuid::new_v4();
    let idx = n_sstables + 1;
    let slug = format!("{}-{}.{}", idx, uuid, RKV);
    let dirname = sstable_dir.join(RKV).join("data");
    create_dir_all(dirname.clone()).unwrap();
    let filename = dirname.join(slug);
    SSTable::new(filename, true, true, true).unwrap()
}

pub fn merge(
    sstable_left: SSTable,
    sstable_right: SSTable,
    mut merged_sstable: SSTable,
) -> Vec<SSTable> {
    let mut map: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let (mut i, mut j) = (0, 0);

    let (mut l_data, mut l_index) = sstable_left.open().unwrap();
    let l_end = l_index.seek(SeekFrom::End(0)).unwrap();

    let (mut r_data, mut r_index) = sstable_right.open().unwrap();
    let r_end = r_index.seek(SeekFrom::End(0)).unwrap();

    while i < l_end && j < r_end {
        let (l_key, l_value) = futil::key_value_at(i, &mut l_index, &mut l_data).unwrap();
        let (r_key, r_value) = futil::key_value_at(j, &mut r_index, &mut r_data).unwrap();

        if l_key < r_key {
            map.insert(l_key, l_value);
            i += WORD as u64;
        } else {
            map.insert(r_key, r_value);
            j += WORD as u64;
        }

        if map.len() > 100_000 {
            merged_sstable.write(&map).unwrap();
            map.clear();
        }
    }

    vec![merged_sstable]
}
