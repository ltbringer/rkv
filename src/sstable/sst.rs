use crate::sstable::constants::{RKV, TOMBSTONE};
use crate::utils::futil;
use log::error;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs::create_dir_all;
use std::fs::{remove_file, File, OpenOptions};
use std::io::{Result, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use uuid::Uuid;

#[derive(Clone)]
pub struct SSTable {
    dat: PathBuf,
    index: PathBuf,
    level: u16,
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
    pub fn new(
        filename: PathBuf,
        level: u16,
        read: bool,
        write: bool,
        create: bool,
    ) -> Result<SSTable> {
        Ok(SSTable {
            dat: filename.clone(),
            index: filename.with_extension("index"),
            level,
            read,
            write,
            create,
        })
    }

    pub fn delete(&self) {
        let filename = self.dat.clone();
        let display_name = filename.as_path().display().to_string();
        if let Err(e) = remove_file(filename) {
            error!(
                "Failed deleting the data file for {} because {}",
                display_name, e
            );
        }
        if let Err(e) = remove_file(self.index.clone()) {
            error!(
                "Failed deleting the data file for {} because {}",
                display_name, e
            );
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

    pub fn get_level(&self) -> u16 {
        self.level
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
    pub fn write(&mut self, map: &BTreeMap<Vec<u8>, Vec<u8>>) -> Result<()> {
        let (mut data, mut index) = self.open()?;
        data.seek(SeekFrom::End(0))?;
        index.seek(SeekFrom::End(0))?;

        for (key, value) in map {
            let mut buf = vec![];
            let seek_pos = data.stream_position()?;
            futil::set_index(&mut index, seek_pos)?;
            futil::set_key(&mut buf, key.len(), key)?;
            futil::set_value(&mut buf, value.len(), value)?;
            data.write_all(&buf)?;
        }

        Ok(())
    }

    /**
     * Search for the latest value of a given key in an SSTable.
     */
    pub fn search(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let (mut data, mut index) = self.open()?;
        let (mut start, mut end) = futil::get_index_range(&mut index);
        while start < end {
            let mid = start + (end - start) / 2;
            let (current_key, value) = futil::key_value_at(mid, &mut index, &mut data)?;

            match key.cmp(&current_key) {
                Ordering::Less => {
                    end = mid;
                }
                Ordering::Equal => {
                    if value == TOMBSTONE {
                        return Ok(None);
                    }
                    if mid + 1 < end {
                        let (next_key, _) = futil::key_value_at(mid + 1, &mut index, &mut data)?;
                        if next_key != key {
                            return Ok(Some(value));
                        } else {
                            start = mid + 1;
                        }
                    } else {
                        return Ok(Some(value));
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

pub fn create_sstable(level: u16, name: String, sstable_dir: &Path) -> SSTable {
    let uuid = Uuid::new_v4();
    let this_level = level + 1;
    let slug = format!("{}-{}.{}", this_level, uuid, RKV);
    let dirname = sstable_dir.join(name).join(RKV).join("data");
    create_dir_all(dirname.clone()).unwrap();
    let filename = dirname.join(slug);
    SSTable::new(filename, this_level, true, true, true).unwrap()
}

fn merge_two(
    sstable_old: &SSTable,
    sstable_new: &SSTable,
    merged_sstable: &mut SSTable,
    log_size: usize,
) -> Result<()> {
    let mut map: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let (mut i, mut j) = (0, 0);

    let (mut o_data, mut o_index) = sstable_old.open()?;
    let (_, o_end) = futil::get_index_range(&mut o_index);

    let (mut n_data, mut n_index) = sstable_new.open()?;
    let (_, n_end) = futil::get_index_range(&mut n_index);

    while i < o_end && j < n_end {
        let (o_key, o_value) = futil::key_value_at(i, &mut o_index, &mut o_data)?;
        let (n_key, n_value) = futil::key_value_at(j, &mut n_index, &mut n_data)?;

        match o_key.cmp(&n_key) {
            Ordering::Less => {
                map.insert(o_key, o_value);
                i += 1;
            }
            Ordering::Equal => {
                map.insert(n_key, n_value);
                i += 1;
                j += 1;
            }
            Ordering::Greater => {
                map.insert(n_key, n_value);
                j += 1;
            }
        }

        if map.len() > log_size {
            merged_sstable.write(&map)?;
            map.clear();
        }
    }

    while i < o_end {
        let (o_key, o_value) = futil::key_value_at(i, &mut o_index, &mut o_data)?;
        map.insert(o_key, o_value);
        i += 1;
        if map.len() > log_size {
            merged_sstable.write(&map)?;
            map.clear();
        }
    }

    while j < n_end {
        let (n_key, n_value) = futil::key_value_at(j, &mut n_index, &mut n_data)?;
        map.insert(n_key, n_value);
        j += 1;
        if map.len() > log_size {
            merged_sstable.write(&map)?;
            map.clear();
        }
    }

    merged_sstable.write(&map)?;
    Ok(())
}

fn merge_sstables(
    sstables: Vec<SSTable>,
    name: String,
    sstable_dir: &Path,
    level: u16,
) -> Vec<SSTable> {
    let mut merged_sstables = Vec::new();
    for pair in sstables.chunks(2) {
        match pair.len() {
            1 => {
                let sstable = pair[0].clone();
                merged_sstables.push(sstable);
            }
            2 => {
                let sstable_old = pair[0].clone();
                let sstable_new = pair[1].clone();
                let mut merged_sstable = create_sstable(level, name.clone(), sstable_dir);
                merge_two(&sstable_old, &sstable_new, &mut merged_sstable, 1000).unwrap();
                sstable_old.delete();
                sstable_new.delete();
                merged_sstables.push(merged_sstable);
            }
            _ => unreachable!("SSTable length should be 1 or 2"),
        }
    }
    merged_sstables
}

pub fn sstable_compaction(
    shared_sstables: Arc<Mutex<Vec<SSTable>>>,
    name: String,
    level: u16,
    sstable_dir: &Path,
) -> Arc<Mutex<Vec<SSTable>>> {
    let sstable_dir = Arc::new(sstable_dir.to_path_buf());
    let this_level = Arc::new(Mutex::new(level));
    let merged_sstables = thread::spawn(move || {
        let mut sstables = shared_sstables.lock().unwrap();
        while sstables.len() > 1 {
            match this_level.lock() {
                Ok(mut level) => {
                    *level += 1;
                    *sstables =
                        merge_sstables(sstables.to_vec(), name.clone(), &sstable_dir, *level);
                }
                Err(poisoned) => panic!("Poisoned lock: {:?}", poisoned),
            }
        }
        sstables.len();
        sstables.to_vec()
    })
    .join()
    .unwrap();

    Arc::new(Mutex::new(merged_sstables))
}

#[cfg(test)]
mod test {
    use super::*;
    use std::io::Read;
    use std::panic::{self, AssertUnwindSafe};
    use tempfile::TempDir;

    #[test]
    fn test_merge_n_sstable_large() {
        let result = panic::catch_unwind(AssertUnwindSafe(|| {
            let temp_dir = TempDir::new().unwrap();
            let sstable_dir = temp_dir.path();
            let name = "test_merge_n_sstable_large".to_owned();
            let mut sstable_o = create_sstable(0, name.clone(), sstable_dir);
            let mut sstable_n = create_sstable(1, name.clone(), sstable_dir);
            let mut sstable_m = create_sstable(2, name, sstable_dir);
            let (mut dat, _) = sstable_m.open().unwrap();
            let mut map = BTreeMap::new();
            map.insert(b"key1".to_vec(), b"value1".to_vec());
            map.insert(b"key5".to_vec(), b"value2".to_vec());
            map.insert(b"key3".to_vec(), b"value3".to_vec());
            map.insert(b"key10".to_vec(), b"value6".to_vec());
            sstable_o.write(&map).unwrap();
            map.clear();

            map.insert(b"key2".to_vec(), b"value4".to_vec());
            map.insert(b"key3".to_vec(), b"value5".to_vec());
            map.insert(b"key4".to_vec(), b"value2".to_vec());
            map.insert(b"key10".to_vec(), b"value9".to_vec());
            map.insert(b"key11".to_vec(), b"value7".to_vec());
            map.insert(b"key60".to_vec(), b"value7".to_vec());
            sstable_n.write(&map).unwrap();

            merge_two(&sstable_o, &sstable_n, &mut sstable_m, 0).unwrap();

            let buf = &mut Vec::new();
            dat.rewind().unwrap();
            dat.read_to_end(buf).unwrap();
            let string = String::from_utf8(buf.to_vec()).unwrap();
            assert_eq!(
                string,
                "\u{4}\0key1\u{6}\0\0\0value1\
                \u{5}\0key10\u{6}\0\0\0value9\
                \u{5}\0key11\u{6}\0\0\0value7\
                \u{4}\0key2\u{6}\0\0\0value4\
                \u{4}\0key3\u{6}\0\0\0value5\
                \u{4}\0key4\u{6}\0\0\0value2\
                \u{4}\0key5\u{6}\0\0\0value2\
                \u{5}\0key60\u{6}\0\0\0value7"
            );
            drop(temp_dir);
        }));
        assert!(result.is_ok());
    }

    #[test]
    fn test_merge_o_sstable_large() {
        let result = panic::catch_unwind(AssertUnwindSafe(|| {
            let temp_dir = TempDir::new().unwrap();
            let sstable_dir = temp_dir.path();
            let name = "test_merge_o_sstable_large".to_owned();
            let mut sstable_o = create_sstable(0, name.clone(), sstable_dir);
            let mut sstable_n = create_sstable(1, name.clone(), sstable_dir);
            let mut sstable_m = create_sstable(2, name, sstable_dir);
            let (mut dat, _) = sstable_m.open().unwrap();
            let mut map = BTreeMap::new();
            map.insert(b"key2".to_vec(), b"value4".to_vec());
            map.insert(b"key3".to_vec(), b"value5".to_vec());
            map.insert(b"key4".to_vec(), b"value2".to_vec());
            map.insert(b"key10".to_vec(), b"value9".to_vec());
            map.insert(b"key11".to_vec(), b"value7".to_vec());
            map.insert(b"key60".to_vec(), b"value7".to_vec());
            sstable_o.write(&map).unwrap();
            map.clear();

            map.insert(b"key1".to_vec(), b"value1".to_vec());
            map.insert(b"key5".to_vec(), b"value2".to_vec());
            map.insert(b"key3".to_vec(), b"value3".to_vec());
            map.insert(b"key10".to_vec(), b"value6".to_vec());
            sstable_n.write(&map).unwrap();

            merge_two(&sstable_o, &sstable_n, &mut sstable_m, 0).unwrap();

            let buf = &mut Vec::new();
            dat.rewind().unwrap();
            dat.read_to_end(buf).unwrap();
            let string = String::from_utf8(buf.to_vec()).unwrap();
            assert_eq!(
                string,
                "\u{4}\0key1\u{6}\0\0\0value1\
                \u{5}\0key10\u{6}\0\0\0value6\
                \u{5}\0key11\u{6}\0\0\0value7\
                \u{4}\0key2\u{6}\0\0\0value4\
                \u{4}\0key3\u{6}\0\0\0value3\
                \u{4}\0key4\u{6}\0\0\0value2\
                \u{4}\0key5\u{6}\0\0\0value2\
                \u{5}\0key60\u{6}\0\0\0value7"
            );
            drop(temp_dir);
        }));
        assert!(result.is_ok());
    }
}
