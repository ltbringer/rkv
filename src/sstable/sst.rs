use crate::sstable::constants::{KEY_WORD, TOMBSTONE, VALUE_WORD, WORD};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use log::error;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::{remove_file, File, OpenOptions};
use std::io::{Read, Result, Seek, SeekFrom, Write};
use std::path::PathBuf;

#[derive(Clone)]
pub struct SSTable {
    filename: PathBuf,
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
            filename,
            read,
            write,
            create,
        })
    }

    pub fn delete(&self) {
        let filename = self.filename.clone();
        let display_name = filename.as_path().display().to_string();
        if let Err(e) = remove_file(filename) {
            error!("Failed deleting file {} {}", display_name, e);
        }
    }

    fn open(&self, filename: &PathBuf) -> Result<File> {
        OpenOptions::new()
            .read(self.read)
            .write(self.write)
            .create(self.create)
            .open(filename)
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
        let index_filename = self.filename.with_extension("index");
        let mut data_file = self.open(&self.filename)?;
        let mut index_file = self.open(&index_filename)?;
        data_file.seek(SeekFrom::End(0))?;
        index_file.seek(SeekFrom::End(0))?;

        let mut sorted_hashmap: Vec<(&Vec<u8>, &Vec<u8>)> = hashmap.iter().collect();
        sorted_hashmap.sort_by(|a, b| {
            let a_key = a.0;
            let other_key = b.0;
            a_key.cmp(other_key)
        });

        for (key, value) in sorted_hashmap {
            let mut buf = vec![];
            let seek_pos = data_file.stream_position()?;
            index_file.write_u64::<LittleEndian>(seek_pos)?;
            self.set_key(&mut buf, key.len(), key)?;
            self.set_value(&mut buf, value.len(), value)?;
            data_file.write_all(&buf)?;
        }

        Ok(())
    }

    fn set_key(&self, buf: &mut Vec<u8>, key_len: usize, key: &[u8]) -> Result<()> {
        buf.write_u16::<LittleEndian>(key_len as u16)?;
        buf.write_all(key)
    }

    fn set_value(&self, buf: &mut Vec<u8>, value_len: usize, value: &[u8]) -> Result<()> {
        buf.write_u32::<LittleEndian>(value_len as u32)?;
        buf.write_all(value)
    }

    fn get_key_size(&self, buf: &[u8], i: usize) -> usize {
        u16::from_le_bytes(buf[i..i + KEY_WORD].try_into().unwrap()) as usize
    }

    fn get_value_size(&self, buf: &[u8], i: usize) -> usize {
        u32::from_le_bytes(buf[i..i + VALUE_WORD].try_into().unwrap()) as usize
    }

    pub fn as_hashmap(&mut self) -> Result<HashMap<Vec<u8>, Vec<u8>>> {
        let mut file = self.open(&self.filename)?;
        file.seek(SeekFrom::Start(0))?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut i: usize = 0;
        let mut hashmap: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

        while i < buf.len() {
            let key_len = self.get_key_size(&buf, i);
            i += KEY_WORD;

            let key_ = &buf[i..i + key_len];
            i += key_len;

            let value_len = self.get_value_size(&buf, i);
            i += VALUE_WORD;

            let value_ = &buf[i..i + value_len];
            i += value_len;

            if value_ != TOMBSTONE {
                hashmap.insert(key_.to_vec(), value_.to_vec());
            }
        }
        Ok(hashmap)
    }

    fn key_at(&self, pos: u64, index_file: &mut File, data_file: &mut File) -> Result<Vec<u8>> {
        index_file.seek(SeekFrom::Start(pos * WORD as u64))?;
        let data_mid = index_file.read_u64::<LittleEndian>()?;
        data_file.seek(SeekFrom::Start(data_mid))?;

        let key_len = data_file.read_u16::<LittleEndian>()?;
        let mut key_buf = vec![0; key_len as usize];

        data_file.read_exact(key_buf.as_mut_slice())?;
        Ok(key_buf)
    }

    fn get_value(&self, data_file: &mut File) -> Result<Vec<u8>> {
        let value_len = data_file.read_u32::<LittleEndian>()?;
        let mut value_buf = vec![0; value_len as usize];
        data_file.read_exact(value_buf.as_mut_slice())?;
        Ok(value_buf)
    }

    pub fn search(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut data_file = self.open(&self.filename)?;
        let mut index_file = self.open(&self.filename.with_extension("index"))?;
        let mut start = index_file.seek(SeekFrom::Start(0))?;
        let mut end = index_file.seek(SeekFrom::End(0))? / WORD as u64;

        while start < end {
            let mid = start + (end - start) / 2;
            let current_key = self.key_at(mid, &mut index_file, &mut data_file)?;

            match key.cmp(&current_key) {
                Ordering::Less => {
                    end = mid;
                }
                Ordering::Equal => {
                    let value = self.get_value(&mut data_file)?;
                    if value != TOMBSTONE {
                        return Ok(Some(value.to_vec()));
                    }
                    if mid + 1 < end {
                        let next_key = self.key_at(mid + 1, &mut index_file, &mut data_file)?;
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
