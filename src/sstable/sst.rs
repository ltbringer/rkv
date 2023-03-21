use crate::sstable::constants::{TOMBSTONE, WORD};
use byteorder::{LittleEndian, WriteBytesExt};
use log::error;
use std::collections::HashMap;
use std::fs::{remove_file, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
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
    pub fn new(filename: PathBuf, read: bool, write: bool, create: bool) -> io::Result<SSTable> {
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

    fn open(&self) -> io::Result<File> {
        OpenOptions::new()
            .read(self.read)
            .write(self.write)
            .create(self.create)
            .open(&self.filename)
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
        let key_len = key.len() as u64;
        let value_len = value.len() as u64;
        let mut buf = vec![];
            let seek_pos = data_file.seek(SeekFrom::Current(0))?;
            index_file.write_u64::<LittleEndian>(seek_pos)?;
        buf.write_u64::<LittleEndian>(key_len)?;
        buf.write_all(key)?;
        buf.write_u64::<LittleEndian>(value_len)?;
        buf.write_all(value)?;
            data_file.write_all(&buf)?;
        }

        Ok(())
    }

    fn get_kv_len_u64(&self, buf: &[u8], i: usize) -> usize {
        u64::from_le_bytes(buf[i..i + 8].try_into().unwrap()) as usize
    }

    pub fn as_hashmap(&mut self) -> io::Result<HashMap<Vec<u8>, Vec<u8>>> {
        let mut file = self.open()?;
        file.seek(SeekFrom::Start(0))?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut i: usize = 0;
        let mut hashmap: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

        while i < buf.len() {
            let key_len = self.get_kv_len_u64(&buf, i);
            i += WORD;

            let key_ = &buf[i..i + key_len];
            i += key_len;

            let value_len = self.get_kv_len_u64(&buf, i);
            i += WORD;

            let value_ = &buf[i..i + value_len];
            i += value_len;

            if value_ != TOMBSTONE {
                hashmap.insert(key_.to_vec(), value_.to_vec());
            }
        }
        Ok(hashmap)
    }

    /**
     * Read the value of a key from an SSTable.
     * If this file was opened for writing,
     * that would change the seek position to EOF,
     * Hence we explicitly change the position.
     */
    pub fn scan(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let mut file = self.open()?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut i: usize = 0;

        while i < buf.len() {
            let key_len = self.get_kv_len_u64(&buf, i);
            i += WORD;

            let key_ = &buf[i..i + key_len];
            i += key_len;

            let value_len = self.get_kv_len_u64(&buf, i);
            i += WORD;

            let value_ = &buf[i..i + value_len];
            i += value_len;

            let is_tombstone = value_ == TOMBSTONE;

            if key_ == key && !is_tombstone {
                return Ok(Some(value_.to_vec()));
            }
        }

        Ok(None)
    }
}
