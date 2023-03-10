use std::collections::HashMap;
use std::fs::{File, OpenOptions, remove_file};
use std::io::{self, Write, Seek, SeekFrom, Read};
use std::path::PathBuf;
use log::error;
use byteorder::{LittleEndian, WriteBytesExt};
use super::constants::{WORD, TOMBSTONE};

pub struct SSTable {
    filename: PathBuf,
    file: File
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
    pub fn new(filename: PathBuf) -> io::Result<SSTable> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(filename.clone())?;

        Ok(SSTable { filename, file })
    }

    pub fn delete(&self) {
        let filename = self.filename.clone();
        let display_name = filename.clone().as_path().display().to_string();
        if let Err(e) = remove_file(filename) {
            error!("Failed deleting file {} {}", display_name, e);
        }
    }

    /**
     * Write a key-value pair to an SSTable.
     * 
     * - Both key length and value length are exactly 8 bytes long because
     *   we are using u64 for both.
     * - Writing the key (and value) length helps us at the time of reading.
     *   or else we would resort to delimiters and handle cases when the
     *   delimiter character is also an input.
     */
    pub fn write(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        let key_len = key.len() as u64;
        let value_len = value.len() as u64;
        let mut buf = vec![];
        buf.write_u64::<LittleEndian>(key_len)?;
        buf.write_all(key)?;
        buf.write_u64::<LittleEndian>(value_len)?;
        buf.write_all(value)?;
        self.file.write_all(&buf)?;
        Ok(())
    }

    fn get_kv_len_u64(&self, buf: &Vec<u8>, i: usize) -> usize {
        u64::from_le_bytes(buf[i..i+8].try_into().unwrap()) as usize
    }

    pub fn as_hashmap(&mut self) -> io::Result<HashMap<Vec<u8>, Vec<u8>>> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut buf = Vec::new();
        self.file.read_to_end(&mut buf)?;
        let mut i: usize = 0;
        let mut hashmap: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

        while i < buf.len() {
            let key_len = self.get_kv_len_u64(&buf, i);
            i += WORD;

            let key_ = &buf[i..i+key_len];
            i += key_len;

            let value_len = self.get_kv_len_u64(&buf, i);
            i += WORD;

            let value_ = &buf[i..i+value_len];
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
    pub fn scan(&mut self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut buf = Vec::new();
        self.file.read_to_end(&mut buf)?;
        let mut i: usize = 0;

        while i < buf.len() {
            let key_len = self.get_kv_len_u64(&buf, i);
            i += WORD;

            let key_ = &buf[i..i+key_len];
            i += key_len;

            let value_len = self.get_kv_len_u64(&buf, i);
            i += WORD;

            let value_ = &buf[i..i+value_len];
            i += value_len;

            let is_tombstone = value_ == TOMBSTONE;

            if key_ == key && !is_tombstone {
                return Ok(Some(value_.to_vec()))
            }
        }

        return Ok(None)
    }
}