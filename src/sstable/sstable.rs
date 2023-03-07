use std::fs::{File, OpenOptions};
use std::io::{self, Write, Seek, SeekFrom, Read};
use std::path::PathBuf;
use byteorder::{LittleEndian, WriteBytesExt};
use super::constants::WORD;

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
