use std::fs::{File, OpenOptions};
use std::io::{self, Write, Seek, SeekFrom, Read};
use std::path::PathBuf;
use byteorder::{LittleEndian, WriteBytesExt};
use super::constants::WORD;

pub struct SSTable {
    filename: PathBuf,
    file: File
}
