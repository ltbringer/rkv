use crate::sstable::constants::WORD;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::fs::File;
use std::io::{Read, Result, Seek, SeekFrom, Write};

pub fn set_key(buf: &mut Vec<u8>, key_len: usize, key: &[u8]) -> Result<()> {
    buf.write_u16::<LittleEndian>(key_len as u16)?;
    buf.write_all(key)
}

pub fn set_value(buf: &mut Vec<u8>, value_len: usize, value: &[u8]) -> Result<()> {
    buf.write_u32::<LittleEndian>(value_len as u32)?;
    buf.write_all(value)
}

pub fn key_value_at(pos: u64, index: &mut File, data: &mut File) -> Result<(Vec<u8>, Vec<u8>)> {
    index.seek(SeekFrom::Start(pos * WORD as u64))?;
    let data_mid = index.read_u64::<LittleEndian>()?;
    data.seek(SeekFrom::Start(data_mid))?;
    let key_len = data.read_u16::<LittleEndian>()?;
    let mut key_buf = vec![0; key_len as usize];
    data.read_exact(key_buf.as_mut_slice())?;

    let value_len = data.read_u32::<LittleEndian>()?;
    let mut value_buf = vec![0; value_len as usize];
    data.read_exact(value_buf.as_mut_slice())?;
    Ok((key_buf, value_buf))
}

pub fn set_index(index_file: &mut File, index: u64) -> Result<()> {
    index_file.write_u64::<LittleEndian>(index)
}
