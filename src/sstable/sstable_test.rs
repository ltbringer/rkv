#[cfg(test)]
mod test {
    use crate::sstable::sst::SSTable;
    use std::{
        collections::HashMap,
        panic::{self, AssertUnwindSafe},
    };
    use tempfile::TempDir;

    #[test]
    fn test_sstable_write() {
        let result = panic::catch_unwind(AssertUnwindSafe(|| {
            let temp_dir = match TempDir::new() {
                Ok(dir) => dir,
                Err(_) => panic!("Failed creating tempdir."),
            };
            let filename = temp_dir.path().join("test.sstable");
            let mut sstable = match SSTable::new(filename, true, true, true) {
                Ok(sst) => sst,
                Err(_) => panic!("Failed creating sstable."),
            };
            let key = b"test_key";
            let value = b"test_value";
            let mut store = HashMap::new();
            store.insert(key.to_vec(), value.to_vec());
            match sstable.write(&store) {
                Ok(_) => (),
                Err(_) => panic!("Failed write to sstable."),
            };
            let value_read = match sstable.search(key) {
                Ok(Some(v)) => v,
                Err(e) => panic!("{}", e),
                _ => panic!("Failed to read value."),
            };
            assert_eq!(value, value_read.as_slice());
            drop(sstable);
        }));
        assert!(result.is_ok());
    }
}
