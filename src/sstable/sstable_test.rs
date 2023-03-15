#[cfg(test)]
mod sstable_test {
    use std::panic::{self, AssertUnwindSafe};
    use tempfile::TempDir;
    use crate::sstable::sstable::SSTable;

    #[test]
    fn test_sstable_write() {
        let result = panic::catch_unwind(AssertUnwindSafe(|| {
            let temp_dir = match TempDir::new() {
                Ok(dir) => dir,
                Err(_) => panic!("Failed creating tempdir.")
            };
            let filename = temp_dir.path().join("test.sstable");
            let mut sstable = match SSTable::new(filename.clone(), true, true, true) {
                Ok(sst) => sst,
                Err(_) => panic!("Failed creating sstable.")
            };
            let key = b"test_key";
            let value = b"test_value";
            match sstable.write(key, value) {
                Ok(_) => (),
                Err(_) => panic!("Failed write to sstable.")
            };
            let value_read = match sstable.scan(key) {
                Ok(Some(v)) => v,
                Err(e) => panic!("{}", e),
                _ => panic!("Failed to read value.")
            };
            assert_eq!(value, value_read.as_slice());
            drop(sstable);
        }));
        assert!(result.is_ok());
    }
}