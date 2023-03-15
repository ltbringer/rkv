#[cfg(test)]
mod test {
    use crate::store::lsm_store::KVStore;
    use std::panic::{self, AssertUnwindSafe};
    use tempfile::TempDir;

    #[test]
    fn test_add_item() {
        let key = b"life";
        let value = b"42";
        let result = panic::catch_unwind(AssertUnwindSafe(|| {
            let temp_dir = match TempDir::new() {
                Ok(dir) => dir,
                Err(_) => panic!("Failed creating tempdir."),
            };
            let mut store = KVStore::new(20, temp_dir.into_path());
            store.set(key, value);
            match store.get(b"life") {
                Some(v) => assert_eq!(v, value, "Expected value to be b'42'"),
                None => panic!("Expected value to be b'42'"),
            }
            drop(store);
        }));
        assert!(result.is_ok());
    }

    #[test]
    fn test_sstable_read() {
        let setup = [
            (b"key1", b"value100"),
            (b"key2", b"value200"),
            (b"key3", b"value300"),
            (b"key1", b"value121"),
            (b"key4", b"value400"),
            (b"key5", b"value500"),
            (b"key6", b"value600"),
            (b"key7", b"value700"),
        ];

        let result = panic::catch_unwind(AssertUnwindSafe(|| {
            let temp_dir = match TempDir::new() {
                Ok(dir) => dir,
                Err(_) => panic!("Failed creating tempdir."),
            };
            let mut store = KVStore::new(20, temp_dir.into_path());
            for (key, value) in setup {
                store.set(key, value);
            }

            match store.get(b"key4") {
                Some(v) => assert_eq!(v, b"value400", "Value mismatch"),
                None => panic!("Expected a value to be found'"),
            }

            match store.get(b"key1") {
                Some(v) => assert_eq!(v, b"value121", "Value mismatch"),
                None => panic!("Expected a value to be found'"),
            }
            drop(store);
        }));
        assert!(result.is_ok());
    }

    #[test]
    fn test_delete_key() {
        let setup = [
            (b"key1", b"value1"),
            (b"key2", b"value2"),
            (b"key3", b"value3"),
            (b"key4", b"value4"),
            (b"key5", b"value5"),
            (b"key6", b"value6"),
            (b"key7", b"value7"),
        ];

        let result = panic::catch_unwind(AssertUnwindSafe(|| {
            let temp_dir = match TempDir::new() {
                Ok(dir) => dir,
                Err(_) => panic!("Failed creating tempdir."),
            };
            let mut store = KVStore::new(20, temp_dir.into_path());
            for (key, value) in setup {
                store.set(key, value);
            }

            store.delete(b"key2");

            if let Some(v) = store.get(b"key2") {
                panic!("Unexpected value {:?} found", v);
            }
            drop(store);
        }));
        assert!(result.is_ok());
    }

    #[test]
    fn test_compaction() {
        let setup = [
            (b"key1", b"value1"),
            (b"key2", b"value2"),
            (b"key3", b"value3"),
            (b"key4", b"value4"),
            (b"key5", b"value5"),
            (b"key6", b"value6"),
            (b"key7", b"value7"),
        ];

        let result = panic::catch_unwind(AssertUnwindSafe(|| {
            let temp_dir = match TempDir::new() {
                Ok(dir) => dir,
                Err(_) => panic!("Failed creating tempdir."),
            };
            let mut store = KVStore::new(10, temp_dir.into_path());
            for (key, value) in setup {
                store.set(key, value);
            }

            store.compaction();

            match store.get(b"key1") {
                Some(v) => assert_eq!(v, b"value1", "Expected value to be b'value1'"),
                None => panic!("Expected a value to be found'"),
            }
            match store.get(b"key2") {
                Some(v) => assert_eq!(v, b"value2", "Expected value to be b'value1'"),
                None => panic!("Expected a value to be found'"),
            }
            match store.get(b"key3") {
                Some(v) => assert_eq!(v, b"value3", "Expected value to be b'value1'"),
                None => panic!("Expected a value to be found'"),
            }

            assert_eq!(
                store.get_sstables_count(),
                1,
                "Compaction should result in 1 table."
            );
            drop(store);
        }));
        assert!(result.is_ok());
    }
}
