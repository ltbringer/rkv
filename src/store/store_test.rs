#[cfg(test)]
mod store_test {
    use std::panic::{self, AssertUnwindSafe};
    use tempfile::TempDir;
    use super::super::store::KVStore;

    #[test]
    fn test_add_item() {
        let key = b"life";
        let value = b"42";
        let result = panic::catch_unwind(AssertUnwindSafe(|| {
            let temp_dir = match TempDir::new() {
                Ok(dir) => dir,
                Err(_) => panic!("Failed creating tempdir.")
            };
            let mut store = KVStore::new(20, temp_dir.into_path());
            store.set(key, value);
            match store.get(b"life") {
                Some(v) => assert_eq!(v, value, "Expected value to be b'42'"),
                None => panic!("Expected value to be b'42'")
            }
            drop(store);
        }));
        assert!(result.is_ok());
    }
}
