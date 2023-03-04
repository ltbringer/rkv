#[cfg(test)]
mod store_test {
    use super::super::store::KVStore;

    #[test]
    fn test_add_item() {
        let key = b"life";
        let value = b"42";

        let mut store = KVStore::new(None);
        store.set(key, value);
        match store.get(b"life") {
            Some(v) => assert_eq!(v, value, "Expected value to be b'42'"),
            None => panic!("Expected value to be b'42'")
        }
    }
}
