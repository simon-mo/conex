use std::collections::{HashMap, HashSet};

use containerd_snapshots::{api::types::Mount, Info};

fn clone_info_hack(info: &Info) -> Info {
    // a hack because the Info doesn't have copy trait
    serde_json::from_str(&serde_json::to_string(info).unwrap()).unwrap()
}

pub struct DataStore {
    info_vec: Vec<Info>,
    key_to_sha_map: HashMap<String, String>,
    key_to_mount: HashMap<String, Mount>,
    sha_fetched: HashSet<String>,
}

impl DataStore {
    pub fn new() -> Self {
        DataStore {
            info_vec: Vec::new(),
            key_to_sha_map: HashMap::new(),
            key_to_mount: HashMap::new(),
            sha_fetched: HashSet::new(),
        }
    }

    pub fn get_all_sha_fetched(&self) -> HashSet<String> {
        self.sha_fetched.clone()
    }

    pub fn get_all_info(&self) -> Vec<Info> {
        self.info_vec.iter().map(clone_info_hack).collect()
    }

    pub fn insert_sha_fetched(&mut self, sha: String) {
        self.sha_fetched.insert(sha);
    }

    pub fn insert_info(&mut self, info: Info) {
        self.info_vec.push(info);
    }

    pub fn insert_mount(&mut self, key: String, mount: Mount) {
        self.key_to_mount.insert(key, mount);
    }

    pub fn insert_key_to_sha(&mut self, key: String, sha: String) {
        self.key_to_sha_map.insert(key, sha);
    }

    pub fn find_info_by_name(&self, name: &str) -> Option<Info> {
        self.info_vec
            .iter()
            .find(|info| info.name == name)
            .map(clone_info_hack)
    }

    pub fn find_sha_by_key(&self, key: &str) -> Option<&String> {
        self.key_to_sha_map.get(key)
    }

    pub fn find_mount_by_key(&self, key: &str) -> Option<&Mount> {
        self.key_to_mount.get(key)
    }

    pub fn is_sha_fetched(&self, sha: &str) -> bool {
        self.sha_fetched.contains(sha)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use containerd_snapshots::api::types::Mount;

    #[test]
    fn test_new() {
        let store = DataStore::new();
        assert_eq!(store.get_all_info().len(), 0);
        assert_eq!(store.get_all_sha_fetched().len(), 0);
    }

    #[test]
    fn test_insert_and_get_all_sha_fetched() {
        let mut store = DataStore::new();
        store.insert_sha_fetched("sha1".to_string());
        store.insert_sha_fetched("sha2".to_string());

        let shas = store.get_all_sha_fetched();
        assert_eq!(shas.len(), 2);
        assert!(shas.contains("sha1"));
        assert!(shas.contains("sha2"));
    }

    #[test]
    fn test_insert_and_get_all_info() {
        let mut store = DataStore::new();
        let info1 = Info::default();
        let info2 = Info::default();
        store.insert_info(clone_info_hack(&info1));
        store.insert_info(clone_info_hack(&info2));

        let infos = store.get_all_info();
        assert_eq!(infos.len(), 2);
    }

    #[test]
    fn test_insert_and_find_info_by_name() {
        let mut store = DataStore::new();
        let mut info = Info::default();
        info.name = "test_name".to_string();
        store.insert_info(info);

        let result = store.find_info_by_name("test_name");
        assert!(result.is_some());
        assert_eq!(result.unwrap().name, "test_name")
    }

    #[test]
    fn test_insert_key_to_sha_and_find_sha_by_key() {
        let mut store = DataStore::new();
        store.insert_key_to_sha("key1".to_string(), "sha1".to_string());

        assert_eq!(store.find_sha_by_key("key1"), Some(&"sha1".to_string()));
    }

    #[test]
    fn test_insert_mount_and_find_mount_by_key() {
        let mut store = DataStore::new();
        let mount = Mount::default();
        store.insert_mount("key1".to_string(), mount.clone());

        assert_eq!(store.find_mount_by_key("key1"), Some(&mount));
    }

    #[test]
    fn test_is_sha_fetched() {
        let mut store = DataStore::new();
        store.insert_sha_fetched("sha1".to_string());

        assert!(store.is_sha_fetched("sha1"));
        assert!(!store.is_sha_fetched("sha2"));
    }
}
