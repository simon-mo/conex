use containerd_snapshots::{api::types::Mount, Info};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, fmt::Debug};

#[derive(Serialize, Deserialize)]
pub struct SerdeMount {
    r#type: String,
    source: String,
    options: Vec<String>,
}

impl From<Mount> for SerdeMount {
    fn from(mount: Mount) -> Self {
        SerdeMount {
            r#type: mount.r#type,
            source: mount.source,
            options: mount.options,
        }
    }
}

impl From<SerdeMount> for Mount {
    fn from(val: SerdeMount) -> Self {
        Mount {
            r#type: val.r#type,
            source: val.source,
            options: val.options,
            ..Default::default()
        }
    }
}

pub struct DataStore {
    info_tree: sled::Tree,
    key_to_sha_tree: sled::Tree,
    key_to_mount_tree: sled::Tree,
    sha_fetched_tree: sled::Tree,
    // info_vec: Vec<Info>,
    // key_to_sha_map: HashMap<String, String>,
    // key_to_mount: HashMap<String, Mount>,
    // sha_fetched: HashSet<String>,
}

impl Debug for DataStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataStore")
            .field(
                "info_tree",
                &self
                    .info_tree
                    .iter()
                    .map(|x| String::from_utf8(x.unwrap().0.to_vec()).unwrap())
                    .join(","),
            )
            .finish()
    }
}

impl DataStore {
    pub fn new(path: String) -> Self {
        let db = sled::open(path).unwrap();
        DataStore {
            info_tree: db.open_tree("info").unwrap(),
            key_to_sha_tree: db.open_tree("key_to_sha").unwrap(),
            key_to_mount_tree: db.open_tree("key_to_mount").unwrap(),
            sha_fetched_tree: db.open_tree("sha_fetched").unwrap(),
            // info_vec: Vec::new(),
            // key_to_sha_map: HashMap::new(),
            // key_to_mount: HashMap::new(),
            // sha_fetched: HashSet::new(),
        }
    }

    pub fn get_all_sha_fetched(&self) -> HashSet<String> {
        self.sha_fetched_tree
            .iter()
            .map(|x| x.unwrap().0.to_vec())
            .map(|s| String::from_utf8(s).unwrap())
            .collect::<HashSet<String>>()
        // self.sha_fetched.clone()
    }

    pub fn get_all_info(&self) -> Vec<Info> {
        self.info_tree
            .iter()
            .map(|x| x.unwrap().1.to_vec())
            .map(|s| String::from_utf8(s).unwrap())
            .map(|s| serde_json::from_str(&s).unwrap())
            .collect::<Vec<Info>>()
        // self.info_vec.iter().map(clone_info_hack).collect()
    }

    pub fn insert_sha_fetched(&mut self, sha: String) {
        self.sha_fetched_tree
            .insert(sha.clone(), "".as_bytes())
            .unwrap();
        // self.sha_fetched.insert(sha);
    }

    pub fn upsert_info(&mut self, info: Info) {
        let serialized = serde_json::to_string(&info).unwrap();
        self.info_tree
            .insert(info.name.clone(), serialized.as_str())
            .unwrap();

        // self.info_vec.push(info);
    }

    pub fn insert_mount(&mut self, key: String, mount: Mount) {
        let serde_mount = SerdeMount::from(mount);
        let serialized = serde_json::to_string(&serde_mount).unwrap();
        self.key_to_mount_tree
            .insert(key.clone(), serialized.as_str())
            .unwrap();
        // self.key_to_mount.insert(key, mount);
    }

    pub fn insert_key_to_sha(&mut self, key: String, sha: String) {
        self.key_to_sha_tree
            .insert(key.clone(), sha.clone().as_str())
            .unwrap();

        // self.key_to_sha_map.insert(key, sha);
    }

    pub fn find_info_by_name(&self, name: &str) -> Option<Info> {
        self.info_tree
            .get(name)
            .unwrap()
            .map(|x| x.to_vec())
            .map(|s| String::from_utf8(s).unwrap())
            .map(|s| serde_json::from_str(&s).unwrap())

        // self.info_vec
        //     .iter()
        //     .find(|info| info.name == name)
        //     .map(clone_info_hack)
    }

    pub fn find_sha_by_key(&self, key: &str) -> Option<String> {
        self.key_to_sha_tree
            .get(key)
            .unwrap()
            .map(|x| x.to_vec())
            .map(|s| String::from_utf8(s).unwrap())

        // self.key_to_sha_map.get(key)
    }

    pub fn find_mount_by_key(&self, key: &str) -> Option<Mount> {
        self.key_to_mount_tree
            .get(key)
            .unwrap()
            .map(|x| x.to_vec())
            .map(|s| String::from_utf8(s).unwrap())
            .map(|s| serde_json::from_str(&s).unwrap())
            .map(|s: SerdeMount| s.into())

        // self.key_to_mount.get(key)
    }

    pub fn is_sha_fetched(&self, sha: &str) -> bool {
        self.sha_fetched_tree.contains_key(sha).unwrap()

        // self.sha_fetched.contains(sha)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use containerd_snapshots::api::types::Mount;

    fn clone_info_hack(info: &Info) -> Info {
        serde_json::from_str(&serde_json::to_string(info).unwrap()).unwrap()
    }

    fn _new_store() -> DataStore {
        DataStore::new(
            tempfile::TempDir::new()
                .unwrap()
                .into_path()
                .to_path_buf()
                .to_string_lossy()
                .to_string(),
        )
    }

    #[test]
    fn test_new() {
        let store = _new_store();
        assert_eq!(store.get_all_info().len(), 0);
        assert_eq!(store.get_all_sha_fetched().len(), 0);
    }

    #[test]
    fn test_insert_and_get_all_sha_fetched() {
        let mut store = _new_store();
        store.insert_sha_fetched("sha1".to_string());
        store.insert_sha_fetched("sha2".to_string());

        let shas = store.get_all_sha_fetched();
        assert_eq!(shas.len(), 2);
        assert!(shas.contains("sha1"));
        assert!(shas.contains("sha2"));
    }

    #[test]
    fn test_insert_and_get_all_info() {
        let mut store = _new_store();
        let info1 = Info {
            name: "test_name1".to_string(),
            ..Default::default()
        };
        let info2 = Info {
            name: "test_name2".to_string(),
            ..Default::default()
        };
        store.upsert_info(clone_info_hack(&info1));
        store.upsert_info(clone_info_hack(&info2));

        let infos = store.get_all_info();
        assert_eq!(infos.len(), 2);
    }

    #[test]
    fn test_insert_and_find_info_by_name() {
        let mut store = _new_store();
        let info = Info {
            name: "test_name".to_string(),
            ..Default::default()
        };
        store.upsert_info(info);

        let result = store.find_info_by_name("test_name");
        assert!(result.is_some());
        assert_eq!(result.unwrap().name, "test_name")
    }

    #[test]
    fn test_insert_key_to_sha_and_find_sha_by_key() {
        let mut store = _new_store();
        store.insert_key_to_sha("key1".to_string(), "sha1".to_string());

        assert_eq!(store.find_sha_by_key("key1"), Some("sha1".to_string()));
    }

    #[test]
    fn test_insert_mount_and_find_mount_by_key() {
        let mut store = _new_store();
        let mount = Mount::default();
        store.insert_mount("key1".to_string(), mount.clone());

        assert_eq!(store.find_mount_by_key("key1"), Some(mount));
    }

    #[test]
    fn test_is_sha_fetched() {
        let mut store = _new_store();
        store.insert_sha_fetched("sha1".to_string());

        assert!(store.is_sha_fetched("sha1"));
        assert!(!store.is_sha_fetched("sha2"));
    }
}
