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
