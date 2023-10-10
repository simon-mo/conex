use core::panic;
use std::collections::{HashMap, VecDeque};
use std::fs::{self, Metadata};
use std::path::PathBuf;

pub struct ConexPlanner {
    pub layer_to_files: HashMap<String, Vec<ConexFile>>,
}

#[derive(Clone, Debug)]
pub struct ConexFile {
    pub path: PathBuf,
    pub metadata: Metadata,
    pub relative_path: PathBuf,
}

impl ConexPlanner {
    pub fn default() -> Self {
        Self {
            layer_to_files: HashMap::new(),
        }
    }

    pub fn ingest_dir(&mut self, dir_path: &str) {
        let base_path = PathBuf::from(dir_path.clone());

        if base_path.metadata().is_err() {
            if base_path.metadata().err().unwrap().kind() == std::io::ErrorKind::PermissionDenied {
                panic!(
                    "Path is not accessible.
                Run `sudo setfacl -m u:ubuntu:rx /var /var/lib /var/lib/docker`
                and `sudo setfacl -R -m u:ubuntu:rx /var /var/lib /var/lib/docker/overlay2`
                "
                );
            }
        }

        if !base_path.is_dir() {
            // TODO: change to log fatal.
            panic!("Path is not a directory");
        }

        let mut queue = VecDeque::new();
        queue.push_back(PathBuf::new());

        let mut file_metadata_vec = Vec::new();

        while let Some(current_path) = queue.pop_front() {
            let absolute_path = base_path.join(&current_path);
            for entry in fs::read_dir(&absolute_path).unwrap() {
                let entry = entry.unwrap();
                let metadata = entry.metadata().unwrap();
                let relative_path = entry.path().strip_prefix(&base_path).unwrap().to_path_buf();

                if entry.path().is_dir() && !metadata.is_symlink() {
                    queue.push_back(relative_path.to_owned());
                } else {
                    file_metadata_vec.push(ConexFile {
                        path: entry.path(),
                        metadata,
                        relative_path,
                    });
                }
            }
        }

        self.layer_to_files
            .insert(dir_path.to_owned(), file_metadata_vec);
    }

    pub fn generate_plan(&self) -> HashMap<String, Vec<ConexFile>> {
        // currently it is a noop.
        return self.layer_to_files.clone();
    }
}
