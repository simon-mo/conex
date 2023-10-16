use core::panic;
use std::collections::{HashMap, VecDeque};
use std::fs::{self};
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;

pub struct ConexPlanner {
    pub layer_to_files: Vec<(String, Vec<ConexFile>)>,
}

#[derive(Clone, Debug)]
pub struct ConexFile {
    pub path: PathBuf,
    pub relative_path: PathBuf,
    pub size: usize,
    pub inode: u64,
    pub hard_link_to: Option<PathBuf>,
    pub ctime_nsec: i64,
}

impl ConexPlanner {
    pub fn default() -> Self {
        Self {
            layer_to_files: Vec::new(),
        }
    }

    pub fn ingest_dir(&mut self, dir_path: &str) {
        let base_path = PathBuf::from(dir_path.clone());

        if base_path.metadata().is_err()
            && base_path.metadata().err().unwrap().kind() == std::io::ErrorKind::PermissionDenied
        {
            panic!(
                "Path is not accessible.
            Run `sudo setfacl -m u:ubuntu:rx /var /var/lib /var/lib/docker`
            and `sudo setfacl -R -m u:ubuntu:rx /var /var/lib /var/lib/docker/overlay2`
            "
            );
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
                }

                file_metadata_vec.push(ConexFile {
                    path: entry.path(),
                    relative_path,
                    size: metadata.len() as usize,
                    inode: metadata.ino() as u64,
                    hard_link_to: None,
                    ctime_nsec: metadata.ctime_nsec(),
                });
            }
        }

        self.layer_to_files
            .push((dir_path.to_owned(), file_metadata_vec));
    }

    pub fn generate_plan(mut self) -> Vec<(String, Vec<ConexFile>)> {
        // Pass 1: find all hard links, point them to the same inode.
        self.layer_to_files = self
            .layer_to_files
            .into_iter()
            .map(|(key, mut files)| {
                let mut inode_map: HashMap<u64, PathBuf> = HashMap::new();

                for file in files.iter_mut() {
                    if let Some(hard_link_to) = inode_map.get(&file.inode) {
                        file.hard_link_to = Some(hard_link_to.to_owned());
                    } else {
                        inode_map.insert(file.inode, file.relative_path.to_owned());
                    }
                }

                (key, files)
            })
            .collect::<Vec<(String, Vec<ConexFile>)>>();

        self.layer_to_files.clone()
    }
}
