use core::panic;
use std::collections::{HashMap, HashSet, VecDeque};
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
                // let metadata = entry.metadata().unwrap();
                let metadata = std::fs::symlink_metadata(entry.path()).unwrap();
                let relative_path = entry.path().strip_prefix(&base_path).unwrap().to_path_buf();

                if entry.path().is_dir() && !metadata.is_symlink() {
                    queue.push_back(relative_path.to_owned());
                }

                file_metadata_vec.push(ConexFile {
                    path: entry.path(),
                    relative_path,
                    size: metadata.len() as usize,
                    inode: metadata.ino(),
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

        // Pass 2: Split and collapse layers so the size is about 512MB.
        // let mut new_layer_to_files = Vec::new();
        // let mut current_layer_size: usize = 0;
        // let mut new_layer = Vec::new();
        // for (layer, files) in self.layer_to_files.iter() {
        //     for file in files.iter() {
        //         if current_layer_size + file.size > 512 * 1024 * 1024 {
        //             new_layer_to_files.push((layer.to_owned(), new_layer.clone()));
        //             new_layer = Vec::new();
        //             current_layer_size = 0;
        //         }

        //         new_layer.push(file.to_owned());
        //         current_layer_size += file.size;
        //     }

        //     if !new_layer.is_empty() {
        //         new_layer_to_files.push((layer.to_owned(), new_layer));
        //     }
        // }

        self.layer_to_files.clone()
    }
}
