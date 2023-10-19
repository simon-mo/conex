use containerd_snapshots as snapshots;
use containerd_snapshots::{api, Info, Usage};
use snapshots::tonic::transport::Server;
use snapshots::Kind;
use std::collections::HashSet;
use std::path::Path;
use std::time::SystemTime;
use std::{collections::HashMap, fmt::Debug, sync::Arc};
use std::{
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::net::UnixListener;
use tokio::sync::Mutex;
use tokio_stream::wrappers::UnixListenerStream;
use tokio_stream::Stream;
use tracing::info;

use crate::puller::ContainerPuller;

fn clone_info_hack(info: &Info) -> Info {
    // a hack because the Info doesn't have copy trait
    serde_json::from_str(&serde_json::to_string(info).unwrap()).unwrap()
}

pub struct WalkStream {
    pub infos: Vec<Info>,
}
impl WalkStream {
    pub fn new() -> Self {
        WalkStream { infos: Vec::new() }
    }
}

impl Stream for WalkStream {
    type Item = Result<Info, snapshots::tonic::Status>;
    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Poll::Ready(None)
        let next: Option<Info> = self.deref_mut().infos.pop();
        match next {
            Some(info) => Poll::Ready(Some(Ok(info))),
            None => Poll::Ready(None),
        }
    }
}

struct DataStore {
    info_vec: Vec<Info>,
    key_to_sha_map: HashMap<String, String>,
    key_to_mount: HashMap<String, api::types::Mount>,
    sha_fetched: HashSet<String>,
}

impl DataStore {
    async fn new() -> Self {
        DataStore {
            info_vec: Vec::new(),
            key_to_sha_map: HashMap::new(),
            key_to_mount: HashMap::new(),
            sha_fetched: HashSet::new(),
        }
    }
}

struct SkySnapshotter {
    client: reqwest::Client,
    data_store: Arc<Mutex<DataStore>>,
    snapshot_dir: String,
    mount_dir: String,
}

impl SkySnapshotter {
    async fn new(snapshot_dir: String, mount_dir: String) -> Self {
        SkySnapshotter {
            client: reqwest::Client::new(),
            data_store: Arc::new(Mutex::new(DataStore::new().await)),
            snapshot_dir,
            mount_dir,
        }
    }
}

impl Debug for SkySnapshotter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SkySnapshotter").finish()
    }
}

impl SkySnapshotter {
    async fn prefetch_image(&self, image_ref: String) {
        let puller = ContainerPuller::new(self.snapshot_dir.clone().into());
        let skip_layers = { self.data_store.lock().await.sha_fetched.clone() };
        let layers_fetched = puller
            .pull(image_ref, num_cpus::get(), false, Some(skip_layers))
            .await;

        // Add to data store
        {
            let mut store = self.data_store.lock().await;
            for layer in layers_fetched {
                store.sha_fetched.insert(layer);
            }
        }
    }
}

#[snapshots::tonic::async_trait]
impl snapshots::Snapshotter for SkySnapshotter {
    type Error = snapshots::tonic::Status;

    #[tracing::instrument(level = "info", skip(self, labels))]
    async fn prepare(
        &self,
        key: String,
        parent: String,
        labels: HashMap<String, String>,
    ) -> Result<Vec<api::types::Mount>, Self::Error> {
        info!(
            "Prepare: key={}, parent={}, label={:?}",
            key, parent, labels
        );

        if !labels.contains_key("containerd.io/snapshot.ref") {
            info!(
                "Preparing a working container, not a snapshot. key={}, parent={}",
                key, parent
            );

            let mut mount_vec = Vec::new();
            {
                let mut store = self.data_store.lock().await;
                let mut keys = vec![key.clone()];

                let mut parent = parent;
                while !parent.is_empty() {
                    let parent_info = store
                        .info_vec
                        .iter()
                        .find(|info| info.name == parent)
                        .unwrap();
                    keys.push(parent_info.name.clone());
                    parent = parent_info.parent.clone();
                }
                info!("Preparing an overlay fs for keys: {:?}", keys);

                let lower_dir_keys = &keys[1..];
                assert!(!lower_dir_keys.is_empty());
                let mut lower_dirs = lower_dir_keys
                    .iter()
                    .map(|key| {
                        let sha = store.key_to_sha_map.get(key).expect(
                            format!(
                            "can't find the corresponding sha for key={}, this shouldn't happen.",
                            key
                        )
                            .as_str(),
                        );
                        let dir = format!("{}/{}", self.snapshot_dir, sha);
                        assert!(std::path::Path::new(&dir).is_dir());
                        dir
                    })
                    .filter(|dir| {
                        std::path::Path::new(dir).is_dir()
                            && std::fs::read_dir(dir).unwrap().count() > 0
                    })
                    .collect::<Vec<String>>();
                lower_dirs.reverse();

                let overylay_dir = Path::new(&self.mount_dir).join(key.replace('/', "-"));
                if !overylay_dir.is_dir() {
                    std::fs::create_dir_all(&overylay_dir).unwrap();
                }
                let upper_dir = overylay_dir.join("fs");
                let work_dir = overylay_dir.join("work");
                std::fs::create_dir(&upper_dir).unwrap();
                std::fs::create_dir(&work_dir).unwrap();

                let mut options: Vec<String> = vec![];
                options.push("index=off".to_string());
                options.push("userxattr".to_string());
                options.push(format!("upperdir={}", upper_dir.to_str().unwrap()));
                options.push(format!("workdir={}", work_dir.to_str().unwrap()));
                options.push(format!("lowerdir={}", lower_dirs.join(":")));

                let mount = api::types::Mount {
                    r#type: "overlay".to_string(),
                    source: "overlay".to_string(),
                    options,
                    ..Default::default()
                };
                info!(
                    "Sending mount array: `mount -t {} {} -o{}`",
                    mount.r#type,
                    mount.source,
                    mount.options.join(","),
                );
                mount_vec.push(mount.clone());

                store.info_vec.push(Info {
                    name: key.clone(),
                    parent,
                    kind: Kind::Active,
                    ..Default::default()
                });
                store.key_to_mount.insert(key.clone(), mount);
            }
            return Ok(mount_vec);
        }

        let image_ref = labels.get("containerd.io/snapshot/cri.image-ref").unwrap();
        let layer_ref = labels
            .get("containerd.io/snapshot/cri.layer-digest")
            .unwrap()
            .replace("sha256:", "");

        let layer_exists = {
            let store = self.data_store.lock().await;
            if store.sha_fetched.contains(layer_ref.as_str()) {
                info!("{} already fetched, skipping", layer_ref);
                true
            } else {
                false
            }
        };

        if !layer_exists {
            self.prefetch_image(image_ref.to_owned()).await;
        }

        {
            let mut store = self.data_store.lock().await;
            assert!(store.sha_fetched.contains(layer_ref.as_str()));
            store.key_to_sha_map.insert(key.clone(), layer_ref.clone());
            store.info_vec.push(Info {
                kind: Kind::Committed,
                name: key.clone(),
                parent,
                labels: labels.clone(),
                created_at: SystemTime::now(),
                updated_at: SystemTime::now(),
            });
        }

        Err(snapshots::tonic::Status::already_exists("already exists"))
    }

    type InfoStream = WalkStream;
    #[tracing::instrument(level = "info")]
    async fn list(
        &self,
        _snapshotter: String,
        filters: Vec<String>,
    ) -> Result<Self::InfoStream, Self::Error> {
        let mut stream = WalkStream::new();
        {
            stream.infos.extend(
                self.data_store
                    .lock()
                    .await
                    .info_vec
                    .iter()
                    .filter(|info| {
                        for filters_ in filters.clone() {
                            for filter in filters_.as_str().split(",") {
                                let [key, value] = filter.split("==").collect::<Vec<&str>>()[..] else {
                                    panic!("Invalid filter {}", filter);
                                };
                                if key == "parent" {
                                    if info.parent != value {
                                        return false;
                                    }
                                } else if key.starts_with("labels") {
                                    let label_key = key.replace("labels.", "").replace('"', "");
                                    if info.labels.get(&label_key) != Some(&value.to_string()) {
                                        return false;
                                    }
                                } else {
                                    panic!("Unknown filter {}", key);
                                }
                                return true;
                            }
                        }
                        true
                    })
                    .map(clone_info_hack)
            );
        }
        info!(
            "List: filters {:?}, returning {} entries",
            &filters,
            stream.infos.len()
        );

        Ok(stream)
    }

    #[tracing::instrument(level = "info")]
    async fn stat(&self, key: String) -> Result<Info, Self::Error> {
        info!("Stat: {}", key);
        self.data_store
            .lock()
            .await
            .info_vec
            .iter()
            .find(|info| info.name == key)
            .map(clone_info_hack)
            .ok_or(snapshots::tonic::Status::not_found(
                "Not found from skysnaphotter",
            ))
    }

    async fn update(
        &self,
        info: Info,
        fieldpaths: Option<Vec<String>>,
    ) -> Result<Info, Self::Error> {
        info!("Update: info={:?}, fieldpaths={:?}", info, fieldpaths);
        todo!();
        // Ok(Info::default())
    }

    async fn usage(&self, key: String) -> Result<Usage, Self::Error> {
        info!("Usage: {}", key);
        todo!();
        // Ok(Usage::default())
    }

    #[tracing::instrument(level = "info")]
    async fn mounts(&self, key: String) -> Result<Vec<api::types::Mount>, Self::Error> {
        info!("Mounts: {}", key);
        {
            let store = self.data_store.lock().await;
            if let Some(mount) = store.key_to_mount.get(&key) {
                return Ok(vec![mount.clone()]);
            } else {
                return Err(snapshots::tonic::Status::not_found(
                    "Not found from skysnaphotter",
                ));
            }
        }
    }

    async fn view(
        &self,
        key: String,
        parent: String,
        labels: HashMap<String, String>,
    ) -> Result<Vec<api::types::Mount>, Self::Error> {
        info!("View: key={}, parent={}, labels={:?}", key, parent, labels);
        todo!();
        // Ok(Vec::new())
    }

    async fn commit(
        &self,
        name: String,
        key: String,
        labels: HashMap<String, String>,
    ) -> Result<(), Self::Error> {
        info!("Commit: name={}, key={}, labels={:?}", name, key, labels);
        todo!();
        // Ok(())
    }

    async fn remove(&self, key: String) -> Result<(), Self::Error> {
        info!("Remove: {}, returning Ok", key);
        // todo!();
        Ok(())
    }
}

pub async fn serve_snapshotter(snapshot_path: String, mount_path: String, socket_path: String) {
    // remove socket_path if it exists
    if std::fs::metadata(&socket_path).is_ok() {
        std::fs::remove_file(&socket_path).expect("Failed to remove socket");
    }

    let sky_snapshotter = SkySnapshotter::new(snapshot_path, mount_path).await;

    let incoming = {
        let uds = UnixListener::bind(&socket_path).expect("Failed to bind listener");
        UnixListenerStream::new(uds)
    };

    Server::builder()
        .add_service(snapshots::server(Arc::new(sky_snapshotter)))
        .serve_with_incoming(incoming)
        .await
        .expect("Serve failed");
}
