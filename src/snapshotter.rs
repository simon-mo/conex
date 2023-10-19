use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    path::Path,
    sync::Arc,
    time::SystemTime,
    vec,
};

use containerd_snapshots as snapshots;
use containerd_snapshots::{api, Info, Kind, Usage};
use futures::TryStreamExt;
use oci_spec::image::{Descriptor, ImageManifest};
use snapshots::tonic::transport::Server;
use tokio::net::UnixListener;
use tokio::sync::Mutex;
use tokio_stream::wrappers::UnixListenerStream;

use tracing::info;

use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::{
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll},
};

use tokio_stream::Stream;

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

struct DataStore {}

impl DataStore {
    async fn new() -> Self {
        DataStore {}
    }
}

struct SkySnapshotter {}

impl SkySnapshotter {
    async fn new() -> Self {
        SkySnapshotter {}
    }
}

impl Debug for SkySnapshotter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SkySnapshotter").finish()
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

        Err(snapshots::tonic::Status::already_exists("already exists"))
    }

    type InfoStream = WalkStream;
    #[tracing::instrument(level = "info")]
    async fn list(
        &self,
        snapshotter: String,
        filters: Vec<String>,
    ) -> Result<Self::InfoStream, Self::Error> {
        let mut stream = WalkStream::new();
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
        todo!()
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
        todo!();
        // info!("Usage: {}", key);
        // Ok(Usage::default())
    }

    #[tracing::instrument(level = "info")]
    async fn mounts(&self, key: String) -> Result<Vec<api::types::Mount>, Self::Error> {
        info!("Mounts: {}", key);
        // {
        //     let store = self.data_store.lock().await;
        //     if let Some(mount) = store.key_to_mount.get(&key) {
        //         return Ok(vec![mount.clone()]);
        //     } else {
        //         return Err(snapshots::tonic::Status::not_found(
        //             "Not found from skysnaphotter",
        //         ));
        //     }
        // }
        todo!();
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
        info!("Remove: {}", key);
        todo!();
        // Ok(())
    }
}

pub async fn serve_snapshotter() {
    let socket_path = "/tmp/conex-snapshotter.sock";

    // remove socket_path if it exists
    if std::fs::metadata(socket_path).is_ok() {
        std::fs::remove_file(socket_path).expect("Failed to remove socket");
    }

    let sky_snapshotter = SkySnapshotter::new().await;

    let incoming = {
        let uds = UnixListener::bind(socket_path).expect("Failed to bind listener");
        UnixListenerStream::new(uds)
    };

    Server::builder()
        .add_service(snapshots::server(Arc::new(sky_snapshotter)))
        .serve_with_incoming(incoming)
        .await
        .expect("Serve failed");
}
