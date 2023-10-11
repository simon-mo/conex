use std::{
    io::Error,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures_core::Stream;

#[derive(Clone, Debug, PartialEq)]
pub enum UpdateType {
    TarAdd,
    SocketSend,
}
pub struct UpdateItem {
    pub key: String, // TODO: change to CoW
    pub delta: usize,
    pub update_type: UpdateType,
}

pub struct ProgressWriter<T: std::io::Write> {
    inner: T,
    key: String, //TODO: this should be cow
    update_type: UpdateType,
    progress_tx: tokio::sync::mpsc::UnboundedSender<UpdateItem>,
}

impl<T: std::io::Write> ProgressWriter<T> {
    pub fn new(
        inner: T,
        key: String,
        update_type: UpdateType,
        progress_tx: tokio::sync::mpsc::UnboundedSender<UpdateItem>,
    ) -> Self {
        Self {
            inner,
            key,
            update_type,
            progress_tx,
        }
    }
}

impl<T: std::io::Write> std::io::Write for ProgressWriter<T> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let len = self.inner.write(buf)?;
        self.progress_tx
            .send(UpdateItem {
                key: self.key.clone(),
                delta: len,
                update_type: self.update_type.clone(),
            })
            .unwrap();
        Ok(len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

pub struct ProgressStreamer {
    progress_tx: tokio::sync::mpsc::UnboundedSender<UpdateItem>,
    key: String,
    update_type: UpdateType,
    data: Bytes,

    offset: usize,
    chunksize: usize,
}

impl ProgressStreamer {
    pub fn new(
        progress_tx: tokio::sync::mpsc::UnboundedSender<UpdateItem>,
        key: String,
        update_type: UpdateType,
        data: Bytes,
    ) -> Self {
        Self {
            progress_tx,
            key,
            update_type,
            data,
            offset: 0,
            chunksize: 8 * 1024,
        }
    }
}

impl Stream for ProgressStreamer {
    type Item = Result<Bytes, Error>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let len = this.data.len();

        if this.offset >= len {
            return Poll::Ready(None);
        }

        let end = std::cmp::min(this.offset + this.chunksize, len);
        let chunk = this.data.slice(this.offset..end);

        this.offset += this.chunksize;
        this.progress_tx
            .send(UpdateItem {
                key: this.key.clone(),
                delta: chunk.len(),
                update_type: this.update_type.clone(),
            })
            .unwrap();
        Poll::Ready(Some(Ok(chunk)))
    }
}
