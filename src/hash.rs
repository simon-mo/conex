use ring::digest::{Context as HashContext, SHA256};

pub struct StreamingHashWriter<T: std::io::Write> {
    inner: T,
    bytes_written: usize,
    on_finish: Option<Box<dyn FnOnce(String, usize)>>,
    context: HashContext,
}

impl<T: std::io::Write> StreamingHashWriter<T> {
    pub fn new(inner: T, on_finish: Option<Box<dyn FnOnce(String, usize)>>) -> Self {
        Self {
            inner,
            bytes_written: 0,
            on_finish,
            context: HashContext::new(&SHA256),
        }
    }

    fn digest(&self) -> String {
        data_encoding::HEXLOWER.encode(self.context.clone().finish().as_ref())
        // hex::encode(self.context.clone().finish().as_ref())
    }
}

impl<T: std::io::Write> Drop for StreamingHashWriter<T> {
    fn drop(&mut self) {
        if let Some(on_finish) = self.on_finish.take() {
            on_finish(self.digest(), self.bytes_written);
        }
    }
}

impl<T: std::io::Write> std::io::Write for StreamingHashWriter<T> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(buf)?;
        self.bytes_written += written;
        self.context.update(&buf[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
