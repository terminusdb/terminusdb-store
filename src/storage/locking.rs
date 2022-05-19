#![allow(unused)]
use crate::storage::{layer, Label};
use fs2::*;
use futures::future::Future;
use futures::task::{Context, Poll};
use std::io::Read;
use std::io::{self, SeekFrom};
use std::path::*;
use std::pin::Pin;
use tokio::fs;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::task::{spawn_blocking, JoinHandle};

pub struct LockedFileLockFuture {
    file: Option<std::fs::File>,
    spawn: Option<JoinHandle<()>>,
    exclusive: bool,
}

impl LockedFileLockFuture {
    fn new_shared(file: std::fs::File) -> Self {
        Self {
            file: Some(file),
            spawn: None,
            exclusive: false,
        }
    }

    fn new_exclusive(file: std::fs::File) -> Self {
        Self {
            file: Some(file),
            spawn: None,
            exclusive: true,
        }
    }
}

impl Future for LockedFileLockFuture {
    type Output = io::Result<std::fs::File>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<std::fs::File>> {
        if self.file.is_some() {
            if self.spawn.is_none() {
                let file = self
                    .file
                    .as_ref()
                    .unwrap()
                    .try_clone()
                    .expect("file clone failed");
                let exclusive = self.exclusive;
                self.spawn = Some(spawn_blocking(move || {
                    if exclusive {
                        file.lock_exclusive()
                            .expect("failed to acquire exclusive lock")
                    } else {
                        if !cfg!(feature = "noreadlock") {
                            file.lock_shared()
                                .expect("failed to acquire exclusive lock")
                        }
                    }
                }));
            }

            match Pin::new(&mut self.spawn.as_mut().unwrap()).poll(cx) {
                Poll::Ready(Ok(_)) => {
                    let mut file = None;
                    std::mem::swap(&mut file, &mut self.file);
                    Poll::Ready(Ok(file.unwrap()))
                }
                Poll::Pending => Poll::Pending,
                Poll::Ready(Err(_)) => {
                    panic!("polled LockedFileLockFuture outside of a tokio context")
                }
            }
        } else {
            panic!("polled LockedFileLockFuture after completion");
        }
    }
}

#[derive(Debug)]
pub struct LockedFile {
    file: Option<fs::File>,
}

impl LockedFile {
    pub async fn open<P: 'static + AsRef<Path> + Send>(path: P) -> io::Result<Self> {
        let file = fs::OpenOptions::new()
            .read(true)
            .open(path)
            .await?
            .into_std()
            .await;
        let file = match file.try_lock_shared() {
            Ok(()) => file,
            Err(_) => LockedFileLockFuture::new_shared(file).await?,
        };

        Ok(LockedFile {
            file: Some(fs::File::from_std(file)),
        })
    }

    pub async fn try_open<P: 'static + AsRef<Path> + Send>(path: P) -> io::Result<Option<Self>> {
        match Self::open(path).await {
            Ok(f) => Ok(Some(f)),
            Err(e) => match e.kind() {
                io::ErrorKind::NotFound => Ok(None),
                _ => Err(e),
            },
        }
    }

    pub async fn create_and_open<P: 'static + AsRef<Path> + Send>(path: P) -> io::Result<Self> {
        let path = PathBuf::from(path.as_ref());
        match Self::try_open(path.clone()).await? {
            Some(file) => Ok(file),
            None => {
                let mut file = fs::OpenOptions::new()
                    .write(true)
                    .truncate(false)
                    .create(true)
                    .open(path.clone())
                    .await?;
                file.shutdown().await?;
                Self::open(path).await
            }
        }
    }
}

impl AsyncRead for LockedFile {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut ReadBuf,
    ) -> Poll<io::Result<()>> {
        Pin::new(
            self.file
                .as_mut()
                .expect("tried to read from dropped LockedFile"),
        )
        .poll_read(cx, buf)
    }
}

impl Drop for LockedFile {
    fn drop(&mut self) {
        let mut file = None;
        std::mem::swap(&mut file, &mut self.file);
        if let Some(file) = file {
            file.try_into_std()
                .expect("could not convert tokio file into std")
                .unlock()
                .unwrap();
        }
    }
}

pub struct ExclusiveLockedFile {
    file: Option<fs::File>,
}
impl ExclusiveLockedFile {
    pub async fn create_and_open<P: 'static + AsRef<Path> + Send>(path: P) -> io::Result<Self> {
        let file = fs::OpenOptions::new()
            .create_new(true)
            .read(false)
            .write(true)
            .open(path)
            .await?
            .into_std()
            .await;

        let file = match file.try_lock_exclusive() {
            Ok(()) => file,
            Err(_) => LockedFileLockFuture::new_exclusive(file).await?,
        };

        Ok(ExclusiveLockedFile {
            file: Some(fs::File::from_std(file)),
        })
    }

    pub async fn open<P: 'static + AsRef<Path> + Send>(path: P) -> io::Result<Self> {
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .await?
            .into_std()
            .await;

        let file = match file.try_lock_exclusive() {
            Ok(()) => file,
            Err(_) => LockedFileLockFuture::new_exclusive(file).await?,
        };

        Ok(ExclusiveLockedFile {
            file: Some(fs::File::from_std(file)),
        })
    }

    pub async fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let file = self
            .file
            .as_mut()
            .expect("tried to truncate a dropped file");
        file.seek(pos).await
    }

    pub async fn truncate(&mut self) -> io::Result<()> {
        let file = self
            .file
            .as_mut()
            .expect("tried to truncate a dropped file");
        let pos = file.seek(SeekFrom::Current(0)).await?;
        file.set_len(pos).await
    }

    pub async fn sync_all(&mut self) -> io::Result<()> {
        let file = self.file.as_mut().expect("tried to sync a dropped file");
        file.sync_all().await
    }
}

impl AsyncRead for ExclusiveLockedFile {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut ReadBuf,
    ) -> Poll<io::Result<()>> {
        Pin::new(
            self.file
                .as_mut()
                .expect("tried to read from dropped ExclusiveLockedFile"),
        )
        .poll_read(cx, buf)
    }
}

impl AsyncWrite for ExclusiveLockedFile {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(
            self.file
                .as_mut()
                .expect("tried to write to a dropped ExclusiveLockedFile"),
        )
        .poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        Pin::new(
            self.file
                .as_mut()
                .expect("tried to flush a dropped ExclusiveLockedFile"),
        )
        .poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        Pin::new(
            self.file
                .as_mut()
                .expect("tried to shutdown a dropped ExclusiveLockedFile"),
        )
        .poll_shutdown(cx)
    }
}

impl Drop for ExclusiveLockedFile {
    fn drop(&mut self) {
        let mut file = None;
        std::mem::swap(&mut file, &mut self.file);
        if let Some(file) = file {
            file.try_into_std()
                .expect("could not convert tokio file into std")
                .unlock()
                .unwrap();
        }
    }
}
