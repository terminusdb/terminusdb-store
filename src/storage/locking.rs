#![allow(unused)]
use crate::storage::{layer, Label};
use fs2::*;
use std::io::{self, SeekFrom};
use std::path::*;
use tokio::fs;
use tokio::prelude::*;
use tokio_threadpool::blocking;

pub struct LockedFileLockFuture {
    file: Option<std::fs::File>,
    exclusive: bool,
}

impl LockedFileLockFuture {
    fn new_shared(file: std::fs::File) -> Self {
        Self {
            file: Some(file),
            exclusive: false,
        }
    }

    fn new_exclusive(file: std::fs::File) -> Self {
        Self {
            file: Some(file),
            exclusive: true,
        }
    }
}

impl Future for LockedFileLockFuture {
    type Item = std::fs::File;
    type Error = io::Error;

    fn poll(&mut self) -> Result<Async<std::fs::File>, io::Error> {
        if self.file.is_none() {
            panic!("polled LockedFileLockFuture after completion");
        }

        match blocking(|| {
            if self.exclusive {
                self.file
                    .as_ref()
                    .unwrap()
                    .lock_exclusive()
                    .expect("failed to acquire exclusive lock")
            } else {
                self.file
                    .as_ref()
                    .unwrap()
                    .lock_shared()
                    .expect("failed to acquire exclusive lock")
            }
        }) {
            Ok(Async::Ready(_)) => {
                let mut file = None;
                std::mem::swap(&mut file, &mut self.file);
                Ok(Async::Ready(file.unwrap()))
            }
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(_) => panic!("polled LockedFileLockFuture outside of a tokio threadpool context"),
        }
    }
}

#[derive(Debug)]
pub struct LockedFile {
    file: Option<fs::File>,
}

impl LockedFile {
    pub fn open<P: 'static + AsRef<Path> + Send>(
        path: P,
    ) -> impl Future<Item = Self, Error = io::Error> + Send {
        fs::OpenOptions::new()
            .read(true)
            .open(path)
            .map(|f| f.into_std())
            .and_then(|f| match f.try_lock_shared() {
                Ok(()) => future::Either::A(future::ok(f)),
                Err(_) => future::Either::B(LockedFileLockFuture::new_shared(f)),
            })
            .map(|f| LockedFile {
                file: Some(fs::File::from_std(f)),
            })
    }

    pub fn try_open<P: 'static + AsRef<Path> + Send>(
        path: P,
    ) -> impl Future<Item = Option<Self>, Error = io::Error> + Send {
        Self::open(path)
            .map(|f| Some(f))
            .or_else(|e| match e.kind() {
                io::ErrorKind::NotFound => Ok(None),
                _ => Err(e),
            })
    }

    pub fn create_and_open<P: 'static + AsRef<Path> + Send>(
        path: P,
    ) -> impl Future<Item = Self, Error = io::Error> + Send {
        let path = PathBuf::from(path.as_ref());
        Self::try_open(path.clone()).and_then(move |f| match f {
            Some(file) => future::Either::A(future::ok(file)),
            None => future::Either::B(
                fs::OpenOptions::new()
                    .write(true)
                    .truncate(false)
                    .create(true)
                    .open(path.clone())
                    .and_then(|f| tokio::io::shutdown(f))
                    .and_then(|_| Self::open(path)),
            ),
        })
    }
}

impl Read for LockedFile {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        self.file
            .as_mut()
            .expect("tried to read from dropped LockedFile")
            .read(buf)
    }
}

impl AsyncRead for LockedFile {}

impl Drop for LockedFile {
    fn drop(&mut self) {
        let mut file = None;
        std::mem::swap(&mut file, &mut self.file);
        if file.is_some() {
            file.unwrap().into_std().unlock().unwrap();
        }
    }
}

pub struct ExclusiveLockedFile {
    file: Option<fs::File>,
}
impl ExclusiveLockedFile {
    pub fn create_and_open<P: 'static + AsRef<Path> + Send>(
        path: P,
    ) -> impl Future<Item = Self, Error = io::Error> + Send {
        fs::OpenOptions::new()
            .create_new(true)
            .read(false)
            .write(true)
            .open(path)
            .map(|f| f.into_std())
            .and_then(|f| match f.try_lock_exclusive() {
                Ok(()) => Box::new(future::ok(f))
                    as Box<dyn Future<Item = std::fs::File, Error = io::Error> + Send>,
                Err(_) => Box::new(LockedFileLockFuture::new_exclusive(f)),
            })
            .map(|f| ExclusiveLockedFile {
                file: Some(fs::File::from_std(f)),
            })
    }

    pub fn open<P: 'static + AsRef<Path> + Send>(
        path: P,
    ) -> impl Future<Item = Self, Error = io::Error> + Send {
        fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map(|f| f.into_std())
            .and_then(|f| match f.try_lock_exclusive() {
                Ok(()) => Box::new(future::ok(f))
                    as Box<dyn Future<Item = std::fs::File, Error = io::Error> + Send>,
                Err(_) => Box::new(LockedFileLockFuture::new_exclusive(f)),
            })
            .map(|f| ExclusiveLockedFile {
                file: Some(fs::File::from_std(f)),
            })
    }

    pub fn truncate(self) -> impl Future<Item = Self, Error = io::Error> + Send {
        self.seek(SeekFrom::Current(0))
            .and_then(|(file, pos)| SetLenFuture {
                file: Some(file),
                len: pos,
            })
    }

    pub fn do_shutdown(mut self) -> impl Future<Item = (), Error = io::Error> + Send {
        future::poll_fn(move || self.shutdown())
    }
}

struct SetLenFuture {
    file: Option<ExclusiveLockedFile>,
    len: u64,
}

impl Future for SetLenFuture {
    type Item = ExclusiveLockedFile;
    type Error = io::Error;

    fn poll(&mut self) -> Result<Async<ExclusiveLockedFile>, io::Error> {
        let mut file = None;
        std::mem::swap(&mut file, &mut self.file);

        let mut file = file.expect("tried to poll unfinished future");

        file.file
            .as_mut()
            .expect("tried to poll dropped file")
            .poll_set_len(self.len)
            .map(|a| match a {
                Async::NotReady => {
                    let mut file = Some(file);
                    std::mem::swap(&mut file, &mut self.file);
                    Async::NotReady
                }
                Async::Ready(_) => Async::Ready(file),
            })
    }
}

impl Read for ExclusiveLockedFile {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        self.file
            .as_mut()
            .expect("tried to read from dropped LockedFile")
            .read(buf)
    }
}

impl AsyncRead for ExclusiveLockedFile {}

impl Write for ExclusiveLockedFile {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        self.file
            .as_mut()
            .expect("tried to write to dropped ExclusiveLockedFile")
            .write(buf)
    }

    fn flush(&mut self) -> Result<(), io::Error> {
        self.file
            .as_mut()
            .expect("tried to flush dropped LockedFileWrite")
            .flush()
    }
}

impl AsyncWrite for ExclusiveLockedFile {
    fn shutdown(&mut self) -> Result<Async<()>, io::Error> {
        let result = self
            .file
            .as_mut()
            .expect("tried to shutdown dropped ExclusiveLockedFile")
            .shutdown();

        match result {
            Ok(Async::Ready(())) => {
                let mut file = None;
                std::mem::swap(&mut file, &mut self.file);
                file.unwrap().into_std().unlock().unwrap();
            }
            _ => {}
        };

        result
    }
}

impl Drop for ExclusiveLockedFile {
    fn drop(&mut self) {
        let mut file = None;
        std::mem::swap(&mut file, &mut self.file);
        if file.is_some() {
            // getting here is not really where we want to be.
            // Ideally user code would have called shutdown, which would have made file None.
            // Since we got here, the lock has not yet been cleared, which we do here.
            file.unwrap().into_std().unlock().unwrap();

            // it is a good indicator that we didn't properly close
            // during a write, which is rather important on shared
            // file systems like NFS, which sync and report errors on
            // close.
            // To make that work well, it also needs to report a backtrace - maybe just on nightly.
        }
    }
}

pub trait FutureSeekable: Sized {
    fn seek(self, pos: SeekFrom) -> Box<dyn Future<Item = (Self, u64), Error = io::Error> + Send>;
}

impl FutureSeekable for LockedFile {
    fn seek(
        mut self,
        pos: SeekFrom,
    ) -> Box<dyn Future<Item = (Self, u64), Error = io::Error> + Send> {
        let mut file = None;
        std::mem::swap(&mut file, &mut self.file);
        let file = file.expect("tried to seek in dropped LockedFile");
        Box::new(
            file.seek(pos)
                .map(|(file, pos)| (LockedFile { file: Some(file) }, pos)),
        )
    }
}

impl FutureSeekable for ExclusiveLockedFile {
    fn seek(
        mut self,
        pos: SeekFrom,
    ) -> Box<dyn Future<Item = (Self, u64), Error = io::Error> + Send> {
        let mut file = None;
        std::mem::swap(&mut file, &mut self.file);
        let file = file.expect("tried to seek in dropped ExclusiveLockedFile");
        Box::new(
            file.seek(pos)
                .map(|(file, pos)| (ExclusiveLockedFile { file: Some(file) }, pos)),
        )
    }
}
