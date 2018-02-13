use std::io::prelude::*;
use std::io::Cursor;
use std::mem;
use std::net::TcpStream;
use std::path::Path;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use futures::prelude::*;
use futures::sync::oneshot::{self, Sender as OneshotTx, Receiver as OneshotRx};
use futures::sync::mpsc::{self, UnboundedSender as MpscTx};
use ssh2::Session;

use arc_slice;
use catalog::Catalog;
use errors::*;
use marshal::{ObjectHash, Hashed, Object};
use repository::{Refs, SshCfg};
use store::{ObjectStore, Local};


pub enum SshRequest {
    CompareAndSwapBranch(String, ObjectHash, ObjectHash, OneshotTx<Result<ObjectHash>>),
    GetBranch(String, OneshotTx<Result<ObjectHash>>),

    ReadObject(ObjectHash, OneshotTx<Result<Object>>),
    WriteObject(Hashed, OneshotTx<Result<bool>>),
}


#[derive(Clone)]
pub struct SshObjectStoreHandle {
    sender: MpscTx<SshRequest>,
}


pub struct SshObjectStoreReadObject {
    receiver: OneshotRx<Result<Object>>,
}


impl Future for SshObjectStoreReadObject {
    type Item = Object;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.receiver.poll() {
            Ok(Async::Ready(Ok(object))) => Ok(Async::Ready(object)),
            Ok(Async::Ready(Err(error))) => Err(error),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(_) => bail!("Sender destroyed!"),
        }
    }
}


pub struct SshObjectStoreWriteObject {
    receiver: OneshotRx<Result<bool>>,
}


impl Future for SshObjectStoreWriteObject {
    type Item = bool;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.receiver.poll() {
            Ok(Async::Ready(Ok(fresh))) => Ok(Async::Ready(fresh)),
            Ok(Async::Ready(Err(error))) => Err(error),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(_) => bail!("Sender destroyed!"),
        }
    }
}


impl ObjectStore for SshObjectStoreHandle {
    type Read = SshObjectStoreReadObject;
    type Write = SshObjectStoreWriteObject;

    fn read_object(&self, object_hash: ObjectHash) -> Self::Read {
        let (sender, receiver) = oneshot::channel();
        let request = SshRequest::ReadObject(object_hash, sender);
        match self.sender.unbounded_send(request) {
            Ok(()) => SshObjectStoreReadObject { receiver },
            Err(_) => panic!(),
        }
    }

    fn write_object(&self, hashed: Hashed) -> Self::Write {
        let (sender, receiver) = oneshot::channel();
        let request = SshRequest::WriteObject(hashed, sender);
        match self.sender.unbounded_send(request) {
            Ok(()) => SshObjectStoreWriteObject { receiver },
            Err(_) => panic!(),
        }
    }
}


pub struct SshObjectStoreThread {
    thread_handle: JoinHandle<Result<()>>,
}


impl SshObjectStoreThread {
    pub fn new(
        local: Local,
        remote_catalog: &Catalog,
        ssh_cfg: &SshCfg,
    ) -> Result<(Self, SshObjectStoreHandle)> {
        let address = ssh_cfg.address;
        let username = ssh_cfg.username.clone();
        let (mpsc_tx, mpsc_rx) = mpsc::unbounded();
        let catalog = remote_catalog.clone();

        let thread_handle = thread::spawn(move || {
            let tcp = TcpStream::connect_timeout(&address, Duration::from_secs(5))?;
            let mut sess = Session::new().unwrap();
            sess.handshake(&tcp)?;
            sess.userauth_agent(&username)?;

            for request in mpsc_rx.wait() {
                let send_result = match request.expect("mpsc receiver never errors") {
                    SshRequest::CompareAndSwapBranch(_, _, _, _) => unimplemented!(),
                    SshRequest::GetBranch(branch, oneshot_tx) => {
                        let channel_and_stat_res =
                            sess.scp_recv(Path::new("refs.bin")).chain_err(
                                || ErrorKind::SshError,
                            );

                        let raw_contents_res =
                            channel_and_stat_res.and_then(|(mut channel, stat)| {
                                let mut raw_contents = Vec::with_capacity(stat.size() as usize);
                                channel
                                    .read_to_end(&mut raw_contents)
                                    .map(|_| raw_contents)
                                    .chain_err(|| ErrorKind::SshError)
                            });

                        let refs_res = raw_contents_res.and_then(|raw_contents| {
                            Refs::from_bytes(&mut Cursor::new(raw_contents))
                        });

                        let branch_res =
                            refs_res.and_then(|refs| match refs.branches.get(&branch) {
                                Some(&hash) => Ok(hash),
                                None => bail!("No such branch!"),
                            });

                        oneshot_tx.send(branch_res).or(Err(()))
                    }
                    SshRequest::ReadObject(object_hash, oneshot_tx) => {
                        match local.read_or_allocate_object(object_hash).wait()? {
                            Ok(object) => oneshot_tx.send(Ok(object)).or(Err(())),
                            Err(buffer_factory) => {
                                let path = Path::new("blobs").join(object_hash.to_path());
                                let channel_and_stat_res =
                                    sess.scp_recv(&path).chain_err(|| ErrorKind::SshError);
                                let raw_contents_res =
                                    channel_and_stat_res.and_then(|(mut channel, stat)| {

                                        let mut raw_contents =
                                            Vec::with_capacity(stat.size() as usize);
                                        channel
                                            .read_to_end(&mut raw_contents)
                                            .map(|_| raw_contents)
                                            .chain_err(|| ErrorKind::SshError)
                                    });
                                let object_res = raw_contents_res.and_then(|raw_contents| {
                                    Object::from_bytes(arc_slice::owned(raw_contents))
                                });

                                oneshot_tx.send(object_res).or(Err(()))
                            }
                        }
                    }
                    SshRequest::WriteObject(hashed, oneshot_tx) => {
                        match catalog.try_lock(*hashed.as_hash()) {
                            Ok(lock) => {
                                match hashed.into_components() {
                                    (object_hash, Some(bytes)) => {
                                        let path = Path::new("blobs").join(object_hash.to_path());
                                        let channel_res =
                                            sess.scp_send(&path, 0o644, bytes.len() as u64, None)
                                                .chain_err(|| ErrorKind::SshError);
                                        let write_all_res = channel_res.and_then(|mut channel| {
                                            channel.write_all(&bytes).chain_err(
                                                || ErrorKind::SshError,
                                            )
                                        });

                                        oneshot_tx
                                            .send(write_all_res.map(|_| {
                                                lock.release();
                                                true
                                            }))
                                            .or(Err(()))
                                    }
                                    (object_hash, None) => {
                                        unimplemented!("TODO: Must load local blob!");
                                    }
                                }
                            }
                            Err(future) => {
                                oneshot_tx.send(future.wait().map(|_| false)).or(Err(()))
                            }
                        }
                    }
                };

                if let Err(_) = send_result {
                    bail!("Receiver died!");
                }
            }

            mem::drop(tcp);

            Ok(())
        });

        Ok((
            Self { thread_handle },
            SshObjectStoreHandle { sender: mpsc_tx },
        ))
    }
}
