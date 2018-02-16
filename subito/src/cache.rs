use std::{fs::File, io::{BufRead, Write}, ops::{BitAnd, BitOr, Not}, os::unix::io::AsRawFd,
          path::{Path, PathBuf}, sync::{Arc, RwLock}, time::{SystemTime, UNIX_EPOCH}};

use attaca::{digest::{Digest, Sha3Digest}, path::ObjectPath};
use capnp::{serialize_packed, Word, message::{self, ScratchSpace, ScratchSpaceHeapAllocator}};
use failure::Error;
use leveldb::{database::Database, kv::KV, options::{ReadOptions, WriteOptions}};
use nix::{self, errno::Errno, libc::c_int, sys::stat::{lstat, FileStat}};
use num_traits::FromPrimitive;
use smallvec::SmallVec;

use cache_capnp::*;
use db::Key;

// Although FS_IOC_GETVERSION in linux/fs.h is noted as taking a long, it actually deals with ints:
// https://lwn.net/Articles/576446/
const FS_IOC_GETVERSION_MAGIC: u8 = b'v';
const FS_IOC_GETVERSION_TYPE_MODE: u8 = 1;
ioctl!(read fs_ioc_getversion with FS_IOC_GETVERSION_MAGIC, FS_IOC_GETVERSION_TYPE_MODE; c_int);

fn ns_from_components(seconds: i64, nanos: i64) -> Option<i64> {
    seconds
        .checked_mul(1_000_000_000)
        .and_then(|ns| ns.checked_add(nanos))
}

#[derive(Debug, Clone, Copy)]
enum InodeVersionOrTimes {
    Version(u64),
    Times { ctime_ns: i64, mtime_ns: i64 },
}

#[derive(Debug, Clone, Copy)]
struct Inode {
    timestamp_ns: i64,

    generation: u32,
    number: u64,

    version_or_times: InodeVersionOrTimes,
}

impl Inode {
    fn is_unchanged(lhs: &Self, rhs: &Self) -> Certainty {
        use self::InodeVersionOrTimes::*;

        let same_file =
            Certainty::from(lhs.generation == rhs.generation && lhs.number == rhs.number);
        let unmodified = match (lhs.version_or_times, rhs.version_or_times) {
            (Version(l), Version(r)) => Certainty::from(l == r),
            (
                Times {
                    ctime_ns: l_ctime_ns,
                    mtime_ns: l_mtime_ns,
                },
                Times {
                    ctime_ns: r_ctime_ns,
                    mtime_ns: r_mtime_ns,
                },
            ) => {
                let l_valid = Certainty::positive_or_unknown(
                    l_ctime_ns < lhs.timestamp_ns && l_mtime_ns < lhs.timestamp_ns,
                );
                let r_valid = Certainty::positive_or_unknown(
                    r_ctime_ns < rhs.timestamp_ns && r_mtime_ns < rhs.timestamp_ns,
                );
                l_valid & r_valid
                    & Certainty::from(l_ctime_ns == r_ctime_ns && l_mtime_ns == r_mtime_ns)
            }
            _ => Certainty::Unknown,
        };

        same_file & unmodified
    }

    fn open(path: &Path) -> Result<Option<Self>, Error> {
        let timestamp_ns = {
            let system_time = SystemTime::now().duration_since(UNIX_EPOCH)?;
            ns_from_components(
                system_time.as_secs() as i64,
                system_time.subsec_nanos() as i64,
            ).unwrap()
        };

        let FileStat {
            st_ino,
            st_ctime,
            st_ctime_nsec,
            st_mtime,
            st_mtime_nsec,
            ..
        } = match lstat(path) {
            Ok(file_stat) => file_stat,
            Err(error) => match error {
                nix::Error::Sys(Errno::ENOENT) => return Ok(None),
                other => bail!(other),
            },
        };

        let generation = {
            let file = File::open(path)?;
            let mut generation: c_int = 0;
            unsafe {
                fs_ioc_getversion(file.as_raw_fd(), &mut generation)?;
            }
            generation as u32
        };

        Ok(Some(Self {
            timestamp_ns,

            generation,
            number: st_ino,

            version_or_times: InodeVersionOrTimes::Times {
                ctime_ns: ns_from_components(st_ctime, st_ctime_nsec).unwrap(),
                mtime_ns: ns_from_components(st_mtime, st_mtime_nsec).unwrap(),
            },
        }))
    }
}

#[derive(Debug, Clone, Copy)]
struct Entry {
    maybe_hash: Option<Sha3Digest>,

    inode: Inode,
}

impl Entry {
    fn decode<R: BufRead>(reader: &mut R) -> Result<Self, Error> {
        use self::entry::{self, inode, maybe_hash};

        let message_reader = serialize_packed::read_message(reader, message::ReaderOptions::new())?;
        let entry = message_reader.get_root::<entry::Reader>()?;

        let maybe_hash = match entry.get_maybe_hash().which()? {
            maybe_hash::Some(data) => {
                let bytes = data?;
                ensure!(bytes.len() == Sha3Digest::SIZE, "Bad hash data!");
                Some(Sha3Digest::from_bytes(bytes))
            }
            maybe_hash::None(()) => None,
        };

        let inode = {
            let inode = entry.get_inode();

            let timestamp_ns = inode.get_timestamp_ns();
            let generation = inode.get_generation();
            let number = inode.get_number();

            let version_or_times = match inode.which()? {
                inode::Version(version) => InodeVersionOrTimes::Version(version),
                inode::Times(times) => InodeVersionOrTimes::Times {
                    ctime_ns: times.get_ctime_ns(),
                    mtime_ns: times.get_mtime_ns(),
                },
            };

            Inode {
                timestamp_ns,
                generation,
                number,
                version_or_times,
            }
        };

        Ok(Self { maybe_hash, inode })
    }

    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        let mut scratch_bytes = [0u8; 1024];
        let mut scratch_space = ScratchSpace::new(Word::bytes_to_words_mut(&mut scratch_bytes));
        let mut message = message::Builder::new(ScratchSpaceHeapAllocator::new(&mut scratch_space));

        {
            let mut entry = message.init_root::<entry::Builder>();
            {
                let mut maybe_hash = entry.borrow().get_maybe_hash();
                match self.maybe_hash {
                    Some(hash) => maybe_hash.set_some(hash.as_bytes()),
                    None => maybe_hash.set_none(()),
                }
            }
            {
                let mut inode = entry.borrow().get_inode();
                inode.set_timestamp_ns(self.inode.timestamp_ns);
                inode.set_generation(self.inode.generation);
                inode.set_number(self.inode.number);
                match self.inode.version_or_times {
                    InodeVersionOrTimes::Version(version) => inode.set_version(version),
                    InodeVersionOrTimes::Times { ctime_ns, mtime_ns } => {
                        let mut times = inode.init_times();
                        times.set_ctime_ns(ctime_ns);
                        times.set_mtime_ns(mtime_ns);
                    }
                }
            }
        }

        serialize_packed::write_message(writer, &message)?;

        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Certainty {
    Negative,
    Unknown,
    Positive,
}

impl From<bool> for Certainty {
    fn from(b: bool) -> Self {
        match b {
            true => Certainty::Positive,
            false => Certainty::Negative,
        }
    }
}

impl BitAnd for Certainty {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self::Output {
        use self::Certainty::*;

        match (self, rhs) {
            (Negative, _) | (_, Negative) => Negative,
            (Unknown, _) | (_, Unknown) => Unknown,
            (Positive, Positive) => Positive,
        }
    }
}

impl BitOr for Certainty {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        use self::Certainty::*;

        match (self, rhs) {
            (Positive, _) | (_, Positive) => Negative,
            (Unknown, _) | (_, Unknown) => Unknown,
            (Negative, Negative) => Negative,
        }
    }
}

impl Not for Certainty {
    type Output = Self;

    fn not(self) -> Self::Output {
        use self::Certainty::*;

        match self {
            Positive => Negative,
            Unknown => Unknown,
            Negative => Positive,
        }
    }
}

impl Certainty {
    pub fn positive_or_unknown(b: bool) -> Self {
        match b {
            true => Certainty::Positive,
            false => Certainty::Unknown,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Snapshot {
    path_buf: PathBuf,
    object_path: ObjectPath,

    inode: Inode,
}

#[derive(Debug, Clone)]
pub enum Status {
    Extant(Certainty, Snapshot),
    New(Snapshot),
    Removed,
    Extinct,
}

pub struct Cache {
    db: Arc<RwLock<Database<Key>>>,
}

impl From<Arc<RwLock<Database<Key>>>> for Cache {
    fn from(db: Arc<RwLock<Database<Key>>>) -> Self {
        Self { db }
    }
}

impl Cache {
    pub fn status(&self, path: &ObjectPath) -> Result<Status, Error> {
        let db_lock = self.db.read().unwrap();
        let key = Key::cache(path);
        let path_buf = path.to_path();
        let entry_opt = db_lock.get(ReadOptions::new(), &key)?;
        let inode_opt = Inode::open(&path_buf)?;

        match (entry_opt, inode_opt) {
            (Some(bytes), Some(newest)) => {
                let mut entry = Entry::decode(&mut &bytes[..])?;
                let is_unchanged = Inode::is_unchanged(&entry.inode, &newest);
                Ok(Status::Extant(
                    is_unchanged,
                    Snapshot {
                        path_buf,
                        object_path: path.clone(),
                        inode: newest,
                    },
                ))
            }
            (Some(_), None) => Ok(Status::Removed),
            (None, Some(inode)) => Ok(Status::New(Snapshot {
                path_buf,
                object_path: path.clone(),
                inode,
            })),
            (None, None) => Ok(Status::Extinct),
        }
    }

    pub fn resolve(&self, snapshot: Snapshot, digest: Sha3Digest) -> Result<(), Error> {
        let newest =
            Inode::open(&snapshot.path_buf)?.ok_or_else(|| format_err!("File has been removed!"))?;

        match Inode::is_unchanged(&snapshot.inode, &newest) {
            Certainty::Positive => {
                let mut entry = Entry {
                    maybe_hash: Some(digest),
                    inode: newest,
                };
                let mut buf = SmallVec::<[u8; 1024]>::new();
                entry.encode(&mut buf)?;

                let db_lock = self.db.read().unwrap();
                let key = Key::cache(&snapshot.object_path);
                db_lock.put(WriteOptions::new(), &key, &buf)?;

                Ok(())
            }
            Certainty::Unknown | Certainty::Negative => bail!("File has been changed!"),
        }
    }

    //     pub fn status_mut(&self, path: &ObjectPath) -> Result<Status, Error> {
    //         let db_lock = self.db.write().unwrap();
    //         let key = Key::cache(path);
    //         let path_buf = path.to_path();
    //         let entry_opt = db_lock.get(ReadOptions::new(), &key)?;
    //         let inode_opt = Inode::open(&path_buf)?;
    //
    //         match (entry_opt, inode_opt) {
    //             (Some(bytes), Some(newest)) => {
    //                 let mut entry = Entry::decode(&mut &bytes[..])?;
    //                 let is_unchanged = Inode::is_unchanged(&entry.inode, &newest);
    //
    //                 match is_unchanged {
    //                     Certainty::Positive => {}
    //                     Certainty::Unknown | Certainty::Negative => {
    //                         entry.maybe_hash = None;
    //                         let mut buf = SmallVec::<[u8; 1024]>::new();
    //                         entry.encode(&mut buf)?;
    //                         db_lock.put(WriteOptions::new(), &key, &buf)?;
    //                     }
    //                 }
    //
    //                 Ok(Status::Modified(is_unchanged))
    //             }
    //             (Some(_), None) => Ok(Status::Removed),
    //             (None, Some(inode)) => {
    //                 let entry = Entry {
    //                     maybe_hash: None,
    //                     inode,
    //                 };
    //                 let mut buf = SmallVec::<[u8; 1024]>::new();
    //                 entry.encode(&mut buf)?;
    //                 db_lock.put(WriteOptions::new(), &key, &buf)?;
    //
    //                 Ok(Status::New)
    //             }
    //             (None, None) => Ok(Status::Ghost),
    //         }
    //     }
}
