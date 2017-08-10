//! Interface with distributed storage - e.g. Ceph/RADOS and etcd.
//!
//! The `store` module has several key pieces of functionality:
//! - Sending/receiving marshaled chunks/encoded files/subtrees/commits to and from the store.
//! - Caching already sent/received blobs.
//! - Coordinating refs with the consensus.
//!
//! As marshaling coalesces into a usable module, it will become more obvious exactly what
//! functionality is required.


use std::collections::HashMap;
use std::ffi::CString;
use std::fmt::Write;
use std::fs::OpenOptions;
use std::io::Cursor;
use std::path::PathBuf;

use bincode;
use colosseum::Arena;
use memmap::{Mmap, Protection};
use rad::RadosConnection;

use errors::Result;
use marshal::{Object, ObjectHash};


#[derive(Clone, Copy)]
pub struct CachedObject<'maps: 'objs, 'objs> {
    serialized: &'maps [u8],
    object: &'objs Object<'maps>,
}


pub struct Cache<'maps: 'objs, 'objs> {
    maps: &'maps Arena<Mmap>,
    objs: &'objs Arena<Object<'maps>>,

    hashmap: HashMap<ObjectHash, CachedObject<'maps, 'objs>>,

    path: PathBuf,
}


impl<'maps: 'objs, 'objs> Cache<'maps, 'objs> {
    fn get_path(&self, object_hash: &ObjectHash) -> PathBuf {
        let mut base = self.path.clone();

        let hash_bytes = object_hash.as_slice();

        base.push(format!("{:02x}", hash_bytes[0]));
        base.push(format!("{:02x}", hash_bytes[1]));

        let mut remaining_bytes = String::new();
        for b in &hash_bytes[2..] {
            write!(remaining_bytes, "{:02x}", b).unwrap();
        }

        base.push(remaining_bytes);

        base
    }


    fn get_mapped_slice(&mut self, size: u64, object_hash: &ObjectHash) -> Result<&'maps mut [u8]> {
        let path = self.get_path(&object_hash);

        assert!(!path.exists());

        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(path)?;
        file.set_len(size)?;

        let mmap = self.maps.alloc(Mmap::open(&file, Protection::ReadWrite)?);
        Ok(unsafe { mmap.as_mut_slice() })
    }


    fn cache_object(&mut self, object: Object) -> Result<CachedObject<'maps, 'objs>> {
        let object_hash = object.hash();
        let slice = self.get_mapped_slice(
            bincode::serialized_size(&object),
            &object_hash,
        )?;

        bincode::serialize_into(&mut Cursor::new(&mut *slice), &object, bincode::Infinite)?;
        let object = bincode::deserialize(&mut &*slice)?;
        let obj_ref = self.objs.alloc(object);
        let cached_object = CachedObject {
            serialized: slice,
            object: obj_ref,
        };
        self.hashmap.insert(object_hash, cached_object);

        Ok(cached_object)
    }


    fn fill_object<F: FnOnce(&mut [u8]) -> Result<()>>(
        &mut self,
        size: u64,
        object_hash: &ObjectHash,
        fill: F,
    ) -> Result<CachedObject<'maps, 'objs>> {
        let slice = self.get_mapped_slice(size, object_hash)?;

        fill(slice)?;

        let object = bincode::deserialize(&mut &*slice)?;
        let obj_ref = self.objs.alloc(object);
        let cached_object = CachedObject {
            serialized: slice,
            object: obj_ref,
        };
        self.hashmap.insert(*object_hash, cached_object);

        Ok(cached_object)
    }


    fn lookup_object(
        &mut self,
        object_hash: &ObjectHash,
    ) -> Result<Option<CachedObject<'maps, 'objs>>> {
        if let Some(&cached_object) = self.hashmap.get(object_hash) {
            return Ok(Some(cached_object));
        }

        let path = self.get_path(object_hash);

        if !path.exists() {
            return Ok(None);
        }

        let mmap = self.maps.alloc(Mmap::open_path(&path, Protection::Read)?);
        let slice = unsafe { mmap.as_slice() };

        let object = bincode::deserialize(slice)?;
        let obj_ref = self.objs.alloc(object);
        let cached_object = CachedObject {
            serialized: slice,
            object: obj_ref,
        };
        self.hashmap.insert(*object_hash, cached_object);

        Ok(Some(cached_object))
    }
}


pub struct Store<'maps: 'objs, 'objs> {
    cache: Cache<'maps, 'objs>,

    conn: RadosConnection,
    pool: CString,
}


impl<'maps: 'objs, 'objs> Store<'maps, 'objs> {
    pub fn read_object(
        &mut self,
        object_hash: &ObjectHash,
    ) -> Result<Option<CachedObject<'maps, 'objs>>> {
        if let Some(object) = self.cache.lookup_object(object_hash)? {
            return Ok(Some(object));
        }

        let mut ctx = self.conn.get_pool_context(&*self.pool)?;
        let object_id = CString::new(object_hash.to_string()).unwrap();

        if !ctx.exists(&*object_id)? {
            return Ok(None);
        } else {
            let size = ctx.stat(&*object_id)?.size;

            let cached_object = self.cache.fill_object(size, object_hash, |buf| {
                ctx.read(&*object_id, buf, 0)?;
                Ok(())
            })?;

            return Ok(Some(cached_object));
        }
    }


    pub fn write_object(&mut self, object: Object) -> Result<CachedObject<'maps, 'objs>> {
        let object_hash = object.hash();

        if let Some(cached_object) = self.cache.lookup_object(&object_hash)? {
            return Ok(cached_object);
        }

        let cached_object = self.cache.cache_object(object)?;

        let mut ctx = self.conn.get_pool_context(&*self.pool)?;
        let object_id = CString::new(object_hash.to_string()).unwrap();

        if !ctx.exists(&*object_id)? {
            ctx.write_full(&*object_id, cached_object.serialized)?;
        }

        return Ok(cached_object);
    }
}


// pub trait Store {
//     fn write_object(&mut self, object: &Object) -> Result<()>;
//     fn read_object<'a>(&mut self, object_hash: &ObjectHash) -> Result<Object<'a>>;
// }
//
//
// pub struct LocalStore {
//     path: PathBuf,
// }
//
//
// impl LocalStore {
//     fn open_hash(&self, object_hash: &ObjectHash) -> Result<File> {
//         let path = {
//             let mut base = self.path.clone();
//
//             let hash_bytes = object_hash.as_slice();
//
//             base.push(format!("{:02x}", hash_bytes[0]));
//             base.push(format!("{:02x}", hash_bytes[1]));
//
//             let mut remaining_bytes = String::new();
//             for b in &hash_bytes[2..] {
//                 write!(remaining_bytes, "{:02x}", b).unwrap();
//             }
//
//             base.push(remaining_bytes);
//
//             base
//         };
//
//         let file = OpenOptions::new()
//             .write(true)
//             .truncate(true)
//             .create(true)
//             .open(path)?;
//
//         Ok(file)
//     }
// }
//
//
// impl Store for LocalStore {
//     fn write_object(&mut self, object: &Object) -> Result<()> {
//         bincode::serialize_into(
//             &mut self.open_hash(&object.hash())?,
//             object,
//             bincode::Infinite,
//         )?;
//
//         Ok(())
//     }
//
//
//     fn read_object<'a>(&mut self, object_hash: &ObjectHash) -> Result<Object<'a>> {
//         let obj = bincode::deserialize_from(&mut self.open_hash(&object_hash)?, bincode::Infinite)?;
//
//         Ok(obj)
//     }
// }
//
//
// pub struct RadosStore {
//     ctx: RadosContext,
// }
//
//
// impl RadosStore {
//     pub fn connect(conf: &RadosCfg) -> Result<RadosStore> {
//         let mut ceph_conf = conf.conf_dir.clone();
//         ceph_conf.push("ceph.conf");
//
//         let mut ceph_keyring = conf.conf_dir.clone();
//         ceph_keyring.push(format!("ceph.{}.keyring", conf.user.to_str().unwrap()));
//
//         let mut connection = RadosConnectionBuilder::with_user(&*conf.user)?
//             .read_conf_file(CString::new(ceph_conf.to_str().unwrap()).unwrap())?
//             .conf_set(
//                 c!("keyring"),
//                 CString::new(ceph_keyring.to_str().unwrap()).unwrap(),
//             )?
//             .connect()?;
//
//         let pool = conf.pool.clone();
//         let ctx = connection.get_pool_context(&*pool)?;
//
//         Ok(RadosStore { ctx })
//     }
//
//
//     fn get_object(&mut self, object_hash: &ObjectHash) -> RadosObject {
//         self.ctx.object(
//             CString::new(object_hash.to_string()).unwrap(),
//         )
//     }
// }
//
//
// impl Store for RadosStore {
//     fn write_object(&mut self, object: &Object) -> Result<()> {
//         bincode::serialize_into(
//             &mut self.get_object(&object.hash()),
//             object,
//             bincode::Infinite,
//         )?;
//
//         Ok(())
//     }
//
//
//     fn read_object<'a>(&mut self, object_hash: &ObjectHash) -> Result<Object<'a>> {
//         let object = bincode::deserialize_from(&mut self.get_object(object_hash), bincode::Infinite)?;
//
//         Ok(object)
//     }
// }
