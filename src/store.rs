//! Interface with distributed storage - e.g. Ceph/RADOS and etcd.
//!
//! The `store` module has several key pieces of functionality:
//! - Sending/receiving marshaled chunks/encoded files/subtrees/commits to and from the store.
//! - Caching already sent/received blobs.
//! - Coordinating refs with the consensus.
//!
//! As marshaling coalesces into a usable module, it will become more obvious exactly what
//! functionality is required.
