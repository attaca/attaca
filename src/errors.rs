//! # `errors` - error-chain generated `Error` types.

use std::path::PathBuf;

use marshal::ObjectHash;


error_chain! {
    types { Error, ErrorKind, ResultExt, Result; }

    links {
        Rados(::rad::Error, ::rad::ErrorKind);
    }

    foreign_links {
        Bincode(::bincode::Error);
        Io(::std::io::Error);
        Nul(::std::ffi::NulError);
        ParseInt(::std::num::ParseIntError);
        TomlSer(::toml::ser::Error);
        TomlDe(::toml::de::Error);
    }

    errors {
        Absurd {
            description("this is absurd and should never happen")
            display("this is absurd and should never happen")
        }

        CatalogDeserialize(path: PathBuf) {
            description("could not deserialize catalog")
            display("could not deserialize catalog at path {}", path.display())
        }

        CatalogLoad(path: PathBuf) {
            description("could not load catalog")
            display("could not load catalog at path {}", path.display())
        }

        CatalogNotFound(name_opt: Option<String>) {
            description("catalog not found")
            display("catalog for {} not found", match name_opt {
                &Some(ref name) => format!("remote repository `{}`", name),
                &None => "local repository".to_owned(),
            })
        }

        CatalogOpen(path: PathBuf) {
            description("could not open catalog file")
            display("could not open catalog file at {}", path.display())
        }

        CatalogPoisoned {
            description("an error occurred while filling a catalog entry")
            display("an error occurred while filling a catalog entry")
        }

        ConcurrentlyModifiedEntry {
            description("an entry in the index may have been modified in between index update and cleaning")
            display("an entry in the index may have been modified in between index update and cleaning")
        }

        ConcurrentlyModifiedFile(path: PathBuf) {
            description("file modified while hashing/updating the index, or racily modified before hashing/updating")
            display("file {} modified while hashing/updating the index, or racily modified before hashing/updating", path.display())
        }

        DirTreeDelta {
            description("failure to build a subtree hierarchy")
            display("failure to build a subtree hierarchy")
        }

        EmptyStore {
            description("attempted to write or read an object to/from the empty store")
            display("Attempted to write or read an object to/from the empty store! The empty store always errors when operated upon.")
        }

        IndexOpen {
            description("an error occurred while opening the index file")
            display("an error occurred while opening the index file")
        }

        IndexParse {
            description("an error occurred while deserializing the index file")
            display("an error occurred while deserializing the index file")
        }

        IndexStat(path: PathBuf) {
            description("an error occurred while statting a repository file")
            display("an error occurred while statting file {}", path.display())
        }

        IndexUpdate {
            description("an error occurred while updating the index")
            display("an error occurred while updating the index")
        }

        IndexUpdateUntracked {
            description("attempted to update the index entry for an untracked file")
            display("attempted to update the index entry for an untracked file")
        }

        InvalidHashLength(len: usize) {
            description("expected a string of 64 hex digits")
            display("expected a string of 64 hex digits, found a string of length {}", len)
        }

        InvalidHashString(s: String) {
            description("could not parse string into hash")
            display("could not parse string `{}` into hash", s)
        }

        LocalLoad {
            description("could not load local store")
            display("could not load local store")
        }

        ObjectNotASubtree(hash: ObjectHash) {
            description("expected a subtree, but got a different kind of object")
            display("Expected {} to be a subtree object, but... it wasn't.", hash)
        }

        MalformedSubtree(parent_hash: Option<ObjectHash>, child_hash: ObjectHash) {
            description("subtree contained a non-data, non-subtree object in its entries")
            display("subtree object with hash {:?} contained a non-data, non-subtree object {} in its entries", parent_hash.as_ref().map(ToString::to_string), child_hash)
        }

        RemoteConnect {
            description("could not connect to remote store")
            display("could not connect to remote store")
        }

        RemoteConnectConfig {
            description("could not configure remote connection")
            display("could not configure remote connection")
        }

        RemoteConnectInit {
            description("could not initialize remote connection")
            display("could not initialize remote connection")
        }

        RemoteConnectReadConf {
            description("could not read conf file")
            display("could not read conf file")
        }

        RemoteGetCatalog(name: String) {
            description("could not get catalog")
            display("could not get catalog for remote `{}`", name)
        }

        RemoteNotFound(name: String) {
            description("no such remote")
            display("no such remote `{}`", name)
        }

        RepositoryNotFound(path: PathBuf) {
            description("repository not found")
            display("no repository found in {} or in any parent directory", path.display())
        }
    }
}
