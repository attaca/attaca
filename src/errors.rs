//! # `errors` - error-chain generated `Error` types.

use std::path::PathBuf;


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
