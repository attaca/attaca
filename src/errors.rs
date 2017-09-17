//! `errors` - error-chain generated `Error` types.


error_chain! {
    types { Error, ErrorKind, ResultExt, Result; }

    links {
        Rados(::rad::Error, ::rad::ErrorKind);
    }

    foreign_links {
        Bincode(::bincode::Error);
        Nul(::std::ffi::NulError);
        Io(::std::io::Error);
        TomlSer(::toml::ser::Error);
        TomlDe(::toml::de::Error);
    }

    errors {
        Impossible(never: !) {
            description("This error can never occur.")
            display("This error can literally never happen.")
        }
    }
}
