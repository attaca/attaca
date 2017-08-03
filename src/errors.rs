//! `errors` - error-chain generated `Error` types.

error_chain! {
    types { Error, ErrorKind, ResultExt, Result; }

    links {}

    foreign_links {
        Io(::std::io::Error);
    }

    errors {}
}
