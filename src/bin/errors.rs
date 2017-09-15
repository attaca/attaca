error_chain! {
    types { Error, ErrorKind, ResultExt, Result; }

    links {
        Attaca(::attaca::Error, ::attaca::ErrorKind);
    }

    foreign_links {
        Nul(::std::ffi::NulError);
        Io(::std::io::Error);
    }

    errors {
        InvalidUsage(args: String) {
            description("invalid usage"),
            display("invalid usage: {}", args),
        }
    }
}
