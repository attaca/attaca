error_chain! {
    types { Error, ErrorKind, ResultExt, Result; }

    links {
        Attaca(::attaca::Error, ::attaca::ErrorKind);
    }

    foreign_links {
        Clap(::clap::Error);
        Fmt(::std::fmt::Error);
        GlobSet(::globset::Error);
        Nul(::std::ffi::NulError);
        Io(::std::io::Error);
    }

    errors {
        InvalidUsage {
            description("invalid usage"),
            display("invalid usage"),
        }
    }
}
