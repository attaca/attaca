use attaca::marshal::ObjectHash;


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
        FsckFailure(expected: ObjectHash, actual: ObjectHash) {
            description("an object did not hash to the expected value"),
            display("an object {} did not hash to the expected value {}", actual, expected)
        }
        
        InvalidUsage {
            description("invalid usage"),
            display("invalid usage"),
        }

        NotACommit(hash: ObjectHash) {
            description("not a commit hash"),
            display("{} is not a commit hash", hash),
        }
    }
}
