extern crate capnpc;

fn main() {
    capnpc::CompilerCommand::new()
        .src_prefix("schema")
        .file("schema/cache.capnp")
        .run().expect("schema compiler command");
}
