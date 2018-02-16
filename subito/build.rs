extern crate capnpc;

fn main() {
    capnpc::CompilerCommand::new()
        .src_prefix("schema")
        .file("schema/cache.capnp")
        .file("schema/digest.capnp")
        .file("schema/state.capnp")
        .run().expect("schema compiler command");
}
