extern crate capnpc;

fn main() {
    capnpc::CompilerCommand::new()
        .src_prefix("schema")
        .file("schema/branch_set.capnp")
        .run()
        .expect("schema compiler command");
}
