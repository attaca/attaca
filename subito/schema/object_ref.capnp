@0xa6bb948721df1d8d;

using import "digest.capnp".Digest;

struct ObjectRef {
    digest @0 :Digest;
    bytes @1 :Data;

    kind :union {
        small :group {
            size @2 :UInt64;
        }
        large :group {
            size @3 :UInt64;
            depth @4 :UInt8;
        }
        tree :group { unit @5 :Void; }
        commit :group { unit @6 :Void; }
    }
}
