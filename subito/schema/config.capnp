@0x91db39bb1db80c3f;

using import "digest.capnp".Digest;

struct Store {
    url @0 :Text;
    digest @1 :Digest;

    union {
        levelDb @2 :Void;
        ceph @3 :Void;
    }
}

struct Config {
    store @0 :Store;
}
