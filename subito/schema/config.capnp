@0x91db39bb1db80c3f;

using import "digest.capnp".Digest;

struct Store {
    url @0 :Text;

    union {
        levelDb @1 :Void;
        ceph @2 :Void;
    }
}

struct Remote {
    name @0 :Text;
    store @1 :Store;
}

struct Config {
    store @0 :Store;
    remotes @1 :List(Remote);
}
