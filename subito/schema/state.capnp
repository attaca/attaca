@0xbbef9cd0d77d3105;

using import "digest.capnp".Digest;

struct State {
    digest @0 :Digest;

    candidate :union {
        some @5 :Data;
        none @6 :Void;
    }

    head :union {
        some @1 :Data;
        none @2 :Void;
    }

    activeBranch :union {
        some @3 :Text;
        none @4 :Void;
    }
}
