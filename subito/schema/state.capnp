@0xbbef9cd0d77d3105;

using import "digest.capnp".Digest;

struct State {
    digest @0 :Digest;

    candidate :union {
        none @1 :Void;
        some @2 :Data;
    }

    head :union {
        none @3 :Void;
        some @4 :Data;
    }

    activeBranch :union {
        none @5 :Void;
        some @6 :Text;
    }
}
