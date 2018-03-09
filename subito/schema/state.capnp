@0xbbef9cd0d77d3105;

struct Map(Key, Value) {
    entries @0 :List(Entry);

    struct Entry {
        key @0 :Key;
        value @1 :Value;
    }
}

struct State {
    struct RemoteRef {
        remote @0 :Text;
        branch @1 :Text;
    }

    candidate :union {
        none @0 :Void;
        some @1 :Data;
    }

    head :union {
        empty @2 :Void;
        detached @3 :Data;
        branch @4 :Text;
    }

    remoteBranches @5 :Map(Text, Map(Text, Data));
    upstreams @6 :Map(Text, RemoteRef);
}
