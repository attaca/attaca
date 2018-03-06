@0xbbef9cd0d77d3105;

struct Branch {
    name @0 :Text;
    commitId @1 :Data;
}

struct RemoteRefs {
    name @0 :Text;
    branches @1 :List(Branch);
}

struct State {
    candidate :union {
        none @0 :Void;
        some @1 :Data;
    }

    head :union {
        empty @2 :Void;
        detached @3 :Data;
        branch @4 :Text;
    }

    remoteRefs @5 :List(RemoteRefs);
}
