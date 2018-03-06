@0xa6bb948721df1d8d;

struct ObjectRef {
    bytes @0 :Data;

    kind :union {
        small :group {
            size @1 :UInt64;
        }
        large :group {
            size @2 :UInt64;
            depth @3 :UInt8;
        }
        tree :group { unit @4 :Void; }
        commit :group { unit @5 :Void; }
    }
}
