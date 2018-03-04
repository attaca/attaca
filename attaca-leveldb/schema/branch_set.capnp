@0xa6bf052c4e0cf470;

struct Branch {
    name @0 :Text;
    hash @1 :Data;
}

struct BranchSet {
    entries @0 :List(Branch);
}
