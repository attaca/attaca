# `attaca` - prototype for a distributed version control system for extremely large quantities of data

This tool is under development and not in working condition!

Install with:

```
cargo install
```

## Commands

```
attaca init                         # Initialize a repository in the current directory.
attaca test chunk   <INPUT>         # Hashsplit a file and print chunk statistics.
attaca test marshal <INPUT>         # Split and marshal a file, and then write its chunks to disk in the local blob store.
attaca test suite noop              # Test the "suite" machinery - with no options this will result in spinning up and then shutting down a local RADOS cluster for testing.
attaca test suite write_all <INPUT> # Test hashsplitting, chunking, and then sending a file into a local RADOS cluster.
```

For more information, try running the above with `--help` or as `attaca help [SUBCOMMAND]`.
