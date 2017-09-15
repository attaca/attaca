# `attaca` - prototype for a distributed version control system for extremely large quantities of data

This tool is under development and not in working condition!

Install with:

```
cargo install
```

## Commands

```
attaca init                 # Initialize a repository in the current directory.
attaca test chunk   <INPUT> # Hashsplit a file and print chunk statistics.
attaca test marshal <INPUT> # Split and marshal a file, and then write its chunks to disk in the local blob store.
```
