# `attaca` - prototype for a distributed version control system for extremely large quantities of data

This is currently a do-nothing tool. It is capable of only one thing: splitting single files into small, deterministic chunks. Installation can be performed like so:

```
cargo install
```

And a file can be chunked with the command:

```
attaca test chunk <FILE>
```

The chunker will not create any output files - it will chunk the file and then display statistics (minimum, average, maximum, and standard deviation) of generated chunk sizes.

This data will be printed to standard output.
