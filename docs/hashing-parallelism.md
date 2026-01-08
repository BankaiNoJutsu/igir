# Hashing parallelism

Igir can compute file checksums in parallel to speed up matching and scanning large collections. Use the `--hash-threads <N>` option to control how many threads are allocated to checksum computation.

- **Default:** number of CPU cores (automatically chosen when the flag is omitted).
- **Constraint:** value must be an integer >= 1 — `0` is rejected.
- **Effect:** Increasing the value uses more CPU (and may increase memory/IO pressure when many large files are processed concurrently). Setting `1` effectively disables parallel hashing.

Example: `igir --hash-threads 4 scan input/`

Tune this value according to your machine and workload — for IO-bound workloads a lower value may be better, while CPU-bound hashing benefits from more threads.
