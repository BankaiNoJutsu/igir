This folder contains a focused Rust reimplementation of the core igir pipeline.

Overview
--------
The Rust rewrite mirrors the Node.js implementation conceptually but centralizes
the execution plan in a compact, fast binary. It implements the same high-level
stages: input scanning → candidate generation → write actions → reporting.

Key goals
- Provide a fast, self-contained CLI for common igir workflows
- Keep parity with Node CLI flags where practical
- Share conceptual modules (dats, rom scanning, candidate generation, actions)
- Produce a JSON execution summary for automation and tests when `--print-plan` is specified (default stdout stays quiet)

Architecture & important files
- `src/cli.rs` — Clap-based argument parsing. Mirrors many Node CLI flags.
- `src/config.rs` — Validates CLI and constructs a runtime `Config` used throughout.
- `src/records.rs`, `src/roms/rom_scanner.rs` — Input scanning and header-aware detection.
- `src/checksum.rs` — CRC32/MD5/SHA1/SHA256 helpers and in-memory checksum sets.
- `src/candidates.rs` / `src/write_candidate.rs` — Candidate generation and write candidate construction.
- `src/dat.rs` — DAT/online lookup helpers (Hasheous + IGDB integration points).
- `src/cache.rs` — SQLite-backed cache for checksums and Hasheous raw JSON (content-keyed).
- `src/actions.rs` — Builds and executes the write plan (copy/move/link/zip/report) and contains cache-read/write + logging.

Cache & offline behavior
- Cache schema (SQLite) uses a content key (SHA256) so cached entries survive path changes.
- Tables: `checksums(key PRIMARY KEY, source, size, crc32, md5, sha1, sha256, updated_at)` and
	`hasheous(key PRIMARY KEY, source, json, updated_at)`.
- New CLI flags:
	- `--cache-db <PATH>`: explicit SQLite database path for the cache.
	- `--cache-only`: runtime-only mode that prevents any network lookups — useful for offline runs.
	- `--hash-threads <N>`: control how many threads the runtime allocates for checksum computation. Defaults to the number of CPU cores when omitted. The value must be an integer >= 1; `1` effectively disables parallel hashing.
	- `--scan-threads <N>`: cap how many concurrent filesystem walkers run while enumerating inputs. The Rayon pool uses the larger of hash vs. scan thread counts so both stages stay saturated without oversubscribing your host.
- Verbosity controls how much of the internal pipeline you see:
	- Default: concise `[SCAN]` style progress bars plus the final summary.
	- `-v`: per-action logs (Copying/Moving/Linking) and error details.
	- `-vv`: cache hits/misses and platform deduction logs (`CACHE-HIT`, `CACHE-MISS`, etc.).
	- `-vvv`: network-level traces (`NET-LOOKUP`, `CACHE-WRITE`) for full diagnostics.

Logging and filtering
- When running with `-vv`/`-vvv`, stderr contains structured tokens that are easy to filter. Example PowerShell filter:

```powershell
Select-String -Path igir_run.log -Pattern 'CACHE-HIT|CACHE-MISS|NET-LOOKUP|CACHE-WRITE' -AllMatches
```

Progress reporting & concurrency
--------------------------------
- `src/progress.rs` drives `[SCAN]` and `[HASH]` bars (plus cache/network spinners) that now mimic the Node CLI UX. Scanning progress updates as soon as files are discovered; checksum progress advances while scanning is still running thanks to streaming job channels.
- `--hash-threads` and `--scan-threads` feed into the global Rayon pool (`src/main.rs`) so heavy checksum runs and IO-bound scans can be tuned independently.
- `records.rs` keeps a bounded queue of “in-flight” checksum jobs. When that queue is full the scanner momentarily blocks to apply backpressure, ensuring the `[HASH]` bar keeps moving instead of waiting until discovery finishes.
- Verbosity flags still control how much detail you see in the “status” pane, but the default experience now includes the concurrent SCAN/HASH progress bars plus background spinners for cache/net activity.

Build / test / run (Windows / PowerShell)
- Build and run locally:

```powershell
cd rust/igir-rs
cargo build
cargo run --bin igir -- --help
```

- Unit tests and integration tests:

```powershell
cd rust/igir-rs
cargo test
```

- Example instrumented copy run that snapshots the hasheous table and saves logs:

```powershell
# snapshot before
sqlite3 "\\SERVER\path\igir_cache.sqlite" "SELECT key || ',' || coalesce(updated_at,0) FROM hasheous;" > pre_hasheous.csv
# run the copy (replace paths)
cargo run --bin igir -- copy -i '\\SERVER\SRC' -o '\\SERVER\DST' --enable-hasheous --cache-db '\\SERVER\path\igir_cache.sqlite' -vv 2>&1 | Tee-Object -FilePath igir_run.log
# snapshot after
sqlite3 "\\SERVER\path\igir_cache.sqlite" "SELECT key || ',' || coalesce(updated_at,0) FROM hasheous;" > post_hasheous.csv
# extract cache/network lines
Select-String -Path igir_run.log -Pattern 'CACHE-HIT|CACHE-MISS|NET-LOOKUP|CACHE-WRITE' -AllMatches | Select-Object -ExpandProperty Line > cache_net_lines.txt
```

Developer notes & conventions
- CLI parity: many flags are intentionally mirrored from the Node.js repo. When adding
	new flags add them in `src/cli.rs` and map in `src/config.rs`.
- When changing cache key behavior, update both `src/cache.rs` (Rust) and the Node side
	so fixtures remain comparable.
- Network integrations (Hasheous, IGDB) are conservative: enable via `--enable-hasheous` and
	provide `--igdb-client-id`/`--igdb-token` as needed. When writing tests, prefer mocking
	network calls in `tests/online_lookup_mock.rs`.

Where to look next
- `src/actions.rs` — best starting point to understand the execution plan and where cache
	reads/writes happen.
- `src/dat.rs` — shows how Hasheous queries are formed and where JSON is consumed.
- `tests/` — multiple integration tests demonstrate parity and edge cases.

If you'd like I can:
- Add a short CONTRIBUTING.md for the Rust tree describing how to add flags and tests.
- Open a PR with this documentation and a short changelog entry.

Tests: run `cargo test` in `rust/igir-rs`.
