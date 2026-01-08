# igir (Node) → igir-rs (Rust) Feature Parity Checklist

This document lists major features from the original Node.js `igir` and the current
status in the Rust rewrite located at `rust/igir-rs`. Each row shows the feature,
the Node implementation status, the igir-rs status, and quick file pointers for where
functionality lives in the Rust tree.

Use this as a checklist when prioritizing parity work.

| Feature | Node (original) | igir-rs (current) | Notes / Rust file pointers |
|---|---:|---|---|
| CLI commands (copy, move, link, extract, zip, playlist, test, dir2dat, fixdat, clean, report) | Full | Implemented | `src/cli.rs`, `src/types.rs`, `src/actions.rs` |
| Input scanning: recursive file discovery, globbing | Full | Implemented | `src/records.rs`, `src/roms/rom_scanner.rs` |
| ZIP archive scanning & extraction | Full | Implemented | `src/archives.rs`, `zip` crate usage |
| 7z archive listing & extraction (via external 7z binary) | Full (external 7z) | Implemented (uses system `7z`) | `src/archives.rs` |
| Checksums: CRC32 / MD5 / SHA1 / SHA256 | Full | Implemented | `src/checksum.rs` |
| Header-aware detection & trimming | Full | Implemented | `src/roms/rom_scanner.rs`; CLI flags in `src/cli.rs` |
| Patch application (.ips, .bps, etc) | Full | Implemented | `src/patch.rs`, `src/patch_apply.rs` |
| DAT parsing and ROM matching (XML DATs) | Full | Implemented (basic) | `src/dat.rs` — `load_dat_roms`, `find_dat_for_record` |
| Hasheous checksum -> metadata lookups (online) | Full | Implemented | `src/dat.rs` (`query_hasheous`) and callers in `src/actions.rs` |
| IGDB name lookups (online) | Full | Implemented | `src/dat.rs` (`query_igdb`), credential persistence in `src/config.rs` |
| Cache (checksums & raw Hasheous JSON) | File-based + behavior | Implemented (SQLite, content-keyed) | `src/cache.rs` — tables: `checksums`, `hasheous` (keyed by SHA256) |
| `--cache-db` explicit DB path | Yes | Implemented | `src/cli.rs` / `src/config.rs` / `src/cache.rs` |
| `--cache-only` (offline mode, no network) | No (Node had `--disable-cache`/`--cache-path`) | Implemented | `src/cli.rs`, `src/config.rs`, enforced in `src/actions.rs` (logs `CACHE-MISS (cache-only)`) |
| Log tokens to show cache/network usage (CACHE-HIT, NET-LOOKUP, CACHE-WRITE) | No (not explicit) | Implemented | `src/actions.rs` prints `CACHE-HIT`, `NET-LOOKUP`, `CACHE-WRITE` |
| Write actions: copy/move/link behaviors and link-mode options | Full | Implemented | `src/actions.rs` (`link_record*`, `move_record*`), `src/types.rs` LinkMode |
| Zip writer / TorrentZip parity (exact headers, EOCD comment) | Full (Node torrentzip package) | Implemented (Rust writer) — parity not fully byte-verified | `src/torrentzip_zip64.rs`, `src/torrentzip.rs` |
| Output path tokens ({datName}, {region}, etc) | Full | Implemented | `src/records.rs` (`resolve_output_path_with_dats`) |
| dir2dat / fixdat output format (XML DAT vs JSON) | XML DAT & fixdat features | Partial (JSON outputs) | `src/actions.rs` (`write_dir2dat` outputs JSON, `write_fixdat` outputs JSON). Node produces DAT XML; Rust currently emits JSON artifacts. |
| Report formats (CSV & JSON) | CSV primary | Partial (JSON) | `src/actions.rs` `write_report` writes JSON; Node default was CSV with formatted timestamp. |
| Trim detection inside archives (`--trim-scan-archives`) | Implemented | CLI flag present, implementation partial | `src/cli.rs` flag exists; scanning logic in `src/archives.rs` / `rom_scanner` needs verification for full parity |
| CHD support & metadata parsing | Native bindings / optional | Partial / Opt-in feature | `Cargo.toml` has optional `libchd` feature; CHD parsing not fully fleshed out — see `chd` optional dep and `rust/igir-rs/src/*` CHD-related code (select modules). |
| Native addon parity (zstd-napi and other node addons) | Node uses native addons | Different approach | Rust uses crates (`zip` + `zstd` features, `rusqlite` bundled). Behavior similar but implementation differs. |
| Interactive progress bars & TTY UX | Rich interactive UI in Node | Implemented | `src/progress.rs` renders `[SCAN]`/`[HASH]` bars plus cache/network spinners; `records.rs` streams jobs so both bars advance concurrently |
| Network retry/backoff policies | Implemented | Implemented (configurable) | `src/dat.rs` uses retry/backoff; `src/config.rs` holds `online_max_retries`, `online_throttle_ms` |
| Tests & fixtures parity | Extensive Node test suite | Substantial, but not 1:1 | `rust/igir-rs/tests/*` and many unit tests under `src/* ::#[cfg(test)]` pass; more end-to-end fixture parity tests may be needed. |
| Credential persistence (IGDB creds) | Node persists config | Implemented | `src/config.rs` saves `igdb_client_id`/`igdb_token` to persisted config path |
| Streaming/extraction performance heuristics | Many Node heuristics | Partial | Some heuristics present; profiling & tuning may be required for parity under heavy workloads. |

## Notes & Recommended Next Steps

- If byte-for-byte ZIP/torrentzip parity is required, add cross-check tests that compare produced archives against Node outputs for representative fixtures (`tests/` + golden files).
- Enable and expand CHD integration via the optional `libchd` feature and add CHD parsing tests.
- If consumers expect DAT XML outputs, add a DAT XML writer to match Node `dir2dat`/`fixdat` format (or document JSON format as canonical for Rust).
- Add an automated test that asserts `--cache-only` suppresses outbound HTTP requests (mock `query_hasheous` via `httpmock` or override hooks already present in `src/dat.rs`).

---

File pointers (quick):

- CLI & config: `src/cli.rs`, `src/config.rs`
- Scanning & records: `src/records.rs`, `src/roms/rom_scanner.rs`, `src/archives.rs`
- Checksums: `src/checksum.rs`
- DAT & online lookups: `src/dat.rs`
- Cache: `src/cache.rs`
- Actions: `src/actions.rs`
- TorrentZip writer: `src/torrentzip_zip64.rs`, `src/torrentzip.rs`
- Tests: `tests/` and `src/*` `#[cfg(test)]` sections

## Unique features (present in only one implementation)

### Node-only features

- Enhanced TTY chrome: while igir-rs now ships SCAN/HASH progress bars, cache spinners, and status panes,
	the Node CLI still includes a more elaborate multi-panel display (per-action panes, live warnings, etc.).
- DAT XML output formats and CSV-centric reporting: Node's `dir2dat`/`fixdat`/`report` workflows
	can produce traditional DAT XML and CSV outputs by default (Rust currently produces JSON artifacts
	for `dir2dat`/`fixdat`/`report`). See Node docs and `README.md` for examples.
- Extensive frontend help examples maintained from `src/modules/argumentsParser.ts` and auto-generated
	help sections—Node has a large help text and curated examples embedded in the help output.
- Some Node native addons: Node uses platform-specific native addons (e.g., `zstd-napi`, other
	node-addons) that behave slightly differently than Rust crate-based implementations.

### Rust-only features

- Content-keyed SQLite cache (SHA256): the Rust cache keys entries by content SHA256 so cache entries
	survive path moves; callable via `--cache-db`. Implemented in `src/cache.rs`.
- `--cache-only` runtime flag and explicit cache/log tokens: Rust implements `--cache-only` to
	suppress network activity and prints `CACHE-HIT`, `NET-LOOKUP`, `CACHE-WRITE`, and
	`CACHE-MISS (cache-only)` tokens for easy auditing (see `src/actions.rs`).
- Single-binary rewrite with JSON execution plan: Rust produces a compact JSON `ExecutionPlan`
	and is distributed as a native binary without Node.js runtime dependencies (`src/types.rs`,
	`src/actions.rs` produce the JSON summary).
- Optional `libchd` Cargo feature stub: Rust exposes an optional `libchd` feature (in `Cargo.toml`)
	to integrate CHD parsing via a crate/binding when enabled.
- Test hooks & overrides for network endpoints: `src/dat.rs` exposes module-level override
	statics (`HASHEOUS_OVERRIDE`, `IGDB_OVERRIDE`) used by tests to inject controlled endpoints.
