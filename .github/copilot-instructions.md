## Copilot / AI agent instructions — quick, actionable

This repo contains two parallel implementations: the canonical Node.js CLI (primary) and
an in-progress Rust rewrite at `rust/igir-rs`. Read the Node entrypoint (`index.ts`) and
the main orchestrator (`src/igir.ts`) first to understand the pipeline: scan → candidate
generation → write → report.

Key files to inspect (fast path):
- `index.ts` — CLI bootstrap, signal handling, and option parsing via `src/modules/argumentsParser.ts`.
- `src/igir.ts` — main flow and orchestration of DATs, ROM scanners, candidate generators, and writers.
- `src/modules/*` — per-domain logic (dats, roms, candidates, archives, writer, patching).
- `rust/igir-rs/src/cli.rs`, `config.rs`, `records.rs`, `candidates.rs`, `actions.rs` — Rust parity points; `actions.rs` executes the write plan.

Developer workflows and exact commands (Windows / PowerShell):
```powershell
npm install
npm run build       # builds dist/ via scripts/build.ts
npm start           # runs via ts-node loader (index.ts)
npm test            # runs `test:unit` (Jest) then lint

# Rust tree
cd rust/igir-rs
cargo build
cargo run -- --help
cargo test
```
Notes: Node tests require `--experimental-vm-modules` (see `package.json` `test:unit`). Native addons (zstd, etc.) rely on `node-gyp`/`node-addon-api` and specific Node versions (see `package.json` engines/volta fields).

Project-specific patterns you must follow (concrete):
- Dual-implementation parity: when changing scanning, checksum, or cache behavior, update both Node (`src/types/*`, `src/modules/*`, `records`) and Rust (`rust/igir-rs/src/*`) to keep tests/fixtures comparable.
- CLI flags: add flags in Node `src/modules/argumentsParser.ts` and mirror them in `rust/igir-rs/src/cli.rs` + `config.rs`.
- Output tokens: token resolution lives in Node `types/outputFactory.js` and Rust `records.rs`—update both when adding tokens.
- Archives: ZIP is scanned natively; 7z uses external `7z`/`7za` binaries. Tests skip when 7z is missing — keep this in mind for CI or local runs.

Integration points & external dependencies (practical notes):
- 7z / 7za binary: required for 7z listing/extraction (external dependency).
- Online services: Hasheous and IGDB are opt-in (`--enable-hasheous`, `--igdb-*`). Use `--cache-db` / cache-only modes in Rust for offline runs.
- Native compressors: Node uses `zstd-napi` and other node-addon packages; builds may require proper Python, build tools, and Node versions compatible with `node-gyp`.

Where to look for tasks / examples:
- To add a new CLI option: edit `src/modules/argumentsParser.ts`, update `src/types/options.ts`, then mirror in `rust/igir-rs/src/cli.rs` and `config.rs`.
- To change checksum/scan behavior: start in `src/modules/roms/*` and `src/types/files/*`, then update `rust/igir-rs/src/checksum.rs` / `records.rs`.
- To inspect write actions and cache behavior: `src/modules/candidates/candidateWriter.ts` (Node) and `rust/igir-rs/src/actions.rs` (Rust).

Testing and CI tips:
- Run `npm run test:unit` locally; it invokes Jest with `--experimental-vm-modules`.
- For Rust, `cargo test` runs unit and integration tests. Use `-vv` in Rust for verbose cache/network traces.

If you'd like, I can iterate this file to include PR-sized task breakdowns (e.g., how to add a flag end-to-end, example unit test, and required fixtures). Please tell me which area you'd like expanded.
