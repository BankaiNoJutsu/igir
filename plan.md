# Granular Progress Tracking Plan

## Goals
- Surface byte-level and per-phase progress for every long-running operation in the Rust CLI so users see steady movement instead of stall-prone counters.
- Keep all UI updates single-threaded through `ProgressReporter` while allowing worker threads to emit structured progress events.
- Maintain feature parity with (or exceed) the Node implementation without regressing non-interactive/log-only modes.

## Workstreams

### 1. Core Progress Infrastructure
1. Extend `ProgressReporter` (`rust/igir-rs/src/progress.rs`) with:
   - Per-task byte accounting and new helpers (`begin_item`, `update_item_bytes`, `finish_item`).
   - Optional aggregate byte bars for background tasks (hashing, caching, downloads).
   - Configurable verbosity fallbacks (spinner-only vs bar+detail).
2. Introduce a lightweight `ProgressEvent` struct (path, phase, bytes_done, total, message) for cross-thread communication.
3. Ensure `ProgressReporter::finalize` cleans up any in-flight byte trackers to avoid memory leaks.

### 2. Hashing & Scanning
1. In `collect_files` (`rust/igir-rs/src/records.rs`):
   - Replace `(PathBuf, u64, u64)` channel with `ProgressEvent`.
   - Track per-path totals so `update_checksums_progress` can advance the HASH bar proportionally.
   - Emit completion events from `handle_checksum_result` after `tick_background_task`.
2. Update `compute_checksums_with_header` (`rust/igir-rs/src/checksum.rs`) to accept an optional event sink and send chunk updates rather than only thresholded reports.
3. Consider scanning byte progress (directory traversal) by summing metadata lengths beforehand and ticking a SCAN-bytes bar alongside the file counter.

### 3. Action Framework Enhancements
1. Redesign `run_action_with_progress` (`rust/igir-rs/src/actions.rs`) to:
   - Use an enum channel (`ActionProgress::{ItemDone, ItemBytes, PhaseMsg}`).
   - Drain the channel in a loop that interleaves blocking waits with non-blocking `try_recv` bursts to keep the UI responsive.
   - Pass an `ActionProgressSink` into each worker closure so operations can stream progress.
2. Update `ProgressReporter::begin_action`/`advance_action` to optionally show a bytes bar when totals exist, falling back to the current count-only message otherwise.

### 4. File Copy / Move / Link / Extract
1. Instrument copy/move/link helpers (e.g., `fs::copy`, `std::io::copy`, manual buffer loops) so they report per-chunk bytes via the action sink.
2. For link operations (hard/symlink) where bytes don’t apply, emit instant `ItemDone` to keep counts accurate.
3. Extraction paths in `actions.rs` and `patch_apply.rs` should observe archive entry sizes and emit progress as entries are unpacked.

### 5. Zip/Torrentzip Writers
1. Refactor `torrentzip.rs` and `torrentzip_zip64.rs` to stream bytes instead of loading entire files into memory:
   - Replace `std::io::copy` with manual buffered read/write loops that send `ActionProgress` updates.
   - Emit per-entry completion events so the UI can highlight the currently zipped filename.
2. Thread an optional progress sink through `zip_record`/`zip_records`, and default to `None` for tests and reusable library calls.

### 6. Archive Scanning & Patch Application
1. `archives.rs` (ZIP/7z listings) and `candidate_archive_hasher.rs` should report progress when large archives are parsed or re-hashed, especially for multi-gigabyte sets.
2. The patch pipeline (`patch.rs`, `patch_apply.rs`) can surface per-file patch application progress to reassure users during long IPS/BPS conversions.

### 7. Network & Cache Operations
1. Background net lookups (Hasheous/IGDB) already have a `[NET ]` spinner; extend it with request counts and optional per-request timing.
2. When cache rebuilds copy many rows (`cache.rs`), emit byte-like progress so users see table churn rather than a static status line.

### 8. Diagnostics & Telemetry
1. Reuse the new byte metrics inside the DIAG phase (`actions.rs`) instead of recomputing rates manually. This keeps one source of truth for throughput.
2. Consider exporting structured progress snapshots (for future TUI or JSON logs) once the event model is in place.

## Testing & Validation
- Unit tests for `ProgressReporter` covering byte accumulation, item completion, and finalization.
- Integration test that runs `Action::Zip` on a large temp file with a mock sink to assert monotonic byte updates.
- Smoke test to ensure `--quiet` and non-TTY stderr still behave (progress disabled, no panics when sinks are None).
- Manual verification on Windows/macOS/Linux TTYs to confirm bars redraw smoothly at the chosen refresh rate.

## Rollout Strategy
1. Land infrastructure changes behind a feature flag or config knob (e.g., `--progress-granular`) for early validation.
2. Incrementally instrument operations (hashing → zip → copy → extraction) to keep PRs reviewable.
3. Once stable, enable the granular mode by default and update docs (`docs/advanced/logging.md`, CLI help) to describe the improved progress output.
