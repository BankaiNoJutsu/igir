use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Context;
use glob::glob;
use globset::GlobSet;
use num_cpus;
use regex::Regex;
use walkdir::WalkDir;

use crate::archives::scan_zip_entries;
use crate::checksum::compute_checksums_with_header;
use crate::config::Config;
use crate::game_console;
use crate::progress::{BackgroundTask, ProgressEvent, ProgressReporter};
use crate::roms::{chd, rom_scanner::scan as scan_rom};
use crate::types::{
    ArchiveChecksumMode, DirGameSubdirMode, FileCollection, FileRecord, SkipReason, SkippedFile,
};
use crate::utils::build_globset;
use rayon::prelude::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, TryRecvError};
use std::thread::JoinHandle;
use std::time::Instant;
fn note_scan_progress(
    progress: Option<&ProgressReporter>,
    counter: &mut usize,
    bytes_counter: &mut u64,
    hint: &Path,
    bytes: u64,
) {
    *counter += 1;
    *bytes_counter = bytes_counter.saturating_add(bytes);
    if let Some(p) = progress {
        p.scanning_tick(*counter, *bytes_counter, Some(hint));
    }
}

struct WorkerGuard(Option<JoinHandle<()>>);

impl WorkerGuard {
    fn new(handle: JoinHandle<()>) -> Self {
        Self(Some(handle))
    }

    fn join(mut self) -> std::thread::Result<()> {
        if let Some(handle) = self.0.take() {
            handle.join()
        } else {
            Ok(())
        }
    }
}

struct JobResult {
    path: PathBuf,
    checksums: Option<crate::types::ChecksumSet>,
    size: u64,
    rom_info: Option<crate::roms::rom_scanner::RomInfo>,
    extra_records: Vec<FileRecord>,
}

type ChecksumJobResult = anyhow::Result<JobResult>;

struct Metrics {
    queued: Arc<AtomicUsize>,
    completed: Arc<AtomicUsize>,
    bytes: Arc<AtomicU64>,
}

fn handle_checksum_result(
    received: ChecksumJobResult,
    records: &mut Vec<FileRecord>,
    progress: Option<&ProgressReporter>,
    metrics: Option<&Metrics>,
) -> anyhow::Result<()> {
    let mut job = received?;
    if let Some(ref info) = job.rom_info {
        if info.is_chd {
            if let Ok(Some(chdinfo)) = chd::parse_chd_header(&job.path) {
                if let Some(ref mut checksums) = job.checksums {
                    if chdinfo.sha1.is_some() {
                        checksums.sha1 = chdinfo.sha1;
                    }
                    if chdinfo.md5.is_some() {
                        checksums.md5 = chdinfo.md5;
                    }
                }
            }
        }
    }

    if let Some(checksums) = job.checksums.take() {
        records.push(FileRecord {
            source: job.path.clone(),
            relative: job
                .path
                .file_name()
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("unknown")),
            size: job.size,
            checksums,
            letter_dir: None,
            derived_platform: None,
            derived_genres: Vec::new(),
            derived_region: None,
            derived_languages: Vec::new(),
            scan_info: job.rom_info,
        });

        if let Some(p) = progress {
            p.tick_background_task(BackgroundTask::Checksums, 1, Some(&job.path));
            p.update_checksums_progress(&job.path, job.size, Some(job.size));
            p.finish_background_item(BackgroundTask::Checksums, &job.path);
        }
        if let Some(m) = metrics {
            m.completed.fetch_add(1, Ordering::Relaxed);
            m.bytes.fetch_add(job.size, Ordering::Relaxed);
        }
    } else if let Some(p) = progress {
        // Jobs that only scanned archive contents still count toward the
        // background task so the HASH bar reaches completion.
        p.tick_background_task(BackgroundTask::Checksums, 1, Some(&job.path));
        p.update_checksums_progress(&job.path, job.size, Some(job.size));
        p.finish_background_item(BackgroundTask::Checksums, &job.path);
        if let Some(m) = metrics {
            m.completed.fetch_add(1, Ordering::Relaxed);
            m.bytes.fetch_add(job.size, Ordering::Relaxed);
        }
    }

    if !job.extra_records.is_empty() {
        records.extend(job.extra_records.into_iter());
    }

    Ok(())
}

fn drain_nonblocking_results(
    result_rx: &mpsc::Receiver<ChecksumJobResult>,
    records: &mut Vec<FileRecord>,
    progress: Option<&ProgressReporter>,
    completed_files: &mut usize,
) -> anyhow::Result<()> {
    // This non-blocking drain no longer updates completed_files directly; callers
    // should rely on the metrics object updated by `handle_checksum_result`.
    loop {
        match result_rx.try_recv() {
            Ok(received) => {
                // We don't have the metrics object here, but handle_checksum_result
                // updates the metrics when called by the blocking drain. For the
                // non-blocking case we still apply the result to records so the
                // rest of the pipeline sees completed entries in a timely manner.
                handle_checksum_result(received, records, progress, None)?;
                *completed_files = completed_files.saturating_add(1);
            }
            Err(TryRecvError::Empty) => break,
            Err(TryRecvError::Disconnected) => {
                anyhow::bail!("checksum worker channel closed unexpectedly");
            }
        }
    }
    Ok(())
}

fn wait_for_checksum_result(
    result_rx: &mpsc::Receiver<ChecksumJobResult>,
    records: &mut Vec<FileRecord>,
    progress: Option<&ProgressReporter>,
    completed_files: &mut usize,
) -> anyhow::Result<()> {
    match result_rx.recv() {
        Ok(received) => {
            handle_checksum_result(received, records, progress, None)?;
            *completed_files = completed_files.saturating_add(1);
            Ok(())
        }
        Err(e) => Err(anyhow::anyhow!("checksum worker terminated: {e}")),
    }
}

fn maybe_apply_backpressure(
    jobs_enqueued: usize,
    completed_files: &mut usize,
    max_in_flight: usize,
    result_rx: &mpsc::Receiver<ChecksumJobResult>,
    records: &mut Vec<FileRecord>,
    progress: Option<&ProgressReporter>,
) -> anyhow::Result<bool> {
    if jobs_enqueued.saturating_sub(*completed_files) < max_in_flight {
        return Ok(false);
    }
    wait_for_checksum_result(result_rx, records, progress, completed_files)?;
    Ok(true)
}

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        if let Some(handle) = self.0.take() {
            let _ = handle.join();
        }
    }
}

fn expand_inputs(raw_inputs: &[PathBuf]) -> anyhow::Result<Vec<PathBuf>> {
    let mut expanded = Vec::new();
    for input in raw_inputs {
        if has_glob(input) {
            for entry in glob(input.to_string_lossy().as_ref())? {
                expanded.push(entry?);
            }
        } else {
            expanded.push(input.clone());
        }
    }
    Ok(expanded)
}

fn count_total_files_and_bytes(
    inputs: &[PathBuf],
    exclude: &Option<GlobSet>,
) -> anyhow::Result<(usize, u64)> {
    let mut total = 0usize;
    let mut bytes = 0u64;

    for path in inputs {
        let metadata = fs::metadata(path).with_context(|| format!("reading input: {path:?}"))?;

        if metadata.is_file() {
            if exclude
                .as_ref()
                .is_some_and(|set| set.is_match(path.to_string_lossy().as_ref()))
            {
                continue;
            }
            total += 1;
            bytes = bytes.saturating_add(metadata.len());
            continue;
        }

        if metadata.is_dir() {
            for entry in WalkDir::new(path)
                .into_iter()
                .filter_map(Result::ok)
                .filter(|e| e.file_type().is_file())
            {
                let entry_path = entry.path();
                if exclude
                    .as_ref()
                    .is_some_and(|set| set.is_match(entry_path.to_string_lossy().as_ref()))
                {
                    continue;
                }
                total += 1;
                let entry_metadata = fs::metadata(entry_path)
                    .with_context(|| format!("reading input: {entry_path:?}"))?;
                bytes = bytes.saturating_add(entry_metadata.len());
            }
        }
    }

    Ok((total, bytes))
}

pub fn collect_files(
    config: &Config,
    progress: Option<&ProgressReporter>,
) -> anyhow::Result<FileCollection> {
    let exclude = build_globset(&config.input_exclude)?;
    let mut records = Vec::new();
    let mut scanned_total = 0usize;
    let mut scanned_bytes = 0u64;

    let expanded_inputs = expand_inputs(&config.input)?;
    let (total_count, total_bytes) = count_total_files_and_bytes(&expanded_inputs, &exclude)?;
    let total_hint = if total_count > 0 {
        Some(total_count)
    } else {
        None
    };
    let total_bytes_hint = if total_bytes > 0 {
        Some(total_bytes)
    } else {
        None
    };

    if let Some(p) = progress {
        p.begin_scanning(expanded_inputs.len(), total_hint, total_bytes_hint);
        p.hint_background_task_total(BackgroundTask::Checksums, total_hint);
        p.hint_background_task_bytes(BackgroundTask::Checksums, total_bytes_hint);
    }
    // First pass: discover input files and prepare jobs for checksum computation.
    #[derive(Clone, Copy)]
    enum JobKind {
        RegularFile,
        Archive { compute_archive_checksum: bool },
    }

    impl JobKind {
        fn should_hash(&self) -> bool {
            match self {
                JobKind::RegularFile => true,
                JobKind::Archive {
                    compute_archive_checksum,
                } => *compute_archive_checksum,
            }
        }

        fn is_archive(&self) -> bool {
            matches!(self, JobKind::Archive { .. })
        }
    }

    #[derive(Clone)]
    struct Job {
        path: PathBuf,
        size: u64,
        rom_info: Option<crate::roms::rom_scanner::RomInfo>,
        kind: JobKind,
    }

    let (result_tx, result_rx) = mpsc::channel::<ChecksumJobResult>();
    let (progress_tx, progress_rx) = mpsc::channel::<ProgressEvent>();
    let (job_tx, job_rx) = mpsc::channel::<Job>();
    let worker_config = config.clone();
    let worker_handle = WorkerGuard::new(std::thread::spawn(move || {
        job_rx.into_iter().par_bridge().for_each(|job| {
            let sender = result_tx.clone();
            let progress_sender = progress_tx.clone();
            let res: ChecksumJobResult = (|| {
                let Job {
                    path,
                    size,
                    rom_info,
                    kind,
                } = job;

                let mut extra_records = Vec::new();
                if kind.is_archive() {
                    let mut inner = scan_zip_entries(
                        &path,
                        &worker_config,
                        Some(progress_sender.clone()),
                    )?;
                    for record in inner.iter_mut() {
                        record.source = path.clone();
                    }
                    extra_records.extend(inner);
                }

                let checksums = if kind.should_hash() {
                    let progress_clone = progress_sender.clone();
                    if let Some(ref info) = rom_info {
                        Some(compute_checksums_with_header(
                            &path,
                            &worker_config,
                            info.header_size,
                            Some(progress_clone),
                        )?)
                    } else {
                        Some(compute_checksums_with_header(
                            &path,
                            &worker_config,
                            None,
                            Some(progress_clone),
                        )?)
                    }
                } else {
                    None
                };

                Ok(JobResult {
                    path,
                    checksums,
                    size,
                    rom_info,
                    extra_records,
                })
            })();
            let _ = sender.send(res);
        });
    }));
    let mut worker_handle = Some(worker_handle);

    let mut jobs_enqueued = 0usize;
    let mut completed_files = 0usize;
    let mut worker_err: Option<anyhow::Error> = None;
    let default_threads = num_cpus::get();
    let hash_parallelism = config.hash_threads.unwrap_or(default_threads).max(1);
    let scan_parallelism = config.scan_threads.unwrap_or(default_threads).max(1);
    // Allow a deep queue of checksum jobs so network I/O stays saturated. We still
    // cap the backlog to avoid unbounded memory growth, but the limit is high
    // enough that scanning rarely blocks unless hashes are extremely slow.
    let max_in_flight = hash_parallelism
        .saturating_mul(32)
        .max(scan_parallelism.saturating_mul(8))
        .max(512);

    // Diagnostics metrics shared between scanner and workers
    let metrics = Metrics {
        queued: Arc::new(AtomicUsize::new(0)),
        completed: Arc::new(AtomicUsize::new(0)),
        bytes: Arc::new(AtomicU64::new(0)),
    };

    // Maintain last-seen counters for DIAG rate computations. We update the
    // DIAG progress bar from the main scanning loop (within
    // `drain_progress_updates`) so we don't need a separate thread and can
    // safely call into the `ProgressReporter` instance.
    let mut last_diag_instant = Instant::now();
    let mut last_diag_completed = metrics.completed.load(Ordering::Relaxed);
    let mut last_diag_bytes = metrics.bytes.load(Ordering::Relaxed);
    let mut last_diag_scanned_total = scanned_total;
    let mut last_diag_scanned_bytes = scanned_bytes;

    let mut drain_progress_updates =
        |scanned_total_snapshot: usize, scanned_bytes_snapshot: u64| {
            loop {
                match progress_rx.try_recv() {
                    Ok(event) => {
                        if let Some(p) = progress {
                            p.handle_event(event);
                        }
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => break,
                }
            }

            // Update DIAG progress bar at ~2s intervals if a progress reporter is
            // available.
            if config.diag {
                if let Some(p) = progress {
                    let now = Instant::now();
                    let elapsed = now.duration_since(last_diag_instant);
                    if elapsed.as_secs_f64() >= 2.0 {
                        let now_completed = metrics.completed.load(Ordering::Relaxed);
                        let now_bytes = metrics.bytes.load(Ordering::Relaxed);
                        let queued = metrics.queued.load(Ordering::Relaxed);
                        let in_flight = queued.saturating_sub(now_completed);
                        let completed_delta = now_completed.saturating_sub(last_diag_completed);
                        let bytes_delta = now_bytes.saturating_sub(last_diag_bytes);
                        // Also consider scanning progress when hashes haven't completed yet.
                        let scanned_delta =
                            scanned_total_snapshot.saturating_sub(last_diag_scanned_total) as u64;
                        let scanned_bytes_delta =
                            scanned_bytes_snapshot.saturating_sub(last_diag_scanned_bytes);
                        let secs = elapsed.as_secs_f64();
                        let files_per_sec = if completed_delta > 0 {
                            (completed_delta as f64) / secs.max(1e-6)
                        } else {
                            (scanned_delta as f64) / secs.max(1e-6)
                        };
                        let mib_per_sec = if bytes_delta > 0 {
                            (bytes_delta as f64) / secs.max(1e-6) / 1024.0 / 1024.0
                        } else {
                            (scanned_bytes_delta as f64) / secs.max(1e-6) / 1024.0 / 1024.0
                        };
                        p.update_diag(queued, in_flight, files_per_sec, mib_per_sec);
                        last_diag_instant = now;
                        last_diag_completed = now_completed;
                        last_diag_bytes = now_bytes;
                        last_diag_scanned_total = scanned_total_snapshot;
                        last_diag_scanned_bytes = scanned_bytes_snapshot;
                    }
                }
            }
        };

    'scan: for matched in expanded_inputs {
        drain_progress_updates(scanned_total, scanned_bytes);
        if let Err(e) =
            drain_nonblocking_results(&result_rx, &mut records, progress, &mut completed_files)
        {
            worker_err = Some(e);
            break 'scan;
        }
        let metadata =
            fs::metadata(&matched).with_context(|| format!("reading input: {matched:?}"))?;
        if metadata.is_file() {
            if exclude
                .as_ref()
                .is_some_and(|set| set.is_match(matched.to_string_lossy().as_ref()))
            {
                continue;
            }
            let file_size = metadata.len();
            note_scan_progress(
                progress,
                &mut scanned_total,
                &mut scanned_bytes,
                &matched,
                file_size,
            );

            if let Some(ext) = matched.extension().and_then(|s| s.to_str()) {
                if ext.eq_ignore_ascii_case("zip") {
                    let compute_archive_checksum =
                        config.input_checksum_archives != ArchiveChecksumMode::Never;
                    if job_tx
                        .send(Job {
                            path: matched.clone(),
                            size: file_size,
                            rom_info: None,
                            kind: JobKind::Archive {
                                compute_archive_checksum,
                            },
                        })
                        .is_err()
                    {
                        worker_err = Some(anyhow::anyhow!(
                            "checksum worker stopped while queuing jobs"
                        ));
                        break 'scan;
                    }
                    jobs_enqueued = jobs_enqueued.saturating_add(1);
                    metrics.queued.fetch_add(1, Ordering::Relaxed);
                    drain_progress_updates(scanned_total, scanned_bytes);
                    if let Err(e) = drain_nonblocking_results(
                        &result_rx,
                        &mut records,
                        progress,
                        &mut completed_files,
                    ) {
                        worker_err = Some(e);
                        break 'scan;
                    }
                    match maybe_apply_backpressure(
                        jobs_enqueued,
                        &mut completed_files,
                        max_in_flight,
                        &result_rx,
                        &mut records,
                        progress,
                    ) {
                        Ok(true) => {
                            drain_progress_updates(scanned_total, scanned_bytes);
                        }
                        Ok(false) => {}
                        Err(e) => {
                            worker_err = Some(e);
                            break 'scan;
                        }
                    }
                    continue;
                }
            }

            let rom_info = scan_rom(&matched).ok();
            if job_tx
                .send(Job {
                    path: matched.clone(),
                    size: file_size,
                    rom_info: rom_info.clone(),
                    kind: JobKind::RegularFile,
                })
                .is_err()
            {
                worker_err = Some(anyhow::anyhow!(
                    "checksum worker stopped while queuing jobs"
                ));
                break 'scan;
            }
            jobs_enqueued = jobs_enqueued.saturating_add(1);
            metrics.queued.fetch_add(1, Ordering::Relaxed);
            drain_progress_updates(scanned_total, scanned_bytes);
            if let Err(e) =
                drain_nonblocking_results(&result_rx, &mut records, progress, &mut completed_files)
            {
                worker_err = Some(e);
                break 'scan;
            }
            match maybe_apply_backpressure(
                jobs_enqueued,
                &mut completed_files,
                max_in_flight,
                &result_rx,
                &mut records,
                progress,
            ) {
                Ok(true) => {
                    drain_progress_updates(scanned_total, scanned_bytes);
                }
                Ok(false) => {}
                Err(e) => {
                    worker_err = Some(e);
                    break 'scan;
                }
            }
            continue;
        }

        for entry in WalkDir::new(&matched)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| e.file_type().is_file())
        {
            let path = entry.into_path();
            drain_progress_updates(scanned_total, scanned_bytes);
            if let Err(e) =
                drain_nonblocking_results(&result_rx, &mut records, progress, &mut completed_files)
            {
                worker_err = Some(e);
                break 'scan;
            }
            if exclude
                .as_ref()
                .is_some_and(|set| set.is_match(path.to_string_lossy().as_ref()))
            {
                continue;
            }

            let file_size = fs::metadata(&path)
                .with_context(|| format!("reading input: {path:?}"))?
                .len();

            note_scan_progress(
                progress,
                &mut scanned_total,
                &mut scanned_bytes,
                &path,
                file_size,
            );

            let rom_info = scan_rom(&path).ok();

            if let Some(ext) = path.extension().and_then(|s| s.to_str()) {
                if ext.eq_ignore_ascii_case("zip") {
                    let compute_archive_checksum =
                        config.input_checksum_archives != ArchiveChecksumMode::Never;
                    if job_tx
                        .send(Job {
                            path: path.clone(),
                            size: file_size,
                            rom_info: None,
                            kind: JobKind::Archive {
                                compute_archive_checksum,
                            },
                        })
                        .is_err()
                    {
                        worker_err = Some(anyhow::anyhow!(
                            "checksum worker stopped while queuing jobs"
                        ));
                        break 'scan;
                    }
                    jobs_enqueued = jobs_enqueued.saturating_add(1);
                    metrics.queued.fetch_add(1, Ordering::Relaxed);
                    drain_progress_updates(scanned_total, scanned_bytes);
                    if let Err(e) = drain_nonblocking_results(
                        &result_rx,
                        &mut records,
                        progress,
                        &mut completed_files,
                    ) {
                        worker_err = Some(e);
                        break 'scan;
                    }
                    match maybe_apply_backpressure(
                        jobs_enqueued,
                        &mut completed_files,
                        max_in_flight,
                        &result_rx,
                        &mut records,
                        progress,
                    ) {
                        Ok(true) => {
                            drain_progress_updates(scanned_total, scanned_bytes);
                        }
                        Ok(false) => {}
                        Err(e) => {
                            worker_err = Some(e);
                            break 'scan;
                        }
                    }
                    continue;
                }
            }

            if job_tx
                .send(Job {
                    path: path.clone(),
                    size: file_size,
                    rom_info: rom_info.clone(),
                    kind: JobKind::RegularFile,
                })
                .is_err()
            {
                worker_err = Some(anyhow::anyhow!(
                    "checksum worker stopped while queuing jobs"
                ));
                break 'scan;
            }
            jobs_enqueued = jobs_enqueued.saturating_add(1);
            drain_progress_updates(scanned_total, scanned_bytes);
            if let Err(e) =
                drain_nonblocking_results(&result_rx, &mut records, progress, &mut completed_files)
            {
                worker_err = Some(e);
                break 'scan;
            }
            match maybe_apply_backpressure(
                jobs_enqueued,
                &mut completed_files,
                max_in_flight,
                &result_rx,
                &mut records,
                progress,
            ) {
                Ok(true) => {
                    drain_progress_updates(scanned_total, scanned_bytes);
                }
                Ok(false) => {}
                Err(e) => {
                    worker_err = Some(e);
                    break 'scan;
                }
            }
        }
    }

    // Do not finish scanning here — we still need to report progress
    // while processing checksum jobs. `finish_scanning` will be called
    // after checksum results are applied so the SCAN and HASH bars
    // reflect actual progress.

    drop(job_tx);

    if worker_err.is_none() {
        while completed_files < jobs_enqueued {
            drain_progress_updates(scanned_total, scanned_bytes);
            match result_rx.recv() {
                Ok(received) => {
                    if let Err(e) =
                        handle_checksum_result(received, &mut records, progress, Some(&metrics))
                    {
                        worker_err = Some(e);
                        break;
                    }
                    completed_files = completed_files.saturating_add(1);
                }
                Err(e) => {
                    worker_err = Some(anyhow::anyhow!("checksum worker terminated: {e}"));
                    break;
                }
            }
        }
    }

    drain_progress_updates(scanned_total, scanned_bytes);

    if let Some(err) = worker_err {
        if let Some(handle) = worker_handle.take() {
            handle
                .join()
                .map_err(|e| anyhow::anyhow!("checksum worker panicked: {e:?}"))?;
        }
        return Err(err);
    }

    if let Some(handle) = worker_handle.take() {
        handle
            .join()
            .map_err(|e| anyhow::anyhow!("checksum worker panicked: {e:?}"))?;
    }

    if let Some(p) = progress {
        p.finish_scanning(scanned_total);
        // Also finish the diagnostics background task if present.
        p.finish_background_task(crate::progress::BackgroundTask::Diag);
    }

    let mut skipped = Vec::new();
    records = apply_filters(records, config, &mut skipped)?;

    annotate_locale_metadata(&mut records);

    if config.dir_letter {
        assign_letter_dirs(&mut records, config)?;
    }

    Ok(FileCollection { records, skipped })
}

#[cfg(test)]
mod chd_integration_tests {
    use super::*;
    use std::io::Write;
    use std::path::PathBuf;
    use tempfile::NamedTempFile;

    #[test]
    fn collect_files_uses_chd_checksums_when_present() {
        // create a temporary CHD-like file containing ASCII SHA1 and MD5 tokens in header
        let mut f = NamedTempFile::new().unwrap();
        // CHD magic
        f.write_all(b"MCHD").unwrap();
        // embed an ASCII sha1 (40 hex) and md5 (32 hex)
        let sha1 = "0123456789abcdef0123456789abcdef01234567"; // 40 chars
        let md5 = "0123456789abcdef0123456789abcdef"; // 32 chars
        f.write_all(b" ").unwrap();
        f.write_all(sha1.as_bytes()).unwrap();
        f.write_all(b" ").unwrap();
        f.write_all(md5.as_bytes()).unwrap();
        f.flush().unwrap();

        // Build minimal config with this file as input
        let cfg = crate::config::Config {
            commands: vec![],
            input: vec![PathBuf::from(f.path())],
            input_exclude: vec![],
            input_checksum_quick: false,
            input_checksum_min: crate::types::Checksum::Crc32,
            input_checksum_max: None,
            input_checksum_archives: crate::types::ArchiveChecksumMode::Auto,
            dat: vec![],
            dat_exclude: vec![],
            dat_name_regex: None,
            dat_name_regex_exclude: None,
            dat_description_regex: None,
            dat_description_regex_exclude: None,
            dat_combine: false,
            dat_ignore_parent_clone: false,
            list_unmatched_dats: false,
            print_plan: true,
            enable_hasheous: false,
            igdb_client_id: None,
            igdb_client_secret: None,
            igdb_token: None,
            igdb_token_expires_at: None,
            igdb_mode: crate::types::IgdbLookupMode::BestEffort,
            cache_only: false,
            cache_db: None,
            hash_threads: None,
            scan_threads: None,
            show_match_reasons: false,
            patch: vec![],
            patch_exclude: vec![],
            output: None,
            dir_mirror: false,
            dir_dat_mirror: false,
            dir_dat_name: false,
            dir_dat_description: false,
            dir_letter: false,
            dir_letter_count: None,
            dir_letter_limit: None,
            dir_letter_group: false,
            dir_game_subdir: crate::types::DirGameSubdirMode::Multiple,
            fix_extension: crate::types::FixExtensionMode::Auto,
            overwrite: false,
            overwrite_invalid: false,
            move_delete_dirs: crate::types::MoveDeleteDirsMode::Auto,
            clean_exclude: vec![],
            clean_backup: None,
            clean_dry_run: false,
            zip_format: crate::types::ZipFormat::Torrentzip,
            zip_exclude: None,
            zip_dat_name: false,
            link_mode: crate::types::LinkMode::Hardlink,
            symlink_relative: false,
            header: None,
            remove_headers: None,
            trimmed_glob: None,
            trim_scan_archives: false,
            merge_roms: crate::types::MergeMode::Fullnonmerged,
            merge_discs: false,
            exclude_disks: false,
            allow_excess_sets: false,
            allow_incomplete_sets: false,
            filter_regex: None,
            filter_regex_exclude: None,
            filter_language: None,
            filter_region: None,
            filter_category_regex: None,
            no_bios: false,
            no_device: false,
            no_unlicensed: false,
            only_retail: false,
            no_debug: false,
            no_demo: false,
            no_beta: false,
            no_sample: false,
            no_prototype: false,
            no_program: false,
            verbose: 0,
            quiet: 0,
            diag: false,
            online_timeout_secs: Some(5),
            online_max_retries: Some(3),
            online_throttle_ms: None,
        };

        let files = collect_files(&cfg, None).unwrap();
        assert_eq!(files.records.len(), 1);
        let rec = &files.records[0];
        assert_eq!(rec.checksums.sha1.as_deref(), Some(sha1));
        assert_eq!(rec.checksums.md5.as_deref(), Some(md5));
    }

    #[test]
    fn streaming_checksums_handles_large_files() {
        // create a temporary large file (several megabytes) to exercise streaming reads
        let mut f = NamedTempFile::new().unwrap();
        let size: usize = 2 * 1024 * 1024; // 2 MiB
        let buf = vec![0x5Au8; 64 * 1024]; // 64 KiB chunk
        let mut written = 0usize;
        while written < size {
            let to_write = std::cmp::min(buf.len(), size - written);
            f.write_all(&buf[..to_write]).unwrap();
            written += to_write;
        }
        f.flush().unwrap();

        let cfg = crate::config::Config {
            commands: vec![],
            input: vec![PathBuf::from(f.path())],
            input_exclude: vec![],
            input_checksum_quick: false,
            input_checksum_min: crate::types::Checksum::Sha1,
            input_checksum_max: None,
            input_checksum_archives: crate::types::ArchiveChecksumMode::Auto,
            dat: vec![],
            dat_exclude: vec![],
            dat_name_regex: None,
            dat_name_regex_exclude: None,
            dat_description_regex: None,
            dat_description_regex_exclude: None,
            dat_combine: false,
            dat_ignore_parent_clone: false,
            list_unmatched_dats: false,
            print_plan: true,
            enable_hasheous: false,
            igdb_client_id: None,
            igdb_client_secret: None,
            igdb_token: None,
            igdb_token_expires_at: None,
            igdb_mode: crate::types::IgdbLookupMode::BestEffort,
            cache_only: false,
            cache_db: None,
            hash_threads: None,
            scan_threads: None,
            show_match_reasons: false,
            patch: vec![],
            patch_exclude: vec![],
            output: None,
            dir_mirror: false,
            dir_dat_mirror: false,
            dir_dat_name: false,
            dir_dat_description: false,
            dir_letter: false,
            dir_letter_count: None,
            dir_letter_limit: None,
            dir_letter_group: false,
            dir_game_subdir: crate::types::DirGameSubdirMode::Multiple,
            fix_extension: crate::types::FixExtensionMode::Auto,
            overwrite: false,
            overwrite_invalid: false,
            move_delete_dirs: crate::types::MoveDeleteDirsMode::Auto,
            clean_exclude: vec![],
            clean_backup: None,
            clean_dry_run: false,
            zip_format: crate::types::ZipFormat::Torrentzip,
            zip_exclude: None,
            zip_dat_name: false,
            link_mode: crate::types::LinkMode::Hardlink,
            symlink_relative: false,
            header: None,
            remove_headers: None,
            trimmed_glob: None,
            trim_scan_archives: false,
            merge_roms: crate::types::MergeMode::Fullnonmerged,
            merge_discs: false,
            exclude_disks: false,
            allow_excess_sets: false,
            allow_incomplete_sets: false,
            filter_regex: None,
            filter_regex_exclude: None,
            filter_language: None,
            filter_region: None,
            filter_category_regex: None,
            no_bios: false,
            no_device: false,
            no_unlicensed: false,
            only_retail: false,
            no_debug: false,
            no_demo: false,
            no_beta: false,
            no_sample: false,
            no_prototype: false,
            no_program: false,
            verbose: 0,
            quiet: 0,
            diag: false,
            online_timeout_secs: Some(5),
            online_max_retries: Some(3),
            online_throttle_ms: None,
        };

        let files = collect_files(&cfg, None).unwrap();
        assert_eq!(files.records.len(), 1);
        let rec = &files.records[0];
        assert_eq!(rec.size as usize, size);
        // ensure a SHA1 was computed (input_checksum_min was Sha1)
        assert!(rec.checksums.sha1.is_some());
    }
}

fn assign_letter_dirs(records: &mut [FileRecord], config: &Config) -> anyhow::Result<()> {
    let mut letter_to_indices: HashMap<String, Vec<usize>> = HashMap::new();

    for (idx, record) in records.iter().enumerate() {
        let key = letter_key(record, config);
        letter_to_indices.entry(key).or_default().push(idx);
    }

    let limit = config.dir_letter_limit;
    let grouped = if config.dir_letter_group {
        let Some(limit) = limit else {
            anyhow::bail!("dir-letter-group requires dir-letter-limit to be set");
        };
        if limit == 0 {
            anyhow::bail!("dir-letter-limit must be greater than zero");
        }

        let mut letters: Vec<String> = letter_to_indices.keys().cloned().collect();
        letters.sort();

        let mut map = HashMap::new();
        for chunk in letters.chunks(limit) {
            if let (Some(first), Some(last)) = (chunk.first(), chunk.last()) {
                let label = format!("{}-{}", first, last);
                for letter in chunk {
                    map.insert(letter.clone(), label.clone());
                }
            }
        }
        map
    } else {
        HashMap::new()
    };

    for (letter, indices) in letter_to_indices {
        if let Some(limit) = limit {
            if limit == 0 {
                anyhow::bail!("dir-letter-limit must be greater than zero");
            }

            if !config.dir_letter_group && indices.len() > limit {
                for (chunk_idx, chunk) in indices.chunks(limit).enumerate() {
                    let label = format!("{}{}", letter, chunk_idx + 1);
                    for idx in chunk {
                        records[*idx].letter_dir = Some(label.clone());
                    }
                }
                continue;
            }
        }

        let label = grouped
            .get(&letter)
            .cloned()
            .unwrap_or_else(|| letter.clone());
        for idx in indices {
            records[idx].letter_dir = Some(label.clone());
        }
    }

    Ok(())
}

fn letter_key(record: &FileRecord, config: &Config) -> String {
    let count = config.dir_letter_count.unwrap_or(1);
    let relative_str = record.relative.to_string_lossy();
    let candidate = record
        .relative
        .parent()
        .and_then(|p| p.to_str())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| {
            record
                .relative
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or(relative_str.as_ref())
        });

    let mut key = candidate
        .chars()
        .take(count)
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '#'
            }
        })
        .collect::<String>();

    while key.len() < count {
        key.push('A');
    }

    if !config.dir_letter_group {
        key = key
            .chars()
            .map(|c| if c.is_ascii_alphabetic() { c } else { '#' })
            .collect();
    }

    if key.is_empty() {
        "_MISC".to_string()
    } else {
        key
    }
}

fn apply_filters(
    records: Vec<FileRecord>,
    config: &Config,
    skipped: &mut Vec<SkippedFile>,
) -> anyhow::Result<Vec<FileRecord>> {
    let mut filtered = records;

    filtered = filter_by_regex(filtered, config, skipped)?;
    filtered = filter_by_region_and_language(filtered, config, skipped);

    Ok(filtered)
}

fn filter_by_regex(
    records: Vec<FileRecord>,
    config: &Config,
    skipped: &mut Vec<SkippedFile>,
) -> anyhow::Result<Vec<FileRecord>> {
    if config.filter_regex.is_none() && config.filter_regex_exclude.is_none() {
        return Ok(records);
    }

    let include = config
        .filter_regex
        .as_ref()
        .map(|r| Regex::new(r))
        .transpose()?;
    let exclude = config
        .filter_regex_exclude
        .as_ref()
        .map(|r| Regex::new(r))
        .transpose()?;

    let mut kept = Vec::new();

    for record in records {
        let name = record.relative.to_string_lossy().to_string();
        let included = include.as_ref().map_or(true, |regex| regex.is_match(&name));
        if !included {
            skipped.push(SkippedFile {
                path: record.relative.clone(),
                reason: SkipReason::RegexInclude,
                detail: config
                    .filter_regex
                    .as_ref()
                    .map(|pat| format!("record '{name}' did not match include regex '{pat}'")),
            });
            continue;
        }

        let excluded = exclude
            .as_ref()
            .map_or(false, |regex| regex.is_match(&name));
        if excluded {
            skipped.push(SkippedFile {
                path: record.relative.clone(),
                reason: SkipReason::RegexExclude,
                detail: config
                    .filter_regex_exclude
                    .as_ref()
                    .map(|pat| format!("record '{name}' matched exclude regex '{pat}'")),
            });
            continue;
        }

        kept.push(record);
    }

    Ok(kept)
}

#[derive(Clone)]
struct CandidateRecord {
    record: FileRecord,
    region: Option<String>,
    languages: Vec<String>,
    // title field unused in current logic; keep for future use
    #[allow(dead_code)]
    title: String,
    quality: QualityInfo,
    revision: RevisionRank,
    set_info: Option<SetInfo>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TagDelimiter {
    Parenthesis,
    Bracket,
}

#[derive(Clone, Debug)]
struct TagSegment {
    value: String,
    delimiter: TagDelimiter,
}

#[derive(Clone, Debug)]
struct SetInfo {
    number: u32,
    label: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QualityTier {
    Verified,
    Fixed,
    Pending,
    Clean,
    Modified,
    Bad,
}

impl QualityTier {
    fn rank(&self) -> u8 {
        match self {
            QualityTier::Verified => 0,
            QualityTier::Fixed => 1,
            QualityTier::Pending => 2,
            QualityTier::Clean => 3,
            QualityTier::Modified => 4,
            QualityTier::Bad => 5,
        }
    }
}

#[derive(Clone, Debug)]
struct QualityInfo {
    tier: QualityTier,
    source: Option<String>,
}

impl Default for QualityInfo {
    fn default() -> Self {
        Self {
            tier: QualityTier::Clean,
            source: None,
        }
    }
}

#[derive(Clone, Debug)]
struct RevisionRank {
    priority: u8,
    score: u32,
    label: Option<String>,
}

impl Default for RevisionRank {
    fn default() -> Self {
        Self {
            priority: u8::MAX,
            score: u32::MAX,
            label: None,
        }
    }
}

impl RevisionRank {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.priority, self.score).cmp(&(other.priority, other.score))
    }
}

#[derive(Clone, Debug)]
struct VariantMeta {
    region: Option<String>,
    languages: Vec<String>,
    quality: QualityInfo,
    revision: RevisionRank,
    set_info: Option<SetInfo>,
    region_rank: usize,
    language_rank: usize,
    matched_language: Option<String>,
}

fn filter_by_region_and_language(
    records: Vec<FileRecord>,
    config: &Config,
    skipped: &mut Vec<SkippedFile>,
) -> Vec<FileRecord> {
    let region_preferences = parse_list(config.filter_region.as_deref());
    let language_preferences = parse_list(config.filter_language.as_deref());

    if region_preferences.is_empty() && language_preferences.is_empty() {
        return records;
    }

    let mut grouped: HashMap<String, Vec<CandidateRecord>> = HashMap::new();

    for record in records {
        let name = record
            .relative
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or_default();
        let tags = extract_tags(name);
        let region = detect_region(&tags);
        let languages = detect_languages(&tags);
        let quality = detect_quality(&tags);
        let revision = detect_revision(&tags);
        let set_info = detect_set_info(&tags);
        let title = normalize_title(name);

        grouped
            .entry(title.clone())
            .or_default()
            .push(CandidateRecord {
                record,
                region,
                languages,
                title,
                quality,
                revision,
                set_info,
            });
    }

    let mut kept = Vec::new();

    for (_title, mut candidates) in grouped {
        candidates
            .sort_by(|a, b| compare_candidates(a, b, &region_preferences, &language_preferences));

        let mut kept_variant: Option<VariantMeta> = None;

        for cand in candidates {
            let region_rank = preference_rank(cand.region.as_deref(), &region_preferences);
            let lang_rank = language_rank(&cand.languages, &language_preferences);
            let matched_lang = matched_language(&cand.languages, &language_preferences);

            let meta = VariantMeta {
                region: cand.region.clone(),
                languages: cand.languages.clone(),
                quality: cand.quality.clone(),
                revision: cand.revision.clone(),
                set_info: cand.set_info.clone(),
                region_rank,
                language_rank: lang_rank,
                matched_language: matched_lang,
            };

            let mut detail = None;
            let region_match = region_rank < region_preferences.len();
            let language_match = lang_rank < language_preferences.len();

            let acceptable = preferences_satisfied(
                region_match,
                language_match,
                !region_preferences.is_empty(),
                !language_preferences.is_empty(),
            );

            if acceptable && kept_variant.is_none() {
                kept_variant = Some(meta.clone());
                kept.push(cand.record);
                continue;
            }

            if acceptable {
                if let Some(best) = &kept_variant {
                    detail = build_skip_detail(&meta, best).or_else(|| {
                        Some("another variant provided a closer region/language match".to_string())
                    });
                }
            } else {
                let region_info = meta.region.clone().unwrap_or_else(|| "unknown".to_string());
                let lang_info = if meta.languages.is_empty() {
                    "unknown".to_string()
                } else {
                    meta.languages.join(", ")
                };
                detail = Some(format!(
                    "region {region_info}, languages {lang_info} not in preferences (regions={:?}, languages={:?})",
                    region_preferences, language_preferences
                ));
            }

            skipped.push(SkippedFile {
                path: cand.record.relative.clone(),
                reason: SkipReason::RegionLanguage,
                detail,
            });
        }
    }

    kept
}

fn preferences_satisfied(
    region_match: bool,
    language_match: bool,
    has_region_pref: bool,
    has_language_pref: bool,
) -> bool {
    match (has_region_pref, has_language_pref) {
        (false, false) => true,
        (true, false) => region_match,
        (false, true) => language_match,
        (true, true) => region_match || language_match,
    }
}

fn compare_candidates(
    a: &CandidateRecord,
    b: &CandidateRecord,
    region_preferences: &[String],
    language_preferences: &[String],
) -> std::cmp::Ordering {
    let region_rank_a = preference_rank(a.region.as_deref(), region_preferences);
    let region_rank_b = preference_rank(b.region.as_deref(), region_preferences);

    if region_rank_a != region_rank_b {
        return region_rank_a.cmp(&region_rank_b);
    }

    let lang_rank_a = language_rank(&a.languages, language_preferences);
    let lang_rank_b = language_rank(&b.languages, language_preferences);

    if lang_rank_a != lang_rank_b {
        return lang_rank_a.cmp(&lang_rank_b);
    }

    let quality_cmp = a.quality.tier.rank().cmp(&b.quality.tier.rank());
    if quality_cmp != std::cmp::Ordering::Equal {
        return quality_cmp;
    }

    let revision_cmp = a.revision.cmp(&b.revision);
    if revision_cmp != std::cmp::Ordering::Equal {
        return revision_cmp;
    }

    let set_cmp = compare_set_info(a.set_info.as_ref(), b.set_info.as_ref());
    if set_cmp != std::cmp::Ordering::Equal {
        return set_cmp;
    }

    a.record.relative.cmp(&b.record.relative)
}

fn compare_set_info(a: Option<&SetInfo>, b: Option<&SetInfo>) -> std::cmp::Ordering {
    match (a, b) {
        (Some(a), Some(b)) => a.number.cmp(&b.number),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

fn preference_rank(value: Option<&str>, preferences: &[String]) -> usize {
    value
        .and_then(|v| preferences.iter().position(|pref| pref == v))
        .unwrap_or(preferences.len())
}

fn language_rank(languages: &[String], preferences: &[String]) -> usize {
    preferences
        .iter()
        .position(|pref| languages.iter().any(|lang| lang == pref))
        .unwrap_or(preferences.len())
}

fn matched_language(languages: &[String], preferences: &[String]) -> Option<String> {
    for pref in preferences {
        if languages.iter().any(|lang| lang == pref) {
            return Some(pref.clone());
        }
    }
    None
}

fn parse_list(raw: Option<&str>) -> Vec<String> {
    raw.map(|r| {
        r.split(',')
            .map(|entry| entry.trim().to_uppercase())
            .filter(|entry| !entry.is_empty())
            .collect()
    })
    .unwrap_or_default()
}

fn extract_tags(name: &str) -> Vec<TagSegment> {
    let mut tags = Vec::new();
    let mut current = String::new();
    let mut stack: Vec<TagDelimiter> = Vec::new();

    for ch in name.chars() {
        match ch {
            '(' => {
                if stack.is_empty() {
                    current.clear();
                }
                stack.push(TagDelimiter::Parenthesis);
            }
            '[' => {
                if stack.is_empty() {
                    current.clear();
                }
                stack.push(TagDelimiter::Bracket);
            }
            ')' => {
                if let Some(TagDelimiter::Parenthesis) = stack.pop() {
                    if stack.is_empty() && !current.trim().is_empty() {
                        tags.push(TagSegment {
                            value: current.trim().to_string(),
                            delimiter: TagDelimiter::Parenthesis,
                        });
                        current.clear();
                    }
                } else {
                    stack.clear();
                    current.clear();
                }
            }
            ']' => {
                if let Some(TagDelimiter::Bracket) = stack.pop() {
                    if stack.is_empty() && !current.trim().is_empty() {
                        tags.push(TagSegment {
                            value: current.trim().to_string(),
                            delimiter: TagDelimiter::Bracket,
                        });
                        current.clear();
                    }
                } else {
                    stack.clear();
                    current.clear();
                }
            }
            _ => {
                if !stack.is_empty() {
                    current.push(ch);
                }
            }
        }
    }

    tags
}

pub(crate) fn normalize_title(name: &str) -> String {
    let mut clean = String::new();
    let mut depth = 0usize;

    for ch in name.chars() {
        match ch {
            '(' | '[' => depth += 1,
            ')' | ']' => {
                if depth > 0 {
                    depth -= 1;
                }
            }
            _ => {
                if depth == 0 {
                    clean.push(ch);
                }
            }
        }
    }

    clean
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_string()
}

fn detect_region(tags: &[TagSegment]) -> Option<String> {
    for tag in tags {
        for token in tag_tokens(&tag.value) {
            if let Some(region) = normalize_region_token(&token) {
                return Some(region.to_string());
            }
        }
    }

    None
}

fn detect_languages(tags: &[TagSegment]) -> Vec<String> {
    let mut langs = Vec::new();

    for tag in tags {
        for token in tag_tokens(&tag.value) {
            if let Some(lang) = normalize_language_token(&token) {
                let lang = lang.to_string();
                if !langs.contains(&lang) {
                    langs.push(lang);
                }
            }
        }
    }

    langs
}

fn normalize_region_token(token: &str) -> Option<&'static str> {
    match token {
        "EUROPE" | "EURO" | "EUR" | "EU" => Some("EUR"),
        "FR" | "FRANCE" | "GERMANY" | "SPAIN" | "ITALY" | "NETHERLANDS" | "BELGIUM"
        | "PORTUGAL" | "SWEDEN" | "NORWAY" | "FINLAND" | "DENMARK" | "POLAND" | "CZECH"
        | "CZECHOSLOVAKIA" | "HUNGARY" | "KINGDOM" | "UK" | "ENGLAND" | "SCOTLAND" | "IRELAND"
        | "WALES" => Some("EUR"),
        "USA" | "US" | "AMERICA" | "STATES" | "NORTHAMERICA" => Some("USA"),
        "CANADA" | "MEXICO" => Some("USA"),
        "WORLD" | "GLOBAL" | "INTERNATIONAL" => Some("WORLD"),
        _ => None,
    }
}

fn normalize_language_token(token: &str) -> Option<&'static str> {
    match token {
        "EN" | "ENG" | "ENGLISH" | "UK" | "BRITISH" | "AMERICAN" | "USA" | "US" | "STATES" => {
            Some("EN")
        }
        "FR" | "FRE" | "FRENCH" | "FRANCE" | "FRA" => Some("FR"),
        "DE" | "GER" | "GERMAN" | "GERMANY" => Some("DE"),
        "ES" | "SPA" | "SPANISH" | "SPAIN" | "ESP" => Some("ES"),
        "IT" | "ITA" | "ITALIAN" | "ITALY" => Some("IT"),
        "PT" | "POR" | "PORTUGUESE" | "PORTUGAL" | "BRAZIL" | "BRA" => Some("PT"),
        "DA" | "DAN" | "DANISH" | "DENMARK" => Some("DA"),
        "FI" | "FIN" | "FINNISH" | "FINLAND" => Some("FI"),
        "EL" | "ELL" | "GRE" | "GREEK" | "GREECE" | "GR" => Some("EL"),
        "JA" | "JPN" | "JAP" | "JAPANESE" | "JAPAN" => Some("JA"),
        "KO" | "KOR" | "KOREAN" | "KOREA" => Some("KO"),
        "NL" | "DUT" | "DUTCH" | "NETHERLANDS" | "HOLLAND" => Some("NL"),
        "NO" | "NOR" | "NORWEGIAN" | "NORWAY" => Some("NO"),
        "RU" | "RUS" | "RUSSIAN" | "RUSSIA" => Some("RU"),
        "SV" | "SWE" | "SWEDISH" | "SWEDEN" => Some("SV"),
        "ZH" | "CH" | "CHN" | "CHINESE" | "CHINA" | "MANDARIN" => Some("ZH"),
        _ => None,
    }
}

fn detect_quality(tags: &[TagSegment]) -> QualityInfo {
    let mut fixed = None;
    let mut pending = None;
    let mut modified = None;
    let mut bad = None;

    for tag in tags {
        let normalized = tag.value.trim().to_ascii_uppercase();
        if normalized.is_empty() {
            continue;
        }

        if tag.delimiter == TagDelimiter::Bracket {
            if normalized == "!" {
                return QualityInfo {
                    tier: QualityTier::Verified,
                    source: Some(format_tag(tag)),
                };
            }

            if normalized == "!P" {
                if pending.is_none() {
                    pending = Some(format_tag(tag));
                }
                continue;
            }

            let first = normalized.chars().next().unwrap_or_default();
            match first {
                'F' => {
                    if fixed.is_none() {
                        fixed = Some(format_tag(tag));
                    }
                }
                'B' => {
                    if bad.is_none() {
                        bad = Some(format_tag(tag));
                    }
                }
                'H' | 'P' | 'T' | 'O' | 'A' | 'U' => {
                    if modified.is_none() {
                        modified = Some(format_tag(tag));
                    }
                }
                _ => {}
            }
        } else {
            if normalized.contains("BETA")
                || normalized.contains("PROTO")
                || normalized.contains("ALPHA")
                || normalized.contains("SAMPLE")
                || normalized.contains("DEMO")
                || normalized.contains("TRIAL")
            {
                if modified.is_none() {
                    modified = Some(format_tag(tag));
                }
            }
        }
    }

    if let Some(source) = bad {
        return QualityInfo {
            tier: QualityTier::Bad,
            source: Some(source),
        };
    }

    if let Some(source) = fixed {
        return QualityInfo {
            tier: QualityTier::Fixed,
            source: Some(source),
        };
    }

    if let Some(source) = pending {
        return QualityInfo {
            tier: QualityTier::Pending,
            source: Some(source),
        };
    }

    if let Some(source) = modified {
        return QualityInfo {
            tier: QualityTier::Modified,
            source: Some(source),
        };
    }

    QualityInfo::default()
}

fn detect_revision(tags: &[TagSegment]) -> RevisionRank {
    let mut best = RevisionRank::default();

    for tag in tags {
        let normalized = tag.value.trim().to_ascii_uppercase();
        if normalized.is_empty() {
            continue;
        }

        if let Some(value) = parse_program_revision(&normalized) {
            let candidate = RevisionRank {
                priority: 0,
                score: u32::MAX - value,
                label: Some(format_tag(tag)),
            };
            if candidate.cmp(&best) == std::cmp::Ordering::Less {
                best = candidate;
            }
            continue;
        }

        if let Some((major, minor, patch)) = parse_version_components(&normalized) {
            let combined = (major << 20) | (minor << 10) | patch;
            let candidate = RevisionRank {
                priority: 1,
                score: u32::MAX - combined,
                label: Some(format_tag(tag)),
            };
            if candidate.cmp(&best) == std::cmp::Ordering::Less {
                best = candidate;
            }
            continue;
        }

        if let Some(value) = parse_rev_number(&normalized) {
            let candidate = RevisionRank {
                priority: 2,
                score: u32::MAX - value,
                label: Some(format_tag(tag)),
            };
            if candidate.cmp(&best) == std::cmp::Ordering::Less {
                best = candidate;
            }
            continue;
        }

        if let Some(value) = parse_rev_letter(&normalized) {
            let candidate = RevisionRank {
                priority: 3,
                score: u32::MAX - value,
                label: Some(format_tag(tag)),
            };
            if candidate.cmp(&best) == std::cmp::Ordering::Less {
                best = candidate;
            }
        }
    }

    best
}

fn detect_set_info(tags: &[TagSegment]) -> Option<SetInfo> {
    for tag in tags {
        if let Some(number) = parse_set_number(&tag.value) {
            return Some(SetInfo {
                number,
                label: format_tag(tag),
            });
        }
    }
    None
}

fn parse_program_revision(input: &str) -> Option<u32> {
    let rest = input.strip_prefix("PRG")?;
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        None
    } else {
        digits.parse().ok()
    }
}

fn parse_version_components(input: &str) -> Option<(u32, u32, u32)> {
    let trimmed = input.trim();
    let upper = trimmed.to_ascii_uppercase();
    if !(upper.starts_with('V') || upper.starts_with("VERSION")) {
        return None;
    }
    let idx = trimmed.find(|c: char| c.is_ascii_digit())?;
    let numeric = &trimmed[idx..];
    let mut parts = numeric.split(|c| c == '.' || c == '_' || c == '-');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
    let patch = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
    Some((major, minor, patch))
}

fn parse_rev_number(input: &str) -> Option<u32> {
    let rest = input.strip_prefix("REV")?;
    let digits: String = rest
        .chars()
        .skip_while(|c| !c.is_ascii_digit())
        .take_while(|c| c.is_ascii_digit())
        .collect();
    if digits.is_empty() {
        None
    } else {
        digits.parse().ok()
    }
}

fn parse_rev_letter(input: &str) -> Option<u32> {
    if !input.starts_with("REV") {
        return None;
    }
    let rest = input
        .trim_start_matches(|c: char| c == 'R' || c == 'E' || c == 'V' || c == '.' || c == ' ');
    let letter = rest
        .chars()
        .find(|c| c.is_ascii_alphabetic())?
        .to_ascii_uppercase();
    if !('A'..='Z').contains(&letter) {
        return None;
    }
    Some((letter as u8 - b'A' + 1) as u32)
}

fn parse_set_number(input: &str) -> Option<u32> {
    let upper = input.to_ascii_uppercase();
    let pos = upper.find("SET")?;
    let remainder = upper[pos + 3..].trim();
    let digits: String = remainder
        .chars()
        .skip_while(|c| !c.is_ascii_digit())
        .take_while(|c| c.is_ascii_digit())
        .collect();
    if digits.is_empty() {
        None
    } else {
        digits.parse().ok()
    }
}

fn format_tag(tag: &TagSegment) -> String {
    let trimmed = tag.value.trim();
    match tag.delimiter {
        TagDelimiter::Parenthesis => format!("({trimmed})"),
        TagDelimiter::Bracket => format!("[{trimmed}]"),
    }
}

fn build_skip_detail(candidate: &VariantMeta, winner: &VariantMeta) -> Option<String> {
    use std::cmp::Ordering;

    let mut parts = Vec::new();

    if winner.region_rank < candidate.region_rank {
        let winner_region = winner
            .region
            .clone()
            .unwrap_or_else(|| "unknown".to_string());
        let candidate_region = candidate
            .region
            .clone()
            .unwrap_or_else(|| "unknown".to_string());
        parts.push(format!(
            "preferred region {winner_region} over {candidate_region}"
        ));
    }

    if winner.language_rank < candidate.language_rank {
        let winner_lang = winner
            .matched_language
            .clone()
            .or_else(|| winner.languages.first().cloned())
            .unwrap_or_else(|| "unknown".to_string());
        let candidate_lang = candidate
            .matched_language
            .clone()
            .or_else(|| candidate.languages.first().cloned())
            .unwrap_or_else(|| "unknown".to_string());
        parts.push(format!(
            "preferred language {winner_lang} over {candidate_lang}"
        ));
    }

    if winner.quality.tier.rank() < candidate.quality.tier.rank() {
        parts.push(format!(
            "preferred {} over {}",
            describe_quality(&winner.quality),
            describe_quality(&candidate.quality)
        ));
    }

    match winner.revision.cmp(&candidate.revision) {
        Ordering::Less => {
            if let Some(win_label) = &winner.revision.label {
                if let Some(cand_label) = &candidate.revision.label {
                    parts.push(format!(
                        "preferred newer revision {win_label} over {cand_label}"
                    ));
                } else {
                    parts.push(format!("preferred newer revision {win_label}"));
                }
            }
        }
        _ => {}
    }

    match compare_set_info(winner.set_info.as_ref(), candidate.set_info.as_ref()) {
        Ordering::Less => {
            if let Some(win_set) = &winner.set_info {
                if let Some(cand_set) = &candidate.set_info {
                    parts.push(format!(
                        "preferred {} over {}",
                        win_set.label, cand_set.label
                    ));
                } else {
                    parts.push(format!(
                        "preferred {} with explicit set information",
                        win_set.label
                    ));
                }
            }
        }
        Ordering::Greater => {
            if let Some(cand_set) = &candidate.set_info {
                parts.push(format!(
                    "skipped variant noted as {} in favor of unnamed set",
                    cand_set.label
                ));
            }
        }
        Ordering::Equal => {}
    }

    if parts.is_empty() {
        None
    } else {
        Some(parts.join("; "))
    }
}

fn describe_quality(info: &QualityInfo) -> String {
    match info.tier {
        QualityTier::Verified => {
            format!("verified good {}", info.source.as_deref().unwrap_or("[!]"))
        }
        QualityTier::Fixed => format!("fixed dump {}", info.source.as_deref().unwrap_or("[f]")),
        QualityTier::Pending => format!(
            "pending verification {}",
            info.source.as_deref().unwrap_or("[!p]")
        ),
        QualityTier::Clean => "untagged dump".to_string(),
        QualityTier::Modified => {
            if let Some(src) = &info.source {
                format!("modified variant {src}")
            } else {
                "modified variant".to_string()
            }
        }
        QualityTier::Bad => format!("bad dump {}", info.source.as_deref().unwrap_or("[b]")),
    }
}

fn tag_tokens(tag: &str) -> Vec<String> {
    tag.split(|c: char| !c.is_ascii_alphabetic())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_uppercase())
        .collect()
}

fn has_glob(path: &Path) -> bool {
    let path_str = path.to_string_lossy();
    path_str.contains('*') || path_str.contains('?') || path_str.contains('[')
}

pub fn resolve_output_path(record: &FileRecord, config: &Config) -> PathBuf {
    resolve_output_path_with_dats(record, config, None)
}

/// Resolve output path, optionally using DAT entries to prefer DAT-derived RomM tokens.
pub fn resolve_output_path_with_dats(
    record: &FileRecord,
    config: &Config,
    dats: Option<&[crate::dat::DatRom]>,
) -> PathBuf {
    let mut base = config
        .output
        .as_ref()
        .cloned()
        .unwrap_or_else(|| PathBuf::from("output"));

    // Expand known output tokens in the base path (e.g. {romm}). Replace
    // `{platform}` and `{romm}` directly with the resolved platform token
    // (preferring DAT-derived mapping when available). Previously the code
    // inserted an intermediate `romm/library/<token>` structure when the
    // user included a `{romm}` token; to match expected behavior we now
    // expand `{romm}` to the platform token directly so `-o <dst>/{romm}`
    // yields `<dst>/<platform>/...`.
    if base.to_string_lossy().contains("{romm}") || base.to_string_lossy().contains("{platform}") {
        let token = record
            .derived_platform
            .clone()
            .or_else(|| game_console::romm_for_record(record, dats))
            .unwrap_or_else(|| get_romm_for_filename(&record.relative));
        let replaced = base
            .to_string_lossy()
            .replace("{platform}", &token)
            .replace("{romm}", &token);
        base = PathBuf::from(replaced);
    }

    if base.to_string_lossy().contains("{genre}") {
        let replacement = resolve_genre_token(record);
        base = PathBuf::from(base.to_string_lossy().replace("{genre}", &replacement));
    }

    if base.to_string_lossy().contains("{region}") {
        let replacement = resolve_region_token(record);
        base = PathBuf::from(base.to_string_lossy().replace("{region}", &replacement));
    }

    if base.to_string_lossy().contains("{language}") {
        let replacement = resolve_language_token(record);
        base = PathBuf::from(base.to_string_lossy().replace("{language}", &replacement));
    }

    if config.dir_mirror {
        if let Some(parent) = record.relative.parent() {
            base = base.join(parent);
        }
    }

    if config.dir_letter {
        if let Some(letter) = &record.letter_dir {
            base = base.join(letter);
        }
    }

    // Replace console-specific tokens like {es}, {batocera}, {mister}, {pocket}, etc.
    if base.to_string_lossy().contains('{') {
        let mut base_str = base.to_string_lossy().to_string();
        // tokens we support mirroring the Node implementation
        let tokens = [
            "es",
            "batocera",
            "pocket",
            "mister",
            "onion",
            "adam",
            "retrodeck",
            "romm",
            "twmenu",
            "minui",
            "funkeyos",
            "jelos",
            "miyoocfw",
        ];
        for t in tokens.iter() {
            let placeholder = format!("{{{}}}", t);
            if base_str.contains(&placeholder) {
                if let Some(val) = crate::game_console::output_token_for(t, record, dats) {
                    base_str = base_str.replace(&placeholder, &val);
                }
            }
        }
        base = PathBuf::from(base_str);
    }

    if matches!(config.dir_game_subdir, DirGameSubdirMode::Always) {
        if let Some(stem) = record.relative.file_stem().and_then(|s| s.to_str()) {
            base = base.join(stem);
        }
    }

    base.join(
        record
            .relative
            .file_name()
            .unwrap_or_else(|| record.relative.as_os_str()),
    )
}

/// Return a RomM platform token for a filename based on its extension.
/// This is a minimal mapping to approximate the original behavior. If no
/// known mapping is found we fall back to the extension without the dot.
fn get_romm_for_filename(path: &std::path::Path) -> String {
    game_console::romm_from_extension(path).unwrap_or_else(|| {
        path.extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_ascii_lowercase()
    })
}

fn resolve_genre_token(record: &FileRecord) -> String {
    const FALLBACK: &str = "unknown-genre";
    let candidate = record
        .derived_genres
        .iter()
        .find_map(|g| {
            let trimmed = g.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        })
        .unwrap_or_else(|| FALLBACK.to_string());
    sanitize_path_segment(&candidate).unwrap_or_else(|| FALLBACK.to_string())
}

fn resolve_region_token(record: &FileRecord) -> String {
    const FALLBACK: &str = "unknown-region";
    let candidate = record
        .derived_region
        .as_deref()
        .map(str::trim)
        .filter(|val| !val.is_empty())
        .map(|val| val.to_string())
        .unwrap_or_else(|| FALLBACK.to_string());
    sanitize_path_segment(&candidate).unwrap_or_else(|| FALLBACK.to_string())
}

fn resolve_language_token(record: &FileRecord) -> String {
    const FALLBACK: &str = "unknown-language";
    let candidate = record
        .derived_languages
        .iter()
        .find_map(|lang| {
            let trimmed = lang.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        })
        .unwrap_or_else(|| FALLBACK.to_string());
    sanitize_path_segment(&candidate).unwrap_or_else(|| FALLBACK.to_string())
}

fn sanitize_path_segment(input: &str) -> Option<String> {
    let mut cleaned = String::new();
    for ch in input.trim().chars() {
        match ch {
            '/' | '\\' => cleaned.push('_'),
            c if c.is_control() => continue,
            _ => cleaned.push(ch),
        }
    }
    let normalized = cleaned
        .trim_matches('.')
        .trim()
        .trim_matches('-')
        .to_string();
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

fn annotate_locale_metadata(records: &mut [FileRecord]) {
    for record in records {
        populate_locale_tokens(record);
    }
}

pub(crate) fn populate_locale_tokens(record: &mut FileRecord) {
    let needs_region = record.derived_region.is_none();
    let needs_languages = record.derived_languages.is_empty();
    if !needs_region && !needs_languages {
        return;
    }

    let name = record
        .relative
        .file_name()
        .and_then(|s| s.to_str())
        .map(|s| s.to_string())
        .or_else(|| record.relative.to_str().map(|s| s.to_string()))
        .unwrap_or_else(|| record.relative.to_string_lossy().to_string());

    if name.trim().is_empty() {
        return;
    }

    let tags = extract_tags(&name);
    if needs_region && record.derived_region.is_none() {
        record.derived_region = detect_region(&tags);
    }
    if needs_languages && record.derived_languages.is_empty() {
        record.derived_languages = detect_languages(&tags);
    }
}

pub fn ensure_parent(path: &Path) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{
        Action, ArchiveChecksumMode, Checksum, ChecksumSet, DirGameSubdirMode, FixExtensionMode,
        LinkMode, MergeMode, MoveDeleteDirsMode, SkipReason, ZipFormat,
    };

    fn dummy_record(name: &str) -> FileRecord {
        FileRecord {
            source: PathBuf::from(name),
            relative: PathBuf::from(name),
            size: 0,
            checksums: ChecksumSet {
                crc32: None,
                md5: None,
                sha1: None,
                sha256: None,
            },
            letter_dir: None,
            derived_platform: None,
            derived_genres: Vec::new(),
            derived_region: None,
            derived_languages: Vec::new(),
            scan_info: None,
        }
    }

    fn test_config(region: Option<&str>, language: Option<&str>) -> Config {
        Config {
            commands: vec![Action::Test],
            input: vec![],
            input_exclude: vec![],
            input_checksum_quick: false,
            input_checksum_min: Checksum::Crc32,
            input_checksum_max: None,
            input_checksum_archives: ArchiveChecksumMode::Auto,
            dat: vec![],
            dat_exclude: vec![],
            dat_name_regex: None,
            dat_name_regex_exclude: None,
            dat_description_regex: None,
            dat_description_regex_exclude: None,
            dat_combine: false,
            dat_ignore_parent_clone: false,
            list_unmatched_dats: false,
            print_plan: true,
            enable_hasheous: false,
            igdb_client_id: None,
            igdb_client_secret: None,
            igdb_token: None,
            igdb_token_expires_at: None,
            igdb_mode: crate::types::IgdbLookupMode::BestEffort,
            cache_only: false,
            cache_db: None,
            hash_threads: None,
            scan_threads: None,
            show_match_reasons: false,
            patch: vec![],
            patch_exclude: vec![],
            output: None,
            dir_mirror: false,
            dir_dat_mirror: false,
            dir_dat_name: false,
            dir_dat_description: false,
            dir_letter: false,
            dir_letter_count: None,
            dir_letter_limit: None,
            dir_letter_group: false,
            dir_game_subdir: DirGameSubdirMode::Multiple,
            fix_extension: FixExtensionMode::Auto,
            overwrite: false,
            overwrite_invalid: false,
            move_delete_dirs: MoveDeleteDirsMode::Auto,
            clean_exclude: vec![],
            clean_backup: None,
            clean_dry_run: false,
            zip_format: ZipFormat::Torrentzip,
            zip_exclude: None,
            zip_dat_name: false,
            link_mode: LinkMode::Hardlink,
            symlink_relative: false,
            header: None,
            remove_headers: None,
            trimmed_glob: None,
            trim_scan_archives: false,
            merge_roms: MergeMode::Fullnonmerged,
            merge_discs: false,
            exclude_disks: false,
            allow_excess_sets: false,
            allow_incomplete_sets: false,
            filter_regex: None,
            filter_regex_exclude: None,
            filter_language: language.map(|s| s.to_string()),
            filter_region: region.map(|s| s.to_string()),
            filter_category_regex: None,
            no_bios: false,
            no_device: false,
            no_unlicensed: false,
            only_retail: false,
            no_debug: false,
            no_demo: false,
            no_beta: false,
            no_sample: false,
            no_prototype: false,
            no_program: false,
            verbose: 0,
            quiet: 0,
            diag: false,
            online_timeout_secs: None,
            online_max_retries: None,
            online_throttle_ms: None,
        }
    }

    #[test]
    fn prefers_europe_over_us_and_english_fallback() {
        let config = test_config(Some("EUR,USA"), Some("EN"));
        let records = vec![
            dummy_record("Super Mario World (USA).sfc"),
            dummy_record("Super Mario World (Europe).sfc"),
            dummy_record("Super Mario World (Australia) (En).sfc"),
            dummy_record("Super Mario World (Japan).sfc"),
        ];

        let mut skipped = Vec::new();
        let filtered = filter_by_region_and_language(records, &config, &mut skipped);

        assert_eq!(filtered.len(), 1);
        assert_eq!(
            filtered[0].relative,
            PathBuf::from("Super Mario World (Europe).sfc")
        );
        assert!(
            skipped
                .iter()
                .all(|entry| entry.reason == SkipReason::RegionLanguage)
        );
    }

    #[test]
    fn skips_titles_without_preferred_regions_or_languages() {
        let config = test_config(Some("EUR,USA"), Some("EN"));
        let records = vec![
            dummy_record("Donkey Kong Country (Japan).sfc"),
            dummy_record("Donkey Kong Country (Korea).sfc"),
        ];

        let mut skipped = Vec::new();
        let filtered = filter_by_region_and_language(records, &config, &mut skipped);

        assert!(filtered.is_empty());
        assert_eq!(skipped.len(), 2);
        assert!(
            skipped
                .iter()
                .all(|entry| entry.reason == SkipReason::RegionLanguage)
        );
    }

    #[test]
    fn regex_filter_records_are_logged() {
        let mut cfg = test_config(None, None);
        cfg.filter_regex = Some("Mario".to_string());
        let records = vec![
            dummy_record("Super Mario World.sfc"),
            dummy_record("Legend of Zelda.sfc"),
        ];
        let mut skipped = Vec::new();
        let kept = filter_by_regex(records, &cfg, &mut skipped).unwrap();
        assert_eq!(kept.len(), 1);
        assert_eq!(skipped.len(), 1);
        assert_eq!(skipped[0].reason, SkipReason::RegexInclude);
        assert!(
            skipped[0]
                .detail
                .as_deref()
                .is_some_and(|detail| detail.contains("Legend of Zelda"))
        );
    }

    #[test]
    fn skip_detail_mentions_quality_differences() {
        let config = test_config(Some("USA"), None);
        let records = vec![
            dummy_record("Mega Game (USA) [!].sfc"),
            dummy_record("Mega Game (USA) [b].sfc"),
        ];

        let mut skipped = Vec::new();
        let filtered = filter_by_region_and_language(records, &config, &mut skipped);
        assert_eq!(filtered.len(), 1);
        assert_eq!(
            filtered[0].relative,
            PathBuf::from("Mega Game (USA) [!].sfc")
        );
        assert_eq!(skipped.len(), 1);
        let detail = skipped[0].detail.as_deref().expect("detail present");
        assert!(detail.contains("verified"));
        assert!(detail.contains("[!]") || detail.contains("verified"));
        assert!(detail.contains("[b]") || detail.contains("bad"));
    }

    #[test]
    fn skip_detail_mentions_revision_preference() {
        let config = test_config(Some("USA"), None);
        let records = vec![
            dummy_record("Cool Game (USA) (PRG1).sfc"),
            dummy_record("Cool Game (USA) (PRG0).sfc"),
        ];

        let mut skipped = Vec::new();
        let filtered = filter_by_region_and_language(records, &config, &mut skipped);
        assert_eq!(filtered.len(), 1);
        assert_eq!(
            filtered[0].relative,
            PathBuf::from("Cool Game (USA) (PRG1).sfc")
        );
        assert_eq!(skipped.len(), 1);
        let detail = skipped[0].detail.as_deref().expect("detail present");
        assert!(detail.contains("PRG1"));
        assert!(detail.contains("PRG0"));
    }

    #[test]
    fn detect_region_and_language_from_country_names() {
        let tags = extract_tags("Ape Escape (France).chd");
        let region = detect_region(&tags);
        let languages = detect_languages(&tags);

        assert_eq!(region.as_deref(), Some("EUR"));
        assert!(languages.iter().any(|lang| lang == "FR"));
    }

    #[test]
    fn detect_languages_handles_no_intro_european_tokens() {
        let tags = extract_tags(
            "2 Games in 1 - Finding Nemo + Finding Nemo - The Continuing Adventures (Europe) (Es,It+En,Es,It,Sv,Da)",
        );
        let languages = detect_languages(&tags);

        assert!(
            languages.contains(&"SV".to_string()),
            "expected Swedish token to be normalized"
        );
        assert!(
            languages.contains(&"DA".to_string()),
            "expected Danish token to be normalized"
        );
        assert!(
            languages.contains(&"EN".to_string()),
            "expected English token to remain present"
        );
    }

    #[test]
    fn romm_token_expands_to_romm_library_when_not_present() {
        let mut cfg = test_config(None, None);
        cfg.output = Some(PathBuf::from("/out/{romm}"));
        let rec = dummy_record("Super Mario World.sfc");
        let out = resolve_output_path(&rec, &cfg);
        // expect /out/snes/Super Mario World.sfc (platform directly under output)
        let comps: Vec<_> = out
            .iter()
            .map(|p| p.to_string_lossy().to_string())
            .collect();
        assert!(comps.contains(&"out".to_string()) || comps.contains(&"/out".to_string()));
        assert!(!comps.iter().any(|c| c.eq_ignore_ascii_case("romm")));
        assert!(!comps.iter().any(|c| c.eq_ignore_ascii_case("library")));
        assert!(comps.iter().any(|c| c.eq_ignore_ascii_case("snes")));
    }

    #[test]
    fn romm_token_replaced_when_romm_segment_present() {
        let mut cfg = test_config(None, None);
        cfg.output = Some(PathBuf::from("/out/romm/roms/{romm}"));
        let rec = dummy_record("Super Mario World.sfc");
        let out = resolve_output_path(&rec, &cfg);
        let comps: Vec<_> = out
            .iter()
            .map(|p| p.to_string_lossy().to_string())
            .collect();
        // should not contain 'library' as we already provided romm segment
        assert!(!comps.iter().any(|c| c.eq_ignore_ascii_case("library")));
        assert!(comps.iter().any(|c| c.eq_ignore_ascii_case("romm")));
        assert!(comps.iter().any(|c| c.eq_ignore_ascii_case("snes")));
    }

    #[test]
    fn romm_uses_dat_sha1_when_available() {
        let mut cfg = test_config(None, None);
        cfg.output = Some(PathBuf::from("/out/{romm}"));

        // Create a record with a known sha1 checksum
        let mut rec = dummy_record("Some Game.sfc");
        rec.checksums.sha1 = Some("deadbeefdeadbeefdeadbeefdeadbeefdeadbeef".to_string());

        // Create a DatRom that matches this sha1 and has a dat name indicating SNES
        let dat = crate::dat::DatRom {
            name: "Super Nintendo (SNES)".to_string(),
            description: None,
            source_dat: PathBuf::from("/tmp/test.dat"),
            size: None,
            crc32: None,
            md5: None,
            sha1: Some("deadbeefdeadbeefdeadbeefdeadbeefdeadbeef".to_string()),
            sha256: None,
            match_reasons: None,
        };

        let out = resolve_output_path_with_dats(&rec, &cfg, Some(&[dat.clone()]));
        let comps: Vec<_> = out
            .iter()
            .map(|p| p.to_string_lossy().to_string())
            .collect();
        let expected = crate::game_console::romm_from_dat(&dat).expect("dat pattern should map");
        assert!(comps.iter().any(|c| c.eq_ignore_ascii_case(&expected)));
    }

    #[test]
    fn derived_platform_overrides_filename_extension() {
        let mut cfg = test_config(None, None);
        cfg.output = Some(PathBuf::from("/out/{romm}"));

        let mut rec = dummy_record("Some Game.iso");
        // simulate a platform derived from Hasheous/online provider
        rec.derived_platform = Some("snes".to_string());

        let out = resolve_output_path(&rec, &cfg);
        let comps: Vec<_> = out
            .iter()
            .map(|p| p.to_string_lossy().to_string())
            .collect();
        assert!(
            comps.iter().any(|c| c.eq_ignore_ascii_case("snes")),
            "expected derived platform token in path: {:?}",
            comps
        );
        // ensure we didn't fall back to 'iso' extension mapping
        assert!(!comps.iter().any(|c| c.eq_ignore_ascii_case("iso")));
    }

    #[test]
    fn genre_token_uses_first_derived_value() {
        let mut cfg = test_config(None, None);
        cfg.output = Some(PathBuf::from("/out/{genre}"));

        let mut rec = dummy_record("Mega Game.iso");
        rec.derived_genres = vec!["Action".to_string(), "Adventure".to_string()];

        let out = resolve_output_path(&rec, &cfg);
        let parent = out.parent().expect("parent expected");
        assert_eq!(parent, Path::new("/out/Action"));
    }

    #[test]
    fn genre_token_falls_back_when_absent() {
        let mut cfg = test_config(None, None);
        cfg.output = Some(PathBuf::from("/out/{genre}"));

        let rec = dummy_record("Mega Game.iso");
        let out = resolve_output_path(&rec, &cfg);
        let parent = out.parent().expect("parent expected");
        assert_eq!(parent, Path::new("/out/unknown-genre"));
    }

    #[test]
    fn region_token_uses_derived_value() {
        let mut cfg = test_config(None, None);
        cfg.output = Some(PathBuf::from("/out/{region}"));

        let mut rec = dummy_record("Mega Game.iso");
        rec.derived_region = Some("USA".to_string());

        let out = resolve_output_path(&rec, &cfg);
        let parent = out.parent().expect("parent expected");
        assert_eq!(parent, Path::new("/out/USA"));
    }

    #[test]
    fn region_token_falls_back_when_absent() {
        let mut cfg = test_config(None, None);
        cfg.output = Some(PathBuf::from("/out/{region}"));

        let rec = dummy_record("Mega Game.iso");
        let out = resolve_output_path(&rec, &cfg);
        let parent = out.parent().expect("parent expected");
        assert_eq!(parent, Path::new("/out/unknown-region"));
    }

    #[test]
    fn language_token_uses_first_value() {
        let mut cfg = test_config(None, None);
        cfg.output = Some(PathBuf::from("/out/{language}"));

        let mut rec = dummy_record("Mega Game.iso");
        rec.derived_languages = vec!["EN".to_string(), "JA".to_string()];

        let out = resolve_output_path(&rec, &cfg);
        let parent = out.parent().expect("parent expected");
        assert_eq!(parent, Path::new("/out/EN"));
    }

    #[test]
    fn language_token_falls_back_when_absent() {
        let mut cfg = test_config(None, None);
        cfg.output = Some(PathBuf::from("/out/{language}"));

        let rec = dummy_record("Mega Game.iso");
        let out = resolve_output_path(&rec, &cfg);
        let parent = out.parent().expect("parent expected");
        assert_eq!(parent, Path::new("/out/unknown-language"));
    }
}
