use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use anyhow::Context;
use once_cell::sync::Lazy;
use quick_xml::Reader;
use quick_xml::events::Event;
use reqwest::blocking::Client;
use reqwest::blocking::Response;
use serde::Serialize;
use std::sync::{Condvar, Mutex};
use std::thread::sleep;
use std::time::{Duration, Instant};

use crate::cache;
use crate::config::Config;
use crate::progress::ProgressReporter;
use crate::records::collect_files;
use crate::types::FileRecord;
use rayon::prelude::*;
use std::sync::mpsc;

static HASHEOUS_OVERRIDE: Lazy<Mutex<Option<String>>> = Lazy::new(|| Mutex::new(None));
static IGDB_OVERRIDE: Lazy<Mutex<Option<String>>> = Lazy::new(|| Mutex::new(None));
static IGDB_RATE_LIMITER: Lazy<IgdbRateLimiter> =
    Lazy::new(|| IgdbRateLimiter::new(4, Duration::from_secs(1), 8));
pub(crate) const IGDB_QUERY_FIELDS: &str = "name,slug,summary,first_release_date,platforms.name,platforms.slug,platforms.abbreviation,genres.name,version_parent,parent_game";
pub(crate) const IGDB_KEYWORD_STRATEGY: &str = "strip extensions, drop bracketed tokens, replace punctuation with spaces, remove standalone years and common region/platform noise, then build multi-tier keyword searches (full phrase, truncated phrases, single tokens when short) and augment each with platform hints plus optional platform filters";
const IGDB_PRIMARY_LIMIT: usize = 20;

#[derive(Clone, Debug)]
pub(crate) struct IgdbQueryPlan {
    pub normalized: String,
    pub keywords: Vec<String>,
}

struct IgdbRateLimiter {
    inner: Mutex<IgdbRateState>,
    condvar: Condvar,
    max_per_window: u32,
    window: Duration,
    max_inflight: usize,
}

struct IgdbRateState {
    window_start: Instant,
    requests_in_window: u32,
    inflight: usize,
}

impl IgdbRateLimiter {
    fn new(max_per_window: u32, window: Duration, max_inflight: usize) -> Self {
        Self {
            inner: Mutex::new(IgdbRateState {
                window_start: Instant::now(),
                requests_in_window: 0,
                inflight: 0,
            }),
            condvar: Condvar::new(),
            max_per_window,
            window,
            max_inflight,
        }
    }

    fn acquire(&self) -> IgdbPermit<'_> {
        let mut state = self.inner.lock().unwrap();
        loop {
            let now = Instant::now();
            if now.duration_since(state.window_start) >= self.window {
                state.window_start = now;
                state.requests_in_window = 0;
            }
            if state.requests_in_window < self.max_per_window && state.inflight < self.max_inflight
            {
                state.requests_in_window += 1;
                state.inflight += 1;
                return IgdbPermit { limiter: self };
            }

            let rate_wait = if state.requests_in_window >= self.max_per_window {
                self.window
                    .checked_sub(now.duration_since(state.window_start))
                    .unwrap_or_else(|| Duration::from_millis(5))
            } else {
                Duration::from_millis(5)
            };
            let inflight_wait = if state.inflight >= self.max_inflight {
                Duration::from_millis(10)
            } else {
                Duration::from_millis(5)
            };
            let wait_for = rate_wait.max(inflight_wait);
            let (next_state, _) = self.condvar.wait_timeout(state, wait_for).unwrap();
            state = next_state;
        }
    }

    fn release(&self) {
        let mut state = self.inner.lock().unwrap();
        if state.inflight > 0 {
            state.inflight -= 1;
        }
        self.condvar.notify_one();
    }
}

struct IgdbPermit<'a> {
    limiter: &'a IgdbRateLimiter,
}

impl Drop for IgdbPermit<'_> {
    fn drop(&mut self) {
        self.limiter.release();
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct DatRom {
    pub name: String,
    pub description: Option<String>,
    pub source_dat: PathBuf,
    pub size: Option<u64>,
    pub crc32: Option<String>,
    pub md5: Option<String>,
    pub sha1: Option<String>,
    pub sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_reasons: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OnlineMatch {
    pub name: String,
    pub source_dat: Option<PathBuf>,
    pub source_path: Option<PathBuf>,
    pub hasheous: Option<serde_json::Value>,
    pub igdb: Option<serde_json::Value>,
}

pub fn load_dat_roms(
    config: &Config,
    progress: Option<&ProgressReporter>,
) -> anyhow::Result<Vec<DatRom>> {
    use glob::glob;
    use std::fs;

    let mut roms = Vec::new();

    // Expand provided dat arguments into concrete file paths. Support:
    // - direct file paths
    // - glob patterns (contains '*' or '?')
    // - simple tokens like 'Nintendo' -> search CWD and D:\\igir\\dat for filenames containing the token
    fn collect_files_recursively(
        dir: &std::path::Path,
        out: &mut Vec<std::path::PathBuf>,
    ) -> anyhow::Result<()> {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                collect_files_recursively(&path, out)?;
            } else if path.is_file() {
                out.push(path);
            }
        }
        Ok(())
    }

    fn looks_like_token(input: &str) -> bool {
        !(input.contains('\\') || input.contains('/') || input.contains(':'))
    }

    let mut resolved: Vec<std::path::PathBuf> = Vec::new();
    let mut missing_explicit: Vec<std::path::PathBuf> = Vec::new();
    for dat_arg in &config.dat {
        let s = dat_arg.to_string_lossy();
        // If the path exists as given, use it (expanding directories)
        if dat_arg.exists() {
            if dat_arg.is_dir() {
                collect_files_recursively(dat_arg, &mut resolved)?;
            } else {
                resolved.push(dat_arg.clone());
            }
            continue;
        }

        // If it looks like a glob pattern, expand
        let s_str = s.as_ref();
        if s_str.contains('*') || s_str.contains('?') {
            for entry in glob(s_str)? {
                if let Ok(path) = entry {
                    if path.is_file() {
                        resolved.push(path);
                    }
                }
            }
            continue;
        }

        if !looks_like_token(s_str) {
            missing_explicit.push(dat_arg.clone());
            continue;
        }

        // Otherwise treat as a substring token: search current dir and a default DAT dir
        let token = s_str.to_lowercase();
        // search current working directory
        if let Ok(entries) = fs::read_dir(".") {
            for ent in entries.flatten() {
                let p = ent.path();
                if p.is_file() {
                    if let Some(name) = p.file_name().and_then(|n| n.to_str()) {
                        if name.to_lowercase().contains(&token) {
                            resolved.push(p.clone());
                        }
                    }
                }
            }
        }

        // also search D:\\igir\\dat if it exists (a convention from the user's environment)
        let default_dat_dir = std::path::Path::new(r"D:\\igir\\dat");
        if default_dat_dir.exists() {
            if let Ok(entries) = fs::read_dir(default_dat_dir) {
                for ent in entries.flatten() {
                    let p = ent.path();
                    if p.is_file() {
                        if let Some(name) = p.file_name().and_then(|n| n.to_str()) {
                            if name.to_lowercase().contains(&token) {
                                resolved.push(p.clone());
                            }
                        }
                    }
                }
            }
        }
    }

    if !missing_explicit.is_empty() {
        let joined = missing_explicit
            .iter()
            .map(|p| p.to_string_lossy().to_string())
            .collect::<Vec<_>>()
            .join(", ");
        anyhow::bail!("DAT path(s) not found: {joined}");
    }

    // Deduplicate
    resolved.sort();
    resolved.dedup();

    if let Some(p) = progress {
        p.begin_dat_loading(resolved.len());
    }

    let mut parsed_count = 0usize;

    if !resolved.is_empty() {
        let job_count = resolved.len();
        let (tx, rx) = mpsc::channel::<anyhow::Result<(PathBuf, Vec<DatRom>)>>();
        let jobs_clone: Vec<PathBuf> = resolved.clone();

        // Spawn a worker thread which runs a Rayon iterator to parse DATs in parallel
        // and sends per-DAT parsed roms back to the receiver as they complete.
        let handle = std::thread::spawn(move || {
            jobs_clone
                .par_iter()
                .for_each_with(tx.clone(), |s, dat_path| {
                    let res: anyhow::Result<(PathBuf, Vec<DatRom>)> = (|| {
                        let mut reader = Reader::from_file(dat_path).with_context(|| {
                            format!("unable to open DAT file: {}", dat_path.to_string_lossy())
                        })?;
                        reader.config_mut().trim_text(true);
                        let mut buf = Vec::new();

                        let mut roms_local: Vec<DatRom> = Vec::new();
                        let mut current_description: Option<String> = None;
                        let mut in_description = false;

                        loop {
                            match reader.read_event_into(&mut buf) {
                                Ok(Event::Start(ref e))
                                    if e.name().as_ref() == b"game"
                                        || e.name().as_ref() == b"machine" =>
                                {
                                    current_description = e
                                        .attributes()
                                        .filter_map(Result::ok)
                                        .find(|a| a.key.as_ref() == b"name")
                                        .and_then(|a| String::from_utf8(a.value.into_owned()).ok());
                                }
                                Ok(Event::Start(ref e)) if e.name().as_ref() == b"description" => {
                                    in_description = true;
                                }
                                Ok(Event::Text(e)) if in_description => {
                                    current_description =
                                        Some(e.unescape().unwrap_or_default().to_string());
                                    in_description = false;
                                }
                                Ok(Event::Empty(ref e)) if e.name().as_ref() == b"rom" => {
                                    let mut rom = DatRom {
                                        name: String::new(),
                                        description: current_description.clone(),
                                        source_dat: dat_path.clone(),
                                        size: None,
                                        crc32: None,
                                        md5: None,
                                        sha1: None,
                                        sha256: None,
                                        match_reasons: None,
                                    };

                                    for attr in e.attributes().flatten() {
                                        let key = attr.key.as_ref();
                                        let value =
                                            String::from_utf8_lossy(&attr.value).to_string();
                                        match key {
                                            b"name" | b"NAME" => rom.name = value,
                                            b"size" | b"SIZE" => rom.size = value.parse().ok(),
                                            b"crc" | b"CRC" => {
                                                rom.crc32 = Some(value.to_ascii_uppercase())
                                            }
                                            b"md5" | b"MD5" => {
                                                rom.md5 = Some(value.to_ascii_lowercase())
                                            }
                                            b"sha1" | b"SHA1" => {
                                                rom.sha1 = Some(value.to_ascii_lowercase())
                                            }
                                            b"sha256" | b"SHA256" => {
                                                rom.sha256 = Some(value.to_ascii_lowercase())
                                            }
                                            _ => {}
                                        }
                                    }

                                    rom.match_reasons = None;
                                    roms_local.push(rom);
                                }
                                // Also accept <rom ...>start</rom> style elements where attributes are on Start
                                Ok(Event::Start(ref e)) if e.name().as_ref() == b"rom" => {
                                    let mut rom = DatRom {
                                        name: String::new(),
                                        description: current_description.clone(),
                                        source_dat: dat_path.clone(),
                                        size: None,
                                        crc32: None,
                                        md5: None,
                                        sha1: None,
                                        sha256: None,
                                        match_reasons: None,
                                    };

                                    for attr in e.attributes().flatten() {
                                        let key = attr.key.as_ref();
                                        let value =
                                            String::from_utf8_lossy(&attr.value).to_string();
                                        match key {
                                            b"name" | b"NAME" => rom.name = value,
                                            b"size" | b"SIZE" => rom.size = value.parse().ok(),
                                            b"crc" | b"CRC" => {
                                                rom.crc32 = Some(value.to_ascii_uppercase())
                                            }
                                            b"md5" | b"MD5" => {
                                                rom.md5 = Some(value.to_ascii_lowercase())
                                            }
                                            b"sha1" | b"SHA1" => {
                                                rom.sha1 = Some(value.to_ascii_lowercase())
                                            }
                                            b"sha256" | b"SHA256" => {
                                                rom.sha256 = Some(value.to_ascii_lowercase())
                                            }
                                            _ => {}
                                        }
                                    }

                                    rom.match_reasons = None;
                                    roms_local.push(rom);
                                }
                                Ok(Event::Eof) => break,
                                _ => {}
                            }
                            buf.clear();
                        }

                        Ok((dat_path.clone(), roms_local))
                    })();
                    let _ = s.send(res);
                });
        });

        // Receive parsed DAT results and update progress on the main thread as each DAT completes
        for _ in 0..job_count {
            match rx.recv() {
                Ok(Ok((dat_path, mut parsed))) => {
                    parsed_count += 1;
                    if let Some(p) = progress {
                        p.advance_dat_loading(parsed_count, Some(&dat_path));
                    }
                    roms.append(&mut parsed);
                }
                Ok(Err(e)) => {
                    // worker signalled an error parsing a DAT
                    // join worker thread then return the error
                    let _ = handle.join();
                    return Err(e);
                }
                Err(e) => {
                    let _ = handle.join();
                    return Err(anyhow::anyhow!("dat worker terminated: {e}"));
                }
            }
        }

        // ensure the worker finished
        let _ = handle.join();
    }

    if let Some(p) = progress {
        p.finish_dat_loading(parsed_count);
    }

    Ok(roms)
}

fn match_reasons_for_record(record: &FileRecord, dat: &DatRom) -> Vec<String> {
    let mut reasons: Vec<String> = Vec::new();

    if let Some(sha1) = &dat.sha1 {
        if record.checksums.sha1.as_deref() == Some(sha1.as_str()) {
            reasons.push("sha1".to_string());
        }
    }
    if let Some(md5) = &dat.md5 {
        if record.checksums.md5.as_deref() == Some(md5.as_str()) {
            reasons.push("md5".to_string());
        }
    }
    if let Some(sha256) = &dat.sha256 {
        if record.checksums.sha256.as_deref() == Some(sha256.as_str()) {
            reasons.push("sha256".to_string());
        }
    }
    // CRC32 requires size equality (per matching policy)
    if let (Some(crc), Some(dat_size)) = (dat.crc32.as_deref(), dat.size) {
        if record
            .checksums
            .crc32
            .as_deref()
            .is_some_and(|c| c.eq_ignore_ascii_case(crc))
            && record.size == dat_size
        {
            reasons.push("crc32+size".to_string());
        }
    }
    // Size+name match (DAT provided size and filename matches exactly)
    if let Some(dat_size) = dat.size {
        if record.size == dat_size {
            if let Some(name) = record.relative.file_name().and_then(|n| n.to_str()) {
                if name == dat.name {
                    reasons.push("size+name".to_string());
                }
            }
        }
    }

    reasons
}

#[cfg(test)]
fn rom_matches(record: &FileRecord, dat: &DatRom) -> bool {
    !match_reasons_for_record(record, dat).is_empty()
}

#[derive(Default)]
struct RecordIndex {
    sha1: HashMap<String, Vec<usize>>,
    sha256: HashMap<String, Vec<usize>>,
    md5: HashMap<String, Vec<usize>>,
    crc_size: HashMap<(String, u64), Vec<usize>>,
    size_name: HashMap<(u64, String), Vec<usize>>,
}

#[derive(Default)]
pub(crate) struct DatIndex {
    sha1: HashMap<String, Vec<usize>>,
    sha256: HashMap<String, Vec<usize>>,
    md5: HashMap<String, Vec<usize>>,
    crc_size: HashMap<(String, u64), Vec<usize>>,
    size_name: HashMap<(u64, String), Vec<usize>>,
}

impl DatIndex {
    pub(crate) fn from_dats(dats: &[DatRom]) -> Self {
        let mut index = DatIndex::default();
        for (idx, dat) in dats.iter().enumerate() {
            if let Some(sha1) = &dat.sha1 {
                index.sha1.entry(sha1.clone()).or_default().push(idx);
            }
            if let Some(sha256) = &dat.sha256 {
                index.sha256.entry(sha256.clone()).or_default().push(idx);
            }
            if let Some(md5) = &dat.md5 {
                index.md5.entry(md5.clone()).or_default().push(idx);
            }
            if let (Some(crc), Some(size)) = (dat.crc32.as_deref(), dat.size) {
                let key = (crc.to_ascii_uppercase(), size);
                index.crc_size.entry(key).or_default().push(idx);
            }
            if let Some(size) = dat.size {
                index
                    .size_name
                    .entry((size, dat.name.clone()))
                    .or_default()
                    .push(idx);
            }
        }
        index
    }

    fn match_candidates(&self, record: &FileRecord) -> Vec<usize> {
        let mut candidates = Vec::new();
        let mut seen = HashSet::new();

        Self::collect(
            &self.sha1,
            record.checksums.sha1.as_deref(),
            &mut seen,
            &mut candidates,
        );
        Self::collect(
            &self.sha256,
            record.checksums.sha256.as_deref(),
            &mut seen,
            &mut candidates,
        );
        Self::collect(
            &self.md5,
            record.checksums.md5.as_deref(),
            &mut seen,
            &mut candidates,
        );
        if let Some(crc) = record.checksums.crc32.as_deref() {
            let key = (crc.to_ascii_uppercase(), record.size);
            Self::collect_tuple_crc(&self.crc_size, &key, &mut seen, &mut candidates);
        }
        if let Some(name) = record.relative.file_name().and_then(|n| n.to_str()) {
            let key = (record.size, name.to_string());
            Self::collect_tuple_size_name(&self.size_name, &key, &mut seen, &mut candidates);
        }

        candidates
    }

    fn collect(
        map: &HashMap<String, Vec<usize>>,
        key: Option<&str>,
        seen: &mut HashSet<usize>,
        candidates: &mut Vec<usize>,
    ) {
        if let Some(k) = key {
            if let Some(indices) = map.get(k) {
                for &idx in indices {
                    if seen.insert(idx) {
                        candidates.push(idx);
                    }
                }
            }
        }
    }

    fn collect_tuple_crc(
        map: &HashMap<(String, u64), Vec<usize>>,
        key: &(String, u64),
        seen: &mut HashSet<usize>,
        candidates: &mut Vec<usize>,
    ) {
        if let Some(indices) = map.get(key) {
            for &idx in indices {
                if seen.insert(idx) {
                    candidates.push(idx);
                }
            }
        }
    }

    fn collect_tuple_size_name(
        map: &HashMap<(u64, String), Vec<usize>>,
        key: &(u64, String),
        seen: &mut HashSet<usize>,
        candidates: &mut Vec<usize>,
    ) {
        if let Some(indices) = map.get(key) {
            for &idx in indices {
                if seen.insert(idx) {
                    candidates.push(idx);
                }
            }
        }
    }
}

impl RecordIndex {
    fn from_records(records: &[FileRecord]) -> Self {
        let mut index = RecordIndex::default();
        for (idx, record) in records.iter().enumerate() {
            if let Some(sha1) = &record.checksums.sha1 {
                index.sha1.entry(sha1.clone()).or_default().push(idx);
            }
            if let Some(sha256) = &record.checksums.sha256 {
                index.sha256.entry(sha256.clone()).or_default().push(idx);
            }
            if let Some(md5) = &record.checksums.md5 {
                index.md5.entry(md5.clone()).or_default().push(idx);
            }
            if let Some(crc) = &record.checksums.crc32 {
                let key = (crc.to_ascii_uppercase(), record.size);
                index.crc_size.entry(key).or_default().push(idx);
            }
            if let Some(name) = record.relative.file_name().and_then(|name| name.to_str()) {
                index
                    .size_name
                    .entry((record.size, name.to_string()))
                    .or_default()
                    .push(idx);
            }
        }
        index
    }

    fn match_candidates<'a>(&'a self, dat: &DatRom) -> Vec<usize> {
        let mut candidates = Vec::new();
        let mut seen = HashSet::new();

        Self::collect(&self.sha1, dat.sha1.as_deref(), &mut seen, &mut candidates);
        Self::collect(
            &self.sha256,
            dat.sha256.as_deref(),
            &mut seen,
            &mut candidates,
        );
        Self::collect(&self.md5, dat.md5.as_deref(), &mut seen, &mut candidates);
        if let (Some(crc), Some(size)) = (dat.crc32.as_deref(), dat.size) {
            let key = (crc.to_ascii_uppercase(), size);
            Self::collect_tuple_crc(&self.crc_size, &key, &mut seen, &mut candidates);
        }
        if let Some(size) = dat.size {
            let key = (size, dat.name.clone());
            Self::collect_tuple_size_name(&self.size_name, &key, &mut seen, &mut candidates);
        }

        candidates
    }

    fn collect(
        map: &HashMap<String, Vec<usize>>,
        key: Option<&str>,
        seen: &mut HashSet<usize>,
        candidates: &mut Vec<usize>,
    ) {
        if let Some(k) = key {
            if let Some(indices) = map.get(k) {
                for &idx in indices {
                    if seen.insert(idx) {
                        candidates.push(idx);
                    }
                }
            }
        }
    }

    fn collect_tuple_crc(
        map: &HashMap<(String, u64), Vec<usize>>,
        key: &(String, u64),
        seen: &mut HashSet<usize>,
        candidates: &mut Vec<usize>,
    ) {
        if let Some(indices) = map.get(key) {
            for &idx in indices {
                if seen.insert(idx) {
                    candidates.push(idx);
                }
            }
        }
    }

    fn collect_tuple_size_name(
        map: &HashMap<(u64, String), Vec<usize>>,
        key: &(u64, String),
        seen: &mut HashSet<usize>,
        candidates: &mut Vec<usize>,
    ) {
        if let Some(indices) = map.get(key) {
            for &idx in indices {
                if seen.insert(idx) {
                    candidates.push(idx);
                }
            }
        }
    }
}

pub fn partition_dat_matches(
    records: &[FileRecord],
    dat_roms: &[DatRom],
) -> (Vec<DatRom>, Vec<DatRom>) {
    let mut matched = Vec::new();
    let mut unmatched = Vec::new();

    let index = RecordIndex::from_records(records);
    for dat in dat_roms {
        let mut found_reasons: Option<Vec<String>> = None;
        for record_idx in index.match_candidates(dat) {
            let record = &records[record_idx];
            let reasons = match_reasons_for_record(record, dat);
            if !reasons.is_empty() {
                found_reasons = Some(reasons);
                break;
            }
        }
        if let Some(reasons) = found_reasons {
            let mut dat_clone = dat.clone();
            dat_clone.match_reasons = Some(reasons);
            matched.push(dat_clone);
        } else {
            unmatched.push(dat.clone());
        }
    }

    (matched, unmatched)
}

pub fn dat_unmatched(records: &[FileRecord], dat_roms: &[DatRom]) -> (Vec<DatRom>, usize) {
    let (matched, unmatched) = partition_dat_matches(records, dat_roms);
    let matched_count = matched.len();
    (unmatched, matched_count)
}

/// Find a DAT entry that matches the provided record, if any.
pub fn find_dat_for_record(record: &FileRecord, dat_roms: &[DatRom]) -> Option<DatRom> {
    let dat_index = DatIndex::from_dats(dat_roms);
    find_dat_for_record_with_index(record, dat_roms, &dat_index)
}

pub(crate) fn find_dat_for_record_with_index(
    record: &FileRecord,
    dat_roms: &[DatRom],
    dat_index: &DatIndex,
) -> Option<DatRom> {
    for dat_idx in dat_index.match_candidates(record) {
        let dat = &dat_roms[dat_idx];
        let reasons = match_reasons_for_record(record, dat);
        if !reasons.is_empty() {
            let mut dat_clone = dat.clone();
            dat_clone.match_reasons = Some(reasons);
            return Some(dat_clone);
        }
    }
    None
}

/// Return the list of records that had no matching DAT entry.
pub fn records_without_dat_match(records: &[FileRecord], dat_roms: &[DatRom]) -> Vec<FileRecord> {
    let dat_index = DatIndex::from_dats(dat_roms);
    records_without_dat_match_with_index(records, dat_roms, &dat_index)
}

pub(crate) fn records_without_dat_match_with_index(
    records: &[FileRecord],
    dat_roms: &[DatRom],
    dat_index: &DatIndex,
) -> Vec<FileRecord> {
    records
        .iter()
        .filter(|record| find_dat_for_record_with_index(record, dat_roms, dat_index).is_none())
        .cloned()
        .collect()
}

pub(crate) fn query_hasheous(
    client: &Client,
    alg: &str,
    hash: &str,
    verbose: u8,
    max_retries: usize,
    throttle_ms: Option<u64>,
) -> anyhow::Result<Option<serde_json::Value>> {
    // Default base is domain only; we append the API path below.
    let base = HASHEOUS_OVERRIDE
        .lock()
        .unwrap()
        .clone()
        .unwrap_or_else(|| {
            std::env::var("HASHEOUS_BASE").unwrap_or_else(|_| "https://hasheous.org".to_string())
        });
    let base = base.trim_end_matches('/');
    let url = format!(
        "{}/api/v1/Lookup/ByHash/{alg}/{hash}",
        base,
        alg = alg,
        hash = hash
    );
    // Use the provided client so timeouts and test-injected clients are respected
    // Implement retry/backoff controlled by configuration.
    // We'll track the last observed error message only for verbose logging; avoid assigning unused variables
    let max_attempts = std::cmp::max(1, max_retries);
    let mut resp_opt: Option<Response> = None;
    for attempt in 0..max_attempts {
        match client.get(&url).send() {
            Ok(r) => {
                resp_opt = Some(r);
                break;
            }
            Err(e) => {
                vprintln!(
                    verbose,
                    1,
                    "hasheous request error for {} (attempt {}): {}",
                    hash,
                    attempt + 1,
                    e
                );
                // throttle between attempts if requested
                if attempt + 1 < max_attempts {
                    if let Some(ms) = throttle_ms {
                        sleep(Duration::from_millis(ms));
                    } else {
                        let backoff = Duration::from_millis(250 * (1 << attempt));
                        sleep(backoff);
                    }
                    continue;
                }
            }
        }
    }
    let resp = match resp_opt {
        Some(r) => r,
        None => return Ok(None),
    };

    vprintln!(verbose, 3, "hasheous: url={} status={}", url, resp.status());

    if !resp.status().is_success() {
        return Ok(None);
    }

    match resp.json::<serde_json::Value>() {
        Ok(j) => {
            vprintln!(verbose, 3, "hasheous: got json for {} -> {}", hash, j);
            Ok(Some(j))
        }
        Err(e) => {
            vprintln!(verbose, 1, "hasheous: json parse error for {}: {}", hash, e);
            Ok(None)
        }
    }
}

pub(crate) fn query_igdb(
    name: &str,
    config: &Config,
    client: &Client,
    platform_hint: Option<&str>,
) -> anyhow::Result<Option<serde_json::Value>> {
    let Some(client_id) = &config.igdb_client_id else {
        return Ok(None);
    };
    let Some(token) = &config.igdb_token else {
        return Ok(None);
    };

    let plan = normalize_name_with_keywords(name);
    let platform_filter_clause = platform_hint.and_then(|token| igdb_platform_filter_clause(token));
    let search_terms =
        build_igdb_search_terms(&plan, platform_hint, platform_filter_clause.as_deref());
    let mut fallback: Option<serde_json::Value> = None;
    for (term, apply_filter) in search_terms {
        if term.trim().is_empty() {
            continue;
        }
        let body = if apply_filter {
            let filter = platform_filter_clause
                .as_deref()
                .expect("search builder should not request a missing filter");
            format!(
                "search \"{}\"; where {}; fields {}; limit {};",
                term, filter, IGDB_QUERY_FIELDS, IGDB_PRIMARY_LIMIT
            )
        } else {
            format!(
                "search \"{}\"; fields {}; limit {};",
                term, IGDB_QUERY_FIELDS, IGDB_PRIMARY_LIMIT
            )
        };
        let response = execute_igdb_games_query(config, client, client_id, token, &body, name)?;
        let Some(raw) = response else {
            continue;
        };
        let (prioritized, matched) = prioritize_results_for_platform(raw, platform_hint);
        if let Some(value) = prioritized {
            if matched {
                return Ok(Some(value));
            }
            if fallback.is_none() {
                fallback = Some(value);
            }
            break;
        }
    }

    Ok(fallback)
}

fn execute_igdb_games_query(
    config: &Config,
    client: &Client,
    client_id: &str,
    token: &str,
    body: &str,
    log_name: &str,
) -> anyhow::Result<Option<serde_json::Value>> {
    let base = IGDB_OVERRIDE.lock().unwrap().clone().unwrap_or_else(|| {
        std::env::var("IGDB_BASE").unwrap_or_else(|_| "https://api.igdb.com/v4".to_string())
    });
    let url = format!("{}/games", base);
    let _permit = IGDB_RATE_LIMITER.acquire();
    let resp = match client
        .post(&url)
        .header("Client-ID", client_id)
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "application/json")
        .header("Content-Type", "text/plain")
        .body(body.to_string())
        .send()
    {
        Ok(r) => r,
        Err(e) => {
            vprintln!(
                config.verbose,
                1,
                "igdb request error for {}: {}",
                log_name,
                e
            );
            return Ok(None);
        }
    };

    if !resp.status().is_success() {
        vprintln!(
            config.verbose,
            1,
            "igdb request failed: name={} status={}",
            log_name,
            resp.status()
        );
        return Ok(None);
    }

    match resp.json::<serde_json::Value>() {
        Ok(j) => Ok(Some(j)),
        Err(_) => Ok(None),
    }
}

pub(crate) fn query_igdb_by_slug(
    slug: &str,
    config: &Config,
    client: &Client,
) -> anyhow::Result<Option<serde_json::Value>> {
    let Some(client_id) = &config.igdb_client_id else {
        return Ok(None);
    };
    let Some(token) = &config.igdb_token else {
        return Ok(None);
    };

    let normalized =
        crate::actions::normalize_igdb_slug_candidate(slug).unwrap_or_else(|| slug.to_string());
    if normalized.is_empty() {
        return Ok(None);
    }

    let body = format!(
        "where slug = \"{}\"; fields {}; limit 1;",
        normalized, IGDB_QUERY_FIELDS
    );
    let base = IGDB_OVERRIDE.lock().unwrap().clone().unwrap_or_else(|| {
        std::env::var("IGDB_BASE").unwrap_or_else(|_| "https://api.igdb.com/v4".to_string())
    });
    let url = format!("{}/games", base);
    let _permit = IGDB_RATE_LIMITER.acquire();
    let resp = match client
        .post(&url)
        .header("Client-ID", client_id)
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "application/json")
        .header("Content-Type", "text/plain")
        .body(body)
        .send()
    {
        Ok(r) => r,
        Err(e) => {
            vprintln!(
                config.verbose,
                1,
                "igdb request error for slug {}: {}",
                slug,
                e
            );
            return Ok(None);
        }
    };

    if !resp.status().is_success() {
        vprintln!(
            config.verbose,
            1,
            "igdb request failed: slug={} status={}",
            slug,
            resp.status()
        );
        return Ok(None);
    }

    match resp.json::<serde_json::Value>() {
        Ok(j) => Ok(Some(j)),
        Err(_) => Ok(None),
    }
}

pub(crate) fn query_igdb_by_id(
    id: i64,
    config: &Config,
    client: &Client,
) -> anyhow::Result<Option<serde_json::Value>> {
    let Some(client_id) = &config.igdb_client_id else {
        return Ok(None);
    };
    let Some(token) = &config.igdb_token else {
        return Ok(None);
    };

    let body = format!("where id = {id}; fields {}; limit 1;", IGDB_QUERY_FIELDS);
    let base = IGDB_OVERRIDE.lock().unwrap().clone().unwrap_or_else(|| {
        std::env::var("IGDB_BASE").unwrap_or_else(|_| "https://api.igdb.com/v4".to_string())
    });
    let url = format!("{}/games", base);
    let _permit = IGDB_RATE_LIMITER.acquire();
    let resp = match client
        .post(&url)
        .header("Client-ID", client_id)
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "application/json")
        .header("Content-Type", "text/plain")
        .body(body)
        .send()
    {
        Ok(r) => r,
        Err(e) => {
            vprintln!(config.verbose, 1, "igdb request error for id {}: {}", id, e);
            return Ok(None);
        }
    };

    if !resp.status().is_success() {
        vprintln!(
            config.verbose,
            1,
            "igdb request failed: id={} status={}",
            id,
            resp.status()
        );
        return Ok(None);
    }

    match resp.json::<serde_json::Value>() {
        Ok(j) => Ok(Some(j)),
        Err(_) => Ok(None),
    }
}

/// Lightweight name normalization for IGDB queries: remove parenthetical tokens, bracket tokens and year tokens.
pub(crate) fn normalize_name(s: &str) -> String {
    normalize_name_with_keywords(s).normalized
}

pub(crate) fn normalize_name_with_keywords(s: &str) -> IgdbQueryPlan {
    let mut out = s.to_string();
    // strip file extension if present (e.g., .zip, .7z, .nes)
    if let Some(pos) = out.rfind('.') {
        // only strip if extension looks reasonable (1..8 chars)
        if out.len() - pos - 1 >= 1 && out.len() - pos - 1 <= 8 {
            out.truncate(pos);
        }
    }

    // remove (...) and [...] tokens
    loop {
        if let Some(start) = out.find('(') {
            if let Some(rel_end) = out[start..].find(')') {
                let end = start + rel_end;
                out.replace_range(start..=end, "");
                continue;
            }
        }
        break;
    }
    loop {
        if let Some(start) = out.find('[') {
            if let Some(rel_end) = out[start..].find(']') {
                let end = start + rel_end;
                out.replace_range(start..=end, "");
                continue;
            }
        }
        break;
    }

    // Replace separators and punctuation with spaces
    let mut cleaned = String::with_capacity(out.len());
    for ch in out.chars() {
        if ch.is_ascii_alphanumeric() || ch.is_whitespace() {
            cleaned.push(ch);
        } else {
            cleaned.push(' ');
        }
    }

    // Remove common noise tokens (regions, platforms) and standalone years
    let noise = [
        "usa", "eu", "japan", "pal", "ntsc", "psx", "ps2", "ps3", "ps4", "snes", "nes", "md",
        "gen", "mega", "sega", "n64", "ds", "3ds", "switch", "xbox", "xbox360", "pc",
    ];
    let mut keywords: Vec<String> = Vec::new();
    let mut skip_numeric_after_rev = false;
    for token in cleaned.split_whitespace().map(|t| t.trim()) {
        if token.is_empty() {
            continue;
        }
        if skip_numeric_after_rev {
            if token.chars().all(|c| c.is_ascii_digit()) {
                skip_numeric_after_rev = false;
                continue;
            }
            skip_numeric_after_rev = false;
        }
        if token.eq_ignore_ascii_case("rev") {
            skip_numeric_after_rev = true;
            continue;
        }
        if token.len() == 4 && token.chars().all(|c| c.is_ascii_digit()) {
            continue;
        }
        let lower = token.to_lowercase();
        if noise.iter().any(|n| *n == lower) {
            continue;
        }
        keywords.push(token.to_string());
    }

    let normalized = keywords.join(" ").trim().to_string();
    IgdbQueryPlan {
        normalized,
        keywords,
    }
}

fn build_igdb_search_terms(
    plan: &IgdbQueryPlan,
    platform_hint: Option<&str>,
    platform_filter_clause: Option<&str>,
) -> Vec<(String, bool)> {
    let mut base_terms: Vec<String> = Vec::new();
    let mut seen = HashSet::new();
    let display_name = platform_hint.and_then(crate::igdb_platform_map::display_name);

    let mut push_base = |candidate: String| {
        let trimmed = candidate.trim();
        if trimmed.is_empty() {
            return;
        }
        let key = trimmed.to_ascii_lowercase();
        if seen.insert(key) {
            base_terms.push(trimmed.to_string());
        }
    };

    if !plan.normalized.is_empty() {
        push_base(plan.normalized.clone());
        if let Some(display) = display_name {
            push_base(format!("{} {}", plan.normalized, display));
        }
    } else if let Some(display) = display_name {
        push_base(display.to_string());
    }

    if plan.keywords.len() > 2 {
        for len in (2..plan.keywords.len()).rev() {
            let slice = &plan.keywords[..len];
            let joined = slice.join(" ");
            push_base(joined.clone());
            if let Some(display) = display_name {
                push_base(format!("{} {}", joined, display));
            }
        }
    } else if !plan.keywords.is_empty() {
        for keyword in &plan.keywords {
            push_base(keyword.clone());
            if let Some(display) = display_name {
                push_base(format!("{} {}", keyword, display));
            }
        }
    }

    if base_terms.is_empty() {
        if !plan.normalized.is_empty() {
            base_terms.push(plan.normalized.clone());
        } else if let Some(first) = plan.keywords.first() {
            base_terms.push(first.clone());
        }
    }

    let mut terms = Vec::new();
    let mut first_term = true;
    for base in base_terms {
        if first_term {
            if platform_filter_clause.is_some() {
                terms.push((base.clone(), true));
            }
            first_term = false;
        }
        terms.push((base, false));
    }

    terms
}

fn igdb_platform_filter_clause(token: &str) -> Option<String> {
    if let Some(slug) = crate::igdb_platform_map::slug(token) {
        return Some(format!("platforms.slug = \"{}\"", slug));
    }
    if let Some(display) = crate::igdb_platform_map::display_name(token) {
        return Some(format!(
            "platforms.name ~ *\"{}\"*",
            escape_igdb_string(display)
        ));
    }
    None
}

fn escape_igdb_string(input: &str) -> String {
    input.replace('"', "\\\"")
}

fn platform_identifier_matches_token(identifier: &str, token: &str) -> bool {
    if identifier.trim().is_empty() {
        return false;
    }
    if let Some(mapped) = crate::igdb_platform_map::lookup(identifier) {
        if mapped == token {
            return true;
        }
    }
    if let Some(mapped) = crate::game_console::romm_from_platform_name(identifier) {
        if mapped == token {
            return true;
        }
    }
    false
}

pub(crate) fn igdb_cache_entry_matches_platform(
    entry: &crate::cache::IgdbCacheEntry,
    derived_platform: &str,
) -> bool {
    if entry.platforms.is_empty() {
        return true;
    }
    entry
        .platforms
        .iter()
        .any(|identifier| platform_identifier_matches_token(identifier, derived_platform))
}

fn json_entry_matches_platform(entry: &serde_json::Value, derived_platform: &str) -> bool {
    let Some(platforms) = entry.get("platforms").and_then(|v| v.as_array()) else {
        return false;
    };
    for platform in platforms {
        if let Some(name) = platform.get("name").and_then(|n| n.as_str()) {
            if platform_identifier_matches_token(name, derived_platform) {
                return true;
            }
        }
        if let Some(slug) = platform.get("slug").and_then(|s| s.as_str()) {
            if platform_identifier_matches_token(slug, derived_platform) {
                return true;
            }
        }
        if let Some(abbrev) = platform.get("abbreviation").and_then(|s| s.as_str()) {
            if platform_identifier_matches_token(abbrev, derived_platform) {
                return true;
            }
        }
    }
    false
}

fn prioritize_results_for_platform(
    json: serde_json::Value,
    derived_platform: Option<&str>,
) -> (Option<serde_json::Value>, bool) {
    match json {
        serde_json::Value::Array(mut entries) => {
            if entries.is_empty() {
                return (None, false);
            }
            let mut matched = false;
            if let Some(token) = derived_platform {
                if let Some(idx) = entries
                    .iter()
                    .position(|entry| json_entry_matches_platform(entry, token))
                {
                    matched = true;
                    if idx != 0 {
                        let entry = entries.remove(idx);
                        entries.insert(0, entry);
                    }
                }
            }
            (Some(serde_json::Value::Array(entries)), matched)
        }
        other => (Some(other), false),
    }
}

/// Test hooks: allow tests to override remote base URLs without touching process env.
pub mod test_hooks {
    // intentionally not importing everything from super; access module-level overrides directly

    pub fn set_hasheous_base_override(val: &str) {
        let mut o = super::HASHEOUS_OVERRIDE.lock().unwrap();
        *o = Some(val.to_string());
    }

    pub fn clear_hasheous_base_override() {
        let mut o = super::HASHEOUS_OVERRIDE.lock().unwrap();
        *o = None;
    }

    pub fn set_igdb_base_override(val: &str) {
        let mut o = super::IGDB_OVERRIDE.lock().unwrap();
        *o = Some(val.to_string());
    }

    pub fn clear_igdb_base_override() {
        let mut o = super::IGDB_OVERRIDE.lock().unwrap();
        *o = None;
    }
}

pub fn online_lookup(records: &[FileRecord], config: &Config) -> anyhow::Result<Vec<OnlineMatch>> {
    if !config.enable_hasheous && config.igdb_client_id.is_none() {
        return Ok(Vec::new());
    }
    // Build a default blocking client with a modest timeout so lookups don't hang indefinitely.
    let timeout = std::time::Duration::from_secs(config.online_timeout_secs.unwrap_or(5));
    let default_client = Client::builder().timeout(timeout).build()?;
    let cache_handle = cache::Cache::open(config.cache_db.as_ref(), config.output.as_ref()).ok();

    online_lookup_with_client(records, config, &default_client, cache_handle.as_ref())
}

/// Same as `online_lookup` but accepts a reqwest blocking client for test injection.
pub fn online_lookup_with_client(
    records: &[FileRecord],
    config: &Config,
    client: &Client,
    cache: Option<&cache::Cache>,
) -> anyhow::Result<Vec<OnlineMatch>> {
    if !config.enable_hasheous && config.igdb_client_id.is_none() {
        return Ok(Vec::new());
    }

    let mut results = Vec::new();

    for record in records {
        let cache_key = cache_key_for_record(record);
        let mut hasheous_result = None;
        if config.enable_hasheous {
            if let Some(c) = cache {
                if let Ok(Some(j)) = c.get_hasheous_raw_by_key(&cache_key) {
                    hasheous_result = Some(j);
                }
            }

            if hasheous_result.is_none() && !config.cache_only {
                // Build ordered list of (alg, hash) pairs from available checksums on the input file.
                // New preference order: md5, sha1, crc32, sha256
                let mut candidates: Vec<(&str, &str)> = Vec::new();
                if let Some(h) = record.checksums.md5.as_deref() {
                    candidates.push(("md5", h));
                }
                if let Some(h) = record.checksums.sha1.as_deref() {
                    candidates.push(("sha1", h));
                }
                if let Some(h) = record.checksums.crc32.as_deref() {
                    candidates.push(("crc32", h));
                }
                if let Some(h) = record.checksums.sha256.as_deref() {
                    candidates.push(("sha256", h));
                }

                let all_algs = ["md5", "sha1", "crc32", "sha256"];
                let max_retries = config.online_max_retries.unwrap_or(3);
                let throttle_ms = config.online_throttle_ms;
                'outer: for (alg, hash) in candidates.iter() {
                    if let Ok(Some(v)) =
                        query_hasheous(client, alg, hash, config.verbose, max_retries, throttle_ms)
                    {
                        hasheous_result = Some(v);
                        break 'outer;
                    }

                    for alt in &all_algs {
                        if alt == alg {
                            continue;
                        }
                        if !hash_length_matches(alt, hash) {
                            continue;
                        }
                        if let Ok(Some(v)) = query_hasheous(
                            client,
                            alt,
                            hash,
                            config.verbose,
                            max_retries,
                            throttle_ms,
                        ) {
                            hasheous_result = Some(v);
                            break 'outer;
                        }
                    }
                }

                if let (Some(json), Some(c)) = (&hasheous_result, cache) {
                    let _ = c.set_hasheous_raw_by_key(&cache_key, &record.source, json);
                }
            }
        }

        let mut igdb_result = None;
        let needs_igdb = config.should_attempt_igdb_lookup(record);
        if needs_igdb && config.igdb_client_id.is_some() {
            if let Some(raw_name) = record.relative.file_name().and_then(|n| n.to_str()) {
                let name = normalize_name(raw_name);
                if !name.is_empty() {
                    let cache_key_name = name.to_ascii_lowercase();
                    if let Some(c) = cache {
                        if let Ok(Some(entry)) = c.get_igdb_entry_by_key(&cache_key_name) {
                            let mut cache_valid = true;
                            if let Some(derived) = record.derived_platform.as_deref() {
                                if !igdb_cache_entry_matches_platform(&entry, derived) {
                                    cache_valid = false;
                                    vprintln!(
                                        config.verbose,
                                        2,
                                        "CACHE-INVALID igdb: {} key={} derived_platform={} cached_platforms={:?}",
                                        record.relative.to_string_lossy(),
                                        cache_key_name,
                                        derived,
                                        entry.platforms
                                    );
                                    let _ = c.delete_igdb_key(&cache_key_name);
                                }
                            }
                            if cache_valid {
                                igdb_result = Some(entry.json);
                            }
                        }
                    }

                    if igdb_result.is_none() {
                        if config.cache_only {
                            vprintln!(
                                config.verbose,
                                2,
                                "CACHE-MISS igdb (online lookup): {} key={}",
                                record.relative.to_string_lossy(),
                                cache_key_name
                            );
                        } else {
                            igdb_result = query_igdb(
                                &name,
                                config,
                                client,
                                record.derived_platform.as_deref(),
                            )
                            .ok()
                            .flatten();
                            if let (Some(json), Some(c)) = (&igdb_result, cache) {
                                let _ = c.set_igdb_raw_by_key(&cache_key_name, json);
                            }
                        }
                    }
                }
            }
        }

        if hasheous_result.is_some() || igdb_result.is_some() {
            results.push(OnlineMatch {
                name: record.relative.to_string_lossy().to_string(),
                source_dat: None,
                source_path: Some(record.source.clone()),
                hasheous: hasheous_result,
                igdb: igdb_result,
            });
        }
    }

    Ok(results)
}

fn cache_key_for_record(record: &FileRecord) -> String {
    record
        .checksums
        .sha256
        .clone()
        .or_else(|| record.checksums.sha1.clone())
        .or_else(|| record.checksums.md5.clone())
        .or_else(|| record.checksums.crc32.clone())
        .unwrap_or_else(|| record.source.to_string_lossy().to_string())
}

fn hash_length_matches(alg: &str, hash: &str) -> bool {
    match alg {
        "sha1" => hash.len() == 40,
        "md5" => hash.len() == 32,
        "sha256" => hash.len() == 64,
        "crc32" => hash.len() == 8,
        _ => true,
    }
}

pub fn scan_inputs_and_dats(
    config: &Config,
) -> anyhow::Result<(Vec<FileRecord>, Vec<DatRom>, Vec<OnlineMatch>)> {
    let records = collect_files(config, None)?.records;
    let dat_roms = load_dat_roms(config, None)?;
    let _ = dat_unmatched(&records, &dat_roms);
    let missing_records = records_without_dat_match(&records, &dat_roms);
    let online = online_lookup(&missing_records, config)?;
    Ok((records, dat_roms, online))
}

/// Group DatRom entries that appear to be multi-file sets by common prefix tokens.
/// Returns a map from prefix -> Vec<DatRom>
pub fn group_multi_file_roms(
    dat_roms: &[DatRom],
) -> std::collections::HashMap<String, Vec<DatRom>> {
    let mut map: std::collections::HashMap<String, Vec<DatRom>> = std::collections::HashMap::new();
    for rom in dat_roms.iter() {
        // Try to detect separators like "Disc 1", "(1)", "- Part 1"
        let name = rom.name.as_str();
        let raw_key = if let Some(idx) = name.to_lowercase().find("disc") {
            name[..idx].trim().to_lowercase()
        } else if let Some(idx) = name.rfind('(') {
            name[..idx].trim().to_lowercase()
        } else if let Some(idx) = name.rfind('-') {
            name[..idx].trim().to_lowercase()
        } else {
            name.to_lowercase()
        };
        // remove trailing separators like '-' or '_' that may remain after cutting
        // e.g. "Game - Disc 1" -> raw_key == "game -" -> trim the trailing '-' to get "game"
        let raw_key = raw_key
            .trim()
            .trim_end_matches('-')
            .trim_end_matches('_')
            .trim()
            .to_string();
        // capitalize first letter for nicer display (match normalize_title)
        let mut c = raw_key.chars();
        let key = match c.next() {
            None => String::new(),
            Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
        };
        map.entry(key).or_default().push(rom.clone());
    }
    map
}

/// Given grouped multi-file roms (prefix -> entries), create logical combined entries
/// where appropriate (e.g., Disc 1 + Disc 2 -> single logical set key)
pub fn combine_multi_file_sets(
    grouped: &std::collections::HashMap<String, Vec<DatRom>>,
) -> std::collections::HashMap<String, Vec<DatRom>> {
    let mut out = std::collections::HashMap::new();
    for (k, v) in grouped.iter() {
        if v.len() > 1 {
            // assume these are parts of the same set; sort by name and keep as one combined list
            let mut vec = v.clone();
            vec.sort_by(|a, b| a.name.cmp(&b.name));
            out.insert(k.clone(), vec);
        } else {
            out.insert(k.clone(), v.clone());
        }
    }
    out
}

#[cfg(test)]
mod dat_combine_tests {
    use super::*;

    #[test]
    fn combine_disc_sets() {
        let roms = vec![
            DatRom {
                name: "Game Disc 1".to_string(),
                description: None,
                source_dat: PathBuf::from("d1"),
                size: None,
                crc32: None,
                md5: None,
                sha1: None,
                sha256: None,
                match_reasons: None,
            },
            DatRom {
                name: "Game Disc 2".to_string(),
                description: None,
                source_dat: PathBuf::from("d1"),
                size: None,
                crc32: None,
                md5: None,
                sha1: None,
                sha256: None,
                match_reasons: None,
            },
        ];

        let grouped = group_multi_file_roms(&roms);
        let combined = combine_multi_file_sets(&grouped);
        assert!(combined.contains_key("Game"));
        let items = combined.get("Game").unwrap();
        assert_eq!(items.len(), 2);
        assert!(items[0].name.contains("Disc"));
    }
}

#[cfg(test)]
mod dat_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn groups_multifile_by_prefix() {
        let roms = vec![
            DatRom {
                name: "Game Disc 1".to_string(),
                description: None,
                source_dat: PathBuf::from("d1"),
                size: None,
                crc32: None,
                md5: None,
                sha1: None,
                sha256: None,
                match_reasons: None,
            },
            DatRom {
                name: "Game Disc 2".to_string(),
                description: None,
                source_dat: PathBuf::from("d1"),
                size: None,
                crc32: None,
                md5: None,
                sha1: None,
                sha256: None,
                match_reasons: None,
            },
            DatRom {
                name: "OtherGame (1)".to_string(),
                description: None,
                source_dat: PathBuf::from("d2"),
                size: None,
                crc32: None,
                md5: None,
                sha1: None,
                sha256: None,
                match_reasons: None,
            },
        ];

        let grouped = group_multi_file_roms(&roms);
        // Expect 'Game' prefix to have 2 entries (capitalized key)
        assert!(grouped.contains_key("Game"));
        assert_eq!(grouped.get("Game").unwrap().len(), 2);
        assert!(grouped.contains_key("Othergame"));
    }

    #[test]
    fn rom_matches_checksum_precedence() {
        use crate::types::{ChecksumSet, FileRecord};
        let rec = FileRecord {
            source: PathBuf::from("s"),
            relative: PathBuf::from("r.bin"),
            size: 10,
            checksums: ChecksumSet {
                crc32: Some("abcd1234".to_string()),
                md5: Some("md5val".to_string()),
                sha1: Some("sha1val".to_string()),
                sha256: None,
            },
            letter_dir: None,
            derived_platform: None,
            derived_genres: Vec::new(),
            derived_region: None,
            derived_languages: Vec::new(),
            scan_info: None,
        };
        // dat with sha1 should match
        let dat = DatRom {
            name: "r.bin".to_string(),
            description: None,
            source_dat: PathBuf::from("d"),
            size: Some(10),
            crc32: None,
            md5: None,
            sha1: Some("sha1val".to_string()),
            sha256: None,
            match_reasons: None,
        };
        assert!(rom_matches(&rec, &dat));
        // dat with md5 should match
        let dat2 = DatRom {
            name: "r.bin".to_string(),
            description: None,
            source_dat: PathBuf::from("d"),
            size: Some(10),
            crc32: None,
            md5: Some("md5val".to_string()),
            sha1: None,
            sha256: None,
            match_reasons: None,
        };
        assert!(rom_matches(&rec, &dat2));
        // dat with crc matching case-insensitive
        let dat3 = DatRom {
            name: "r.bin".to_string(),
            description: None,
            source_dat: PathBuf::from("d"),
            size: Some(10),
            crc32: Some("ABCD1234".to_string()),
            md5: None,
            sha1: None,
            sha256: None,
            match_reasons: None,
        };
        assert!(rom_matches(&rec, &dat3));
    }

    #[test]
    fn rom_matches_size_name() {
        use crate::types::{ChecksumSet, FileRecord};
        let rec = FileRecord {
            source: PathBuf::from("s"),
            relative: PathBuf::from("game.bin"),
            size: 123,
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
        };
        let dat = DatRom {
            name: "game.bin".to_string(),
            description: None,
            source_dat: PathBuf::from("d"),
            size: Some(123),
            crc32: None,
            md5: None,
            sha1: None,
            sha256: None,
            match_reasons: None,
        };
        assert!(rom_matches(&rec, &dat));
    }

    #[test]
    fn normalize_name_drops_revision_tokens() {
        let plan = normalize_name_with_keywords("Donkey Kong Rev 1 (USA).sfc");
        assert_eq!(plan.keywords, vec!["Donkey", "Kong"]);
        assert_eq!(plan.normalized, "Donkey Kong");
    }

    #[test]
    fn normalize_name_keeps_non_revision_numbers() {
        let plan = normalize_name_with_keywords("Super Mario World 2.sfc");
        assert!(plan.keywords.contains(&"2".to_string()));
        assert_eq!(plan.normalized, "Super Mario World 2");
    }

    #[test]
    fn build_search_terms_adds_filtered_variants() {
        let plan = normalize_name_with_keywords("MX 2002 Featuring Ricky Carmichael.gba");
        let clause = igdb_platform_filter_clause("gba").expect("expected clause");
        let terms = build_igdb_search_terms(&plan, Some("gba"), Some(clause.as_str()));
        assert!(!terms.is_empty());
        assert!(terms[0].1, "expected filtered query first");
        assert_eq!(terms.iter().filter(|(_, filtered)| *filtered).count(), 1);
        assert!(
            terms
                .iter()
                .any(|(term, _)| term.to_ascii_lowercase().contains("mx featuring"))
        );
    }

    #[test]
    fn short_titles_emit_single_keywords() {
        let plan = normalize_name_with_keywords("Aero.gba");
        let clause = igdb_platform_filter_clause("gba").expect("expected clause");
        let terms = build_igdb_search_terms(&plan, Some("gba"), Some(clause.as_str()));
        assert!(
            terms
                .iter()
                .any(|(term, _)| term.eq_ignore_ascii_case("Aero"))
        );
    }

    #[test]
    fn prioritize_results_reorders_without_dropping_entries() {
        let payload = json!([
            {
                "name": "Console Release",
                "platforms": [ { "slug": "xbox" } ]
            },
            {
                "name": "Handheld Release",
                "platforms": [ { "slug": "gba" } ]
            }
        ]);
        let (result, matched) = prioritize_results_for_platform(payload, Some("gba"));
        assert!(matched);
        let array = result.unwrap().as_array().cloned().unwrap();
        assert_eq!(array.len(), 2);
        assert_eq!(array[0]["name"], "Handheld Release");
        assert_eq!(array[1]["name"], "Console Release");
    }

    #[test]
    fn dat_unmatched_counts_correctly() {
        use crate::types::{ChecksumSet, FileRecord};
        let rec = FileRecord {
            source: PathBuf::from("s"),
            relative: PathBuf::from("a.bin"),
            size: 1,
            checksums: ChecksumSet {
                crc32: Some("01".to_string()),
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
        };
        let dats = vec![
            DatRom {
                name: "a.bin".to_string(),
                description: None,
                source_dat: PathBuf::from("d1"),
                size: None,
                crc32: Some("01".to_string()),
                md5: None,
                sha1: None,
                sha256: None,
                match_reasons: None,
            },
            DatRom {
                name: "b.bin".to_string(),
                description: None,
                source_dat: PathBuf::from("d2"),
                size: None,
                crc32: None,
                md5: None,
                sha1: None,
                sha256: None,
                match_reasons: None,
            },
        ];
        let (unmatched, matched) = dat_unmatched(&[rec], &dats);
        // CRC32-only dat entries without a size should not be considered a match
        // under the stricter CRC+size policy.
        assert_eq!(matched, 0);
        assert_eq!(unmatched.len(), 2);
    }

    #[test]
    fn grouping_and_combine_ordering() {
        let roms = vec![
            DatRom {
                name: "Game - Disc 2".to_string(),
                description: None,
                source_dat: PathBuf::from("d"),
                size: None,
                crc32: None,
                md5: None,
                sha1: None,
                sha256: None,
                match_reasons: None,
            },
            DatRom {
                name: "Game - Disc 1".to_string(),
                description: None,
                source_dat: PathBuf::from("d"),
                size: None,
                crc32: None,
                md5: None,
                sha1: None,
                sha256: None,
                match_reasons: None,
            },
        ];
        let grouped = group_multi_file_roms(&roms);
        assert!(grouped.contains_key("Game"));
        let combined = combine_multi_file_sets(&grouped);
        let items = combined.get("Game").unwrap();
        // after sorting names, Disc 1 should come before Disc 2
        assert!(items[0].name.contains("Disc 1"));
    }

    #[test]
    fn hash_length_guard_respects_algorithms() {
        let sha1 = "a".repeat(40);
        assert!(hash_length_matches("sha1", &sha1));
        assert!(!hash_length_matches("sha1", "abcd"));

        let crc = "abcd1234";
        assert!(hash_length_matches("crc32", crc));
        assert!(!hash_length_matches("md5", crc));
    }
}
