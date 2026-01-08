use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{Read, Write};
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::sync::mpsc::{self, TryRecvError};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow};
use serde::Serialize;
use zip::write::FileOptions;

use tempfile::tempdir;
use walkdir::WalkDir;
use which::which;

use crate::cache;
use crate::config::Config;
use crate::dat::{
    DatIndex, load_dat_roms, online_lookup, partition_dat_matches,
    records_without_dat_match_with_index,
};
use crate::game_console::record_is_cartridge_based;
use crate::progress::{BackgroundTask, ProgressReporter};
use crate::records::{
    collect_files, ensure_parent, populate_locale_tokens, resolve_output_path,
    resolve_output_path_with_dats,
};
use crate::types::{
    Action, ActionOutcome, Checksum, ChecksumSet, ExecutionPlan, FileRecord, FilterSummary,
    IgdbLookupMode, LinkMode, RunSummary, SkipReason, SkipSummary, SkippedFile, ZipFormat,
};
use crate::utils::build_globset;

enum ActionProgress {
    ItemBytes {
        path: PathBuf,
        bytes_done: u64,
        total_bytes: Option<u64>,
    },
}

#[derive(Clone)]
pub struct ActionProgressHandle {
    tx: mpsc::Sender<ActionProgress>,
    path: PathBuf,
}

impl ActionProgressHandle {
    fn new(tx: mpsc::Sender<ActionProgress>, path: PathBuf) -> Self {
        Self { tx, path }
    }

    pub fn report_bytes(&self, bytes_done: u64, total_bytes: Option<u64>) {
        let _ = self.tx.send(ActionProgress::ItemBytes {
            path: self.path.clone(),
            bytes_done,
            total_bytes,
        });
    }
}

fn log_diag_step(progress: Option<&ProgressReporter>, enabled: bool, message: impl Into<String>) {
    if !enabled {
        return;
    }
    if let Some(p) = progress {
        p.log_diag(message.into());
    }
}

fn record_diag_duration(
    phase: &str,
    elapsed: Duration,
    progress: Option<&ProgressReporter>,
    enabled: bool,
    timings: &mut Vec<(String, Duration)>,
) {
    if !enabled {
        return;
    }
    timings.push((phase.to_string(), elapsed));
    log_diag_step(
        progress,
        enabled,
        format!(
            "phase={} elapsed_ms={:.2}",
            phase,
            elapsed.as_secs_f64() * 1000.0
        ),
    );
}

fn with_diag_timing<T, F>(
    phase: &str,
    progress: Option<&ProgressReporter>,
    enabled: bool,
    timings: &mut Vec<(String, Duration)>,
    work: F,
) -> anyhow::Result<T>
where
    F: FnOnce() -> anyhow::Result<T>,
{
    if enabled {
        if let Some(p) = progress {
            p.begin_diag_phase(phase);
        }
    }
    let start = Instant::now();
    let result = work();
    let elapsed = start.elapsed();
    if enabled {
        if let Some(p) = progress {
            let summary = format!("{:.2}s", elapsed.as_secs_f64());
            p.finish_diag_phase(phase, Some(summary));
        }
    }
    record_diag_duration(phase, elapsed, progress, enabled, timings);
    result
}

fn begin_net_lookup(
    progress: Option<&ProgressReporter>,
    net_goal: &mut usize,
    hint: &Path,
) -> Option<Instant> {
    if let Some(p) = progress {
        *net_goal = net_goal.saturating_add(1);
        p.hint_background_task_total(BackgroundTask::NetLookup, Some(*net_goal));
        p.tick_background_task(BackgroundTask::NetLookup, 1, Some(hint));
        return Some(Instant::now());
    }
    None
}

fn finish_net_lookup(progress: Option<&ProgressReporter>, started: Option<Instant>) {
    if let (Some(p), Some(start)) = (progress, started) {
        p.record_background_task_latency(BackgroundTask::NetLookup, start.elapsed());
    }
}

fn query_hasheous_with_progress(
    progress: Option<&ProgressReporter>,
    net_goal: &mut usize,
    hint: &Path,
    client: &reqwest::blocking::Client,
    alg: &str,
    hash: &str,
    verbose: u8,
    max_retries: usize,
    throttle_ms: Option<u64>,
) -> anyhow::Result<Option<serde_json::Value>> {
    let started = begin_net_lookup(progress, net_goal, hint);
    let result = crate::dat::query_hasheous(client, alg, hash, verbose, max_retries, throttle_ms);
    finish_net_lookup(progress, started);
    result
}

fn query_igdb_with_progress(
    progress: Option<&ProgressReporter>,
    net_goal: &mut usize,
    hint: &Path,
    name: &str,
    config: &Config,
    client: &reqwest::blocking::Client,
    platform_hint: Option<&str>,
) -> anyhow::Result<Option<serde_json::Value>> {
    let started = begin_net_lookup(progress, net_goal, hint);
    let result = crate::dat::query_igdb(name, config, client, platform_hint);
    finish_net_lookup(progress, started);
    result
}

fn best_checksum_key(checksums: &ChecksumSet) -> Option<String> {
    if let Some(value) = &checksums.sha256 {
        return Some(value.clone());
    }
    if let Some(value) = &checksums.sha1 {
        return Some(format!("sha1:{value}"));
    }
    if let Some(value) = &checksums.md5 {
        return Some(format!("md5:{value}"));
    }
    if let Some(value) = &checksums.crc32 {
        return Some(format!("crc32:{value}"));
    }
    None
}

fn merge_checksum_sets(target: &mut ChecksumSet, source: &ChecksumSet) {
    if target.crc32.is_none() {
        target.crc32 = source.crc32.clone();
    }
    if target.md5.is_none() {
        target.md5 = source.md5.clone();
    }
    if target.sha1.is_none() {
        target.sha1 = source.sha1.clone();
    }
    if target.sha256.is_none() {
        target.sha256 = source.sha256.clone();
    }
}

// Extract a platform name string from Hasheous JSON blobs. Placed at module
// scope so it can be reused in multiple places during enrichment and cache
// handling.
fn extract_platform_from_hasheous(v: &serde_json::Value) -> Option<String> {
    // Common shape: { "platform": { "name": "..." } }
    if let Some(pname) = v
        .get("platform")
        .and_then(|p| p.get("name"))
        .and_then(|n| n.as_str())
    {
        return Some(pname.to_string());
    }
    // Sometimes 'platforms' is an array of strings or objects
    if let Some(arr) = v.get("platforms").and_then(|a| a.as_array()) {
        for e in arr {
            if let Some(s) = e.as_str() {
                return Some(s.to_string());
            }
            if let Some(n) = e.get("name").and_then(|n| n.as_str()) {
                return Some(n.to_string());
            }
        }
    }
    // Other possible fields
    if let Some(s) = v.get("console").and_then(|c| c.as_str()) {
        return Some(s.to_string());
    }
    if let Some(s) = v.get("system").and_then(|c| c.as_str()) {
        return Some(s.to_string());
    }
    // metadata array entries may include platform-like info
    if let Some(meta) = v.get("metadata").and_then(|m| m.as_array()) {
        for obj in meta {
            if let Some(src) = obj.get("source").and_then(|s| s.as_str()) {
                if src.eq_ignore_ascii_case("platform") || src.eq_ignore_ascii_case("system") {
                    if let Some(id) = obj.get("id").and_then(|i| i.as_str()) {
                        return Some(id.to_string());
                    }
                    if let Some(n) = obj.get("name").and_then(|n| n.as_str()) {
                        return Some(n.to_string());
                    }
                }
            }
        }
    }
    // Fallback: signature payloads often carry the canonical console string
    if let Some(sig) = v.get("signature") {
        if let Some(game) = sig.get("game") {
            if let Some(system) = game.get("system").and_then(|s| s.as_str()) {
                return Some(system.to_string());
            }
            if let Some(variant) = game.get("systemVariant").and_then(|s| s.as_str()) {
                return Some(variant.to_string());
            }
        }
        if let Some(platform) = sig
            .get("platform")
            .and_then(|p| p.get("name"))
            .and_then(|n| n.as_str())
        {
            return Some(platform.to_string());
        }
    }
    None
}

fn extract_platform_from_igdb(v: &serde_json::Value) -> Vec<String> {
    let mut identifiers = Vec::new();
    if let Some(entries) = v.as_array() {
        for entry in entries {
            if let Some(platforms) = entry.get("platforms").and_then(|p| p.as_array()) {
                for plat in platforms {
                    if let Some(name) = plat.get("name").and_then(|n| n.as_str()) {
                        identifiers.push(name.to_string());
                    }
                    if let Some(slug) = plat.get("slug").and_then(|s| s.as_str()) {
                        identifiers.push(slug.to_string());
                    }
                    if let Some(abbr) = plat.get("abbreviation").and_then(|a| a.as_str()) {
                        identifiers.push(abbr.to_string());
                    }
                }
            }
        }
    }
    identifiers
}

fn extract_genres_from_igdb(v: &serde_json::Value) -> Vec<String> {
    let mut genres: Vec<String> = Vec::new();
    if let Some(entries) = v.as_array() {
        for entry in entries {
            if let Some(raw_genres) = entry.get("genres").and_then(|g| g.as_array()) {
                for genre in raw_genres {
                    if let Some(name) = genre.get("name").and_then(|n| n.as_str()) {
                        let trimmed = name.trim();
                        if trimmed.is_empty() {
                            continue;
                        }
                        if genres
                            .iter()
                            .any(|existing| existing.eq_ignore_ascii_case(trimmed))
                        {
                            continue;
                        }
                        genres.push(trimmed.to_string());
                    }
                }
            }
        }
    }
    genres
}

fn capture_genres_from_igdb(
    record: &mut FileRecord,
    json: &serde_json::Value,
    label: &str,
    verbose: u8,
) -> bool {
    if !record.derived_genres.is_empty() {
        return false;
    }
    let genres = extract_genres_from_igdb(json);
    if genres.is_empty() {
        return false;
    }
    record.derived_genres = genres.clone();
    vprintln!(
        verbose,
        2,
        "{} genres derived for {} -> {}",
        label,
        record.relative.to_string_lossy(),
        genres.join(", ")
    );
    true
}

fn extract_parent_id_from_igdb(json: &serde_json::Value) -> Option<i64> {
    let entries = json.as_array()?;
    let first = entries.first()?;
    if let Some(id) = first.get("version_parent").and_then(|v| v.as_i64()) {
        return Some(id);
    }
    if let Some(parent) = first.get("parent_game") {
        if let Some(id) = parent.as_i64() {
            return Some(id);
        }
        if let Some(obj_id) = parent.get("id").and_then(|v| v.as_i64()) {
            return Some(obj_id);
        }
    }
    None
}

fn igdb_parent_cache_key(id: i64) -> String {
    format!("id:{id}")
}

fn merge_parent_genres_into_child(
    child: &mut serde_json::Value,
    parent: &serde_json::Value,
) -> bool {
    let entries = match child.as_array_mut() {
        Some(entries) if !entries.is_empty() => entries,
        _ => return false,
    };
    let child_entry = &mut entries[0];
    let parent_entries = match parent.as_array() {
        Some(entries) if !entries.is_empty() => entries,
        _ => return false,
    };
    if child_entry
        .get("genres")
        .and_then(|g| g.as_array())
        .map(|arr| !arr.is_empty())
        .unwrap_or(false)
    {
        return false;
    }
    if let Some(parent_genres) = parent_entries[0].get("genres") {
        child_entry["genres"] = parent_genres.clone();
        return true;
    }
    false
}

fn graft_parent_genres_onto_child(
    child: &serde_json::Value,
    parent: &serde_json::Value,
) -> Option<serde_json::Value> {
    let child_entries = child.as_array()?;
    let parent_entries = parent.as_array()?;
    let first_child = child_entries.first()?.clone();
    let parent_genres = parent_entries
        .first()
        .and_then(|entry| entry.get("genres"))?
        .clone();
    let mut combined_entry = first_child;
    combined_entry["genres"] = parent_genres;
    Some(serde_json::Value::Array(vec![combined_entry]))
}

fn fetch_parent_entry(
    parent_id: i64,
    cache: Option<&cache::Cache>,
    config: &Config,
    igdb_client: Option<&reqwest::blocking::Client>,
) -> Option<serde_json::Value> {
    let key = igdb_parent_cache_key(parent_id);
    if let Some(db) = cache {
        if let Ok(Some(entry)) = db.get_igdb_entry_by_key(&key) {
            return Some(entry.json.clone());
        }
    }
    let client = igdb_client?;
    let json = crate::dat::query_igdb_by_id(parent_id, config, client)
        .ok()
        .flatten()?;
    if let Some(db) = cache {
        let _ = db.set_igdb_raw_by_key(&key, &json);
    }
    Some(json)
}

fn ensure_genres_from_igdb_sources(
    record: &mut FileRecord,
    json: &mut serde_json::Value,
    label: &str,
    cache_key: Option<&str>,
    cache: Option<&cache::Cache>,
    config: &Config,
    igdb_client: Option<&reqwest::blocking::Client>,
) -> bool {
    if capture_genres_from_igdb(record, json, label, config.verbose) {
        return true;
    }
    let Some(parent_id) = extract_parent_id_from_igdb(json) else {
        return false;
    };
    let Some(parent_json) = fetch_parent_entry(parent_id, cache, config, igdb_client) else {
        return false;
    };
    let merged = merge_parent_genres_into_child(json, &parent_json);
    if merged {
        if capture_genres_from_igdb(record, json, "IGDB-parent", config.verbose) {
            if let (Some(key), Some(db)) = (cache_key, cache) {
                let _ = db.set_igdb_raw_by_key(key, json);
            }
            return true;
        }
    }
    if capture_genres_from_igdb(record, &parent_json, "IGDB-parent", config.verbose) {
        if let (Some(key), Some(db)) = (cache_key, cache) {
            if let Some(combined) = graft_parent_genres_onto_child(json, &parent_json) {
                *json = combined.clone();
                let _ = db.set_igdb_raw_by_key(key, &combined);
            } else {
                let _ = db.set_igdb_raw_by_key(key, &parent_json);
            }
        }
        return true;
    }
    false
}

fn apply_cached_igdb_entry(
    record: &mut FileRecord,
    entry: &cache::IgdbCacheEntry,
    label: &str,
    config: &Config,
) -> bool {
    let mut updated = false;
    let extension_hint = crate::game_console::romm_from_extension(&record.relative);
    if record.derived_platform.is_none() && !entry.platforms.is_empty() {
        let preferred_token = record
            .derived_platform
            .as_deref()
            .or(extension_hint.as_deref());
        if let Some((tok, identifier)) =
            resolve_igdb_platform_token(&entry.platforms, preferred_token)
        {
            if should_accept_platform_override(record, &tok) {
                record.derived_platform = Some(tok.clone());
                vprintln!(
                    config.verbose,
                    2,
                    "{} platform derived: {} platform_token={} identifier={}",
                    label,
                    record.relative.to_string_lossy(),
                    tok,
                    identifier
                );
                updated = true;
            }
        }
    }

    if record.derived_genres.is_empty() && !entry.genres.is_empty() {
        record.derived_genres = entry.genres.clone();
        vprintln!(
            config.verbose,
            2,
            "{} genres derived for {} -> {}",
            label,
            record.relative.to_string_lossy(),
            entry.genres.join(", ")
        );
        updated = true;
    }

    if record.derived_platform.is_none() {
        let identifiers = extract_platform_from_igdb(&entry.json);
        let preferred_token = record
            .derived_platform
            .as_deref()
            .or(extension_hint.as_deref());
        if let Some((tok, identifier)) = resolve_igdb_platform_token(&identifiers, preferred_token)
        {
            if should_accept_platform_override(record, &tok) {
                record.derived_platform = Some(tok.clone());
                vprintln!(
                    config.verbose,
                    2,
                    "{} platform derived via json: {} platform_token={} identifier={}",
                    label,
                    record.relative.to_string_lossy(),
                    tok,
                    identifier
                );
                updated = true;
            }
        }
    }

    if record.derived_genres.is_empty() {
        if capture_genres_from_igdb(record, &entry.json, label, config.verbose) {
            updated = true;
        }
    }

    updated
}

pub(crate) fn normalize_igdb_slug_candidate(raw: &str) -> Option<String> {
    let mut slug = String::new();
    let trimmed = raw.trim().trim_matches(|c| c == '"' || c == '\'');
    if trimmed.is_empty() {
        return None;
    }
    for ch in trimmed.trim_matches('/').chars() {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch.to_ascii_lowercase());
        } else if matches!(ch, '-' | '_' | ' ') {
            if !slug.ends_with('-') {
                slug.push('-');
            }
        }
    }
    let normalized = slug.trim_matches('-').to_string();
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

fn slug_from_igdb_url(input: &str) -> Option<String> {
    let lower = input.to_ascii_lowercase();
    let marker = if let Some(idx) = lower.find("/games/") {
        idx + 7
    } else if let Some(idx) = lower.find("/game/") {
        idx + 6
    } else {
        return None;
    };
    let tail = &input[marker..];
    let segment = tail.split(['/', '?', '#']).next().unwrap_or_default();
    normalize_igdb_slug_candidate(segment)
}

fn extract_igdb_slugs_from_hasheous(v: &serde_json::Value) -> Vec<String> {
    fn push_slug(slugs: &mut Vec<String>, slug: String) {
        if !slugs
            .iter()
            .any(|existing| existing.eq_ignore_ascii_case(&slug))
        {
            slugs.push(slug);
        }
    }

    fn visit(value: &serde_json::Value, slugs: &mut Vec<String>) {
        match value {
            serde_json::Value::String(s) => {
                if s.contains("igdb.com") {
                    if let Some(slug) = slug_from_igdb_url(s) {
                        push_slug(slugs, slug);
                    }
                }
            }
            serde_json::Value::Array(arr) => {
                for item in arr {
                    visit(item, slugs);
                }
            }
            serde_json::Value::Object(map) => {
                if let Some(source) = map.get("source").and_then(|s| s.as_str()) {
                    if source.eq_ignore_ascii_case("igdb") {
                        if let Some(id) = map.get("id").and_then(|i| i.as_str()) {
                            if let Some(slug) = normalize_igdb_slug_candidate(id) {
                                push_slug(slugs, slug);
                            }
                        }
                        if let Some(url) = map.get("url").and_then(|u| u.as_str()) {
                            if let Some(slug) = slug_from_igdb_url(url) {
                                push_slug(slugs, slug);
                            }
                        }
                    }
                }
                for child in map.values() {
                    visit(child, slugs);
                }
            }
            _ => {}
        }
    }

    let mut slugs = Vec::new();
    visit(v, &mut slugs);
    slugs
}

fn hydrate_record_from_igdb_slug(
    record: &mut FileRecord,
    slug: &str,
    cache: Option<&cache::Cache>,
    config: &Config,
    igdb_client: Option<&reqwest::blocking::Client>,
    attempted_slugs: &mut HashSet<String>,
) {
    if slug.is_empty() || !config.igdb_lookup_enabled() {
        return;
    }

    let cache_key = format!("slug:{slug}");
    let mut json_opt = None;
    let mut from_network = false;

    if let Some(c) = cache {
        if let Ok(Some(entry)) = c.get_igdb_entry_by_key(&cache_key) {
            vprintln!(
                config.verbose,
                2,
                "CACHE-HIT igdb slug: {} slug={}",
                record.relative.to_string_lossy(),
                slug
            );
            apply_cached_igdb_entry(record, &entry, "IGDB-slug-cache", config);
            json_opt = Some(entry.json.clone());
        }
    }

    if json_opt.is_none() {
        if config.cache_only {
            vprintln!(
                config.verbose,
                2,
                "CACHE-MISS igdb slug (cache-only): {} slug={}",
                record.relative.to_string_lossy(),
                slug
            );
            return;
        }
        let Some(client) = igdb_client else {
            return;
        };
        if attempted_slugs.contains(slug) {
            vprintln!(
                config.verbose,
                2,
                "SKIPPED igdb slug (already tried): {} slug={}",
                record.relative.to_string_lossy(),
                slug
            );
            return;
        }
        attempted_slugs.insert(slug.to_string());
        if let Some(j) = crate::dat::query_igdb_by_slug(slug, config, client)
            .ok()
            .flatten()
        {
            if let Some(c) = cache {
                let _ = c.set_igdb_raw_by_key(&cache_key, &j);
            }
            json_opt = Some(j);
            from_network = true;
        } else {
            vprintln!(
                config.verbose,
                2,
                "IGDB slug lookup returned no result: {} slug={}",
                record.relative.to_string_lossy(),
                slug
            );
        }
    }

    if from_network {
        if let Some(json) = json_opt.as_mut() {
            if record.derived_platform.is_none() {
                let identifiers = extract_platform_from_igdb(json);
                let extension_hint = crate::game_console::romm_from_extension(&record.relative);
                let preferred_token = record
                    .derived_platform
                    .as_deref()
                    .or(extension_hint.as_deref());
                if let Some((tok, identifier)) =
                    resolve_igdb_platform_token(&identifiers, preferred_token)
                {
                    if should_accept_platform_override(record, &tok) {
                        record.derived_platform = Some(tok.clone());
                        vprintln!(
                            config.verbose,
                            2,
                            "IGDB-slug platform derived: {} platform_token={} identifier={}",
                            record.relative.to_string_lossy(),
                            tok,
                            identifier
                        );
                    }
                }
            }
            ensure_genres_from_igdb_sources(
                record,
                json,
                "IGDB-slug",
                Some(&cache_key),
                cache,
                config,
                igdb_client,
            );
        }
    }
}

fn enrich_record_with_hasheous_igdb(
    record: &mut FileRecord,
    json: &serde_json::Value,
    config: &Config,
    cache: Option<&cache::Cache>,
    igdb_client: Option<&reqwest::blocking::Client>,
    attempted_slugs: &mut HashSet<String>,
) {
    if !config.igdb_lookup_enabled() {
        return;
    }
    let needs_platform = record.derived_platform.is_none();
    let needs_genres = record.derived_genres.is_empty();
    if !needs_platform && !needs_genres {
        return;
    }

    for slug in extract_igdb_slugs_from_hasheous(json) {
        hydrate_record_from_igdb_slug(record, &slug, cache, config, igdb_client, attempted_slugs);
        if !record.derived_genres.is_empty() {
            break;
        }
    }
}

fn resolve_igdb_platform_token(
    identifiers: &[String],
    preferred_token: Option<&str>,
) -> Option<(String, String)> {
    let mut fallback: Option<(String, String)> = None;

    for ident in identifiers {
        let token = if let Some(mapped) = crate::igdb_platform_map::lookup(ident) {
            mapped.to_string()
        } else if let Some(mapped) = crate::game_console::romm_from_platform_name(ident) {
            mapped
        } else {
            continue;
        };

        let candidate = (token.clone(), ident.clone());
        if fallback.is_none() {
            fallback = Some(candidate.clone());
        }

        if let Some(pref) = preferred_token {
            if token == pref {
                return Some(candidate);
            }
        }
    }

    fallback
}

fn should_accept_platform_override(record: &FileRecord, candidate: &str) -> bool {
    match crate::game_console::romm_from_extension(&record.relative) {
        Some(ext) if ext == candidate => true,
        Some(ext) if is_ambiguous_extension_token(&ext) => true,
        Some(_) => false,
        None => true,
    }
}

fn is_ambiguous_extension_token(token: &str) -> bool {
    matches!(token, "cdrom")
}

#[cfg(test)]
mod tests {
    use super::{
        ensure_genres_from_igdb_sources, extract_platform_from_hasheous,
        extract_platform_from_igdb, extract_record, log_diag_step,
        record_diag_duration, record_is_extractable_archive, resolve_igdb_platform_token,
        should_accept_platform_override,
    };
    use crate::cache;
    use crate::config::Config;
    use crate::progress::ProgressReporter;
    use crate::types::ChecksumSet;
    use serde_json::json;
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use std::time::Duration;
    use tempfile::tempdir;
    use zip::write::FileOptions;

    #[test]
    fn extract_platform_from_various_shapes() {
        // shape: { platform: { name: "..." } }
        let v = json!({ "platform": { "name": "Super Nintendo (SNES)" } });
        assert_eq!(
            extract_platform_from_hasheous(&v).as_deref(),
            Some("Super Nintendo (SNES)")
        );

        // shape: { platforms: ["SNES"] }
        let v = json!({ "platforms": ["SNES"] });
        assert_eq!(extract_platform_from_hasheous(&v).as_deref(), Some("SNES"));

        // shape: { platforms: [{ name: "Nintendo 64" }] }
        let v = json!({ "platforms": [{ "name": "Nintendo 64" }] });
        assert_eq!(
            extract_platform_from_hasheous(&v).as_deref(),
            Some("Nintendo 64")
        );

        // fallback: console field
        let v = json!({ "console": "Game Gear" });
        assert_eq!(
            extract_platform_from_hasheous(&v).as_deref(),
            Some("Game Gear")
        );

        // metadata array
        let v = json!({ "metadata": [{ "source": "platform", "name": "Mega Drive" }] });
        assert_eq!(
            extract_platform_from_hasheous(&v).as_deref(),
            Some("Mega Drive")
        );

        // no platform
        let v = json!({ "foo": "bar" });
        assert!(extract_platform_from_hasheous(&v).is_none());
    }

    #[test]
    fn extract_platform_from_signature_fallback() {
        let v = json!({
            "signature": {
                "game": {
                    "system": "Nintendo - Super Nintendo Entertainment System"
                }
            }
        });

        assert_eq!(
            extract_platform_from_hasheous(&v).as_deref(),
            Some("Nintendo - Super Nintendo Entertainment System")
        );
    }

    #[test]
    fn extract_platform_from_igdb_variants() {
        let v = json!([
            {
                "name": "Example",
                "platforms": [
                    { "name": "Super Nintendo Entertainment System" },
                    { "slug": "snes" },
                    { "abbreviation": "SNES" }
                ]
            }
        ]);
        assert_eq!(
            extract_platform_from_igdb(&v),
            vec![
                "Super Nintendo Entertainment System".to_string(),
                "snes".to_string(),
                "SNES".to_string()
            ]
        );

        let v = json!([
            {
                "name": "Example",
                "platforms": [
                    { "slug": "gba" }
                ]
            }
        ]);
        assert_eq!(extract_platform_from_igdb(&v), vec!["gba".to_string()]);

        let v = json!([
            {
                "name": "Example",
                "platforms": [
                    { "abbreviation": "GG" }
                ]
            }
        ]);
        assert_eq!(extract_platform_from_igdb(&v), vec!["GG".to_string()]);

        let v = json!([{ "name": "No platforms" }]);
        assert!(extract_platform_from_igdb(&v).is_empty());
    }

    #[test]
    fn resolve_igdb_platform_token_prefers_known_map() {
        let identifiers = vec!["Handheld Electronic LCD".to_string()];
        let resolved = resolve_igdb_platform_token(&identifiers, None)
            .expect("expected a mapped platform token");
        assert_eq!(resolved.0, "gamegear");
    }

    #[test]
    fn resolve_igdb_platform_token_honors_preference() {
        let identifiers = vec!["Xbox".to_string(), "Game Boy Advance".to_string()];
        let resolved = resolve_igdb_platform_token(&identifiers, Some("gba"))
            .expect("expected a preferred platform token");
        assert_eq!(resolved.0, "gba");
    }

    fn dummy_record(name: &str) -> crate::types::FileRecord {
        crate::types::FileRecord {
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

    #[test]
    fn extension_specific_overrides_block_conflicting_tokens() {
        let record = dummy_record("Denki Blocks!.gba");
        assert!(!should_accept_platform_override(&record, "gbc"));
        assert!(should_accept_platform_override(&record, "gba"));
    }

    #[test]
    fn ambiguous_cdrom_extensions_allow_overrides() {
        let record = dummy_record("Donkey Kong Country.bin");
        assert!(should_accept_platform_override(&record, "snes"));
    }

    fn record_for_source(path: &Path) -> crate::types::FileRecord {
        crate::types::FileRecord {
            source: path.to_path_buf(),
            relative: PathBuf::from(
                path.file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("rom.bin"),
            ),
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

    #[test]
    fn record_is_extractable_archive_detects_known_formats() {
        let zip = record_for_source(Path::new("game.zip"));
        assert!(record_is_extractable_archive(&zip));

        let tar_gz = record_for_source(Path::new("collection.tar.gz"));
        assert!(record_is_extractable_archive(&tar_gz));

        let rom = record_for_source(Path::new("Super Mario World.sfc"));
        assert!(!record_is_extractable_archive(&rom));
    }

    #[test]
    fn parent_genres_cache_preserves_child_identity() {
        let dir = tempdir().unwrap();
        let cache_path = dir.path().join("igdb-cache.sqlite");
        let cache = cache::Cache::open(Some(&cache_path), None).expect("cache opened");

        let parent_json = json!([{
            "id": 2,
            "slug": "parent-game",
            "name": "Parent Game",
            "genres": [ { "id": 10, "name": "Action" } ],
            "platforms": [ { "slug": "gba" } ]
        }]);
        cache
            .set_igdb_raw_by_key("id:2", &parent_json)
            .expect("parent cached");

        let mut record = dummy_record("Child.gba");
        let mut child_json = json!([{
            "id": 1,
            "slug": "child-game",
            "name": "Child Game",
            "genres": [],
            "version_parent": 2,
            "platforms": [ { "slug": "gba" } ]
        }]);

        let merged = ensure_genres_from_igdb_sources(
            &mut record,
            &mut child_json,
            "test",
            Some("child-key"),
            Some(&cache),
            &Config::default(),
            None,
        );
        assert!(merged, "expected parent genres to merge");
        assert_eq!(record.derived_genres, vec!["Action".to_string()]);

        let cached_child = cache
            .get_igdb_entry_by_key("child-key")
            .expect("cache read")
            .expect("child entry");
        assert_eq!(cached_child.slug.as_deref(), Some("child-game"));
        assert_eq!(cached_child.genres, vec!["Action".to_string()]);

        let cached_parent = cache
            .get_igdb_entry_by_key("id:2")
            .expect("cache read")
            .expect("parent entry");
        assert_eq!(cached_parent.slug.as_deref(), Some("parent-game"));
    }

    fn config_with_output(path: &Path) -> Config {
        let mut cfg = Config::default();
        cfg.output = Some(path.to_path_buf());
        cfg
    }

    #[test]
    fn extract_record_only_unzips_valid_archives() {
        let tmp = tempdir().unwrap();
        let out = tmp.path().join("out");
        let cfg = config_with_output(&out);

        let archive_path = tmp.path().join("real.zip");
        {
            let file = std::fs::File::create(&archive_path).unwrap();
            let mut zipw = zip::ZipWriter::new(file);
            zipw.start_file::<_, ()>("inner.txt", FileOptions::default())
                .unwrap();
            zipw.write_all(b"payload").unwrap();
            zipw.finish().unwrap();
        }

        let record = record_for_source(&archive_path);
        let written = extract_record(&record, &cfg).unwrap();
        let expected = out.join("inner.txt");
        assert_eq!(written, vec![expected.clone()]);
        assert_eq!(std::fs::read(expected).unwrap(), b"payload");
    }

    #[test]
    fn extract_record_falls_back_when_zip_invalid() {
        let tmp = tempdir().unwrap();
        let out = tmp.path().join("out");
        let cfg = config_with_output(&out);

        let fake_path = tmp.path().join("fake.zip");
        std::fs::write(&fake_path, b"not a zip").unwrap();

        let record = record_for_source(&fake_path);
        let written = extract_record(&record, &cfg).unwrap();
        let expected = out.join("fake.zip");
        assert_eq!(written, vec![expected.clone()]);
        assert_eq!(std::fs::read(expected).unwrap(), b"not a zip");
    }

    #[test]
    fn log_diag_step_only_runs_when_enabled() {
        crate::progress::force_progress_tty_for_tests(Some(true));
        let cfg = Config::default();
        let progress = ProgressReporter::maybe_new(&cfg).expect("progress reporter");
        log_diag_step(Some(&progress), false, "skip");
        assert!(progress.diag_last_hint_for_tests().is_none());
        log_diag_step(Some(&progress), true, "emit");
        assert_eq!(progress.diag_last_hint_for_tests().as_deref(), Some("emit"));
        drop(progress);
        crate::progress::force_progress_tty_for_tests(None);
    }

    #[test]
    fn record_diag_duration_tracks_timings_when_enabled() {
        crate::progress::force_progress_tty_for_tests(Some(true));
        let cfg = Config::default();
        let progress = ProgressReporter::maybe_new(&cfg).expect("progress reporter");
        let mut timings = Vec::new();
        record_diag_duration(
            "phase1",
            Duration::from_millis(5),
            Some(&progress),
            false,
            &mut timings,
        );
        assert!(timings.is_empty());
        assert!(progress.diag_last_hint_for_tests().is_none());
        record_diag_duration(
            "phase2",
            Duration::from_millis(10),
            Some(&progress),
            true,
            &mut timings,
        );
        assert_eq!(timings.len(), 1);
        assert!(progress
            .diag_last_hint_for_tests()
            .unwrap()
            .contains("phase=phase2"));
        drop(progress);
        crate::progress::force_progress_tty_for_tests(None);
    }
}

fn copy_file_with_progress(
    src: &Path,
    dest: &Path,
    progress: Option<&ActionProgressHandle>,
) -> anyhow::Result<()> {
    let mut reader = fs::File::open(src).with_context(|| format!("opening {src:?} for copy"))?;
    let mut writer = fs::File::create(dest).with_context(|| format!("creating {dest:?} for copy"))?;
    let total = reader
        .metadata()
        .map(|m| m.len())
        .with_context(|| format!("reading metadata for {src:?}"))?;
    let mut buf = vec![0u8; 1 << 20];
    let mut written = 0u64;
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        writer.write_all(&buf[..n])?;
        written = written.saturating_add(n as u64);
        if let Some(handle) = progress {
            handle.report_bytes(written, Some(total));
        }
    }
    writer.flush()?;
    Ok(())
}

pub fn copy_record(
    record: &FileRecord,
    config: &Config,
    dats: Option<&[crate::dat::DatRom]>,
) -> anyhow::Result<PathBuf> {
    let target = resolve_output_path_with_dats(record, config, dats);
    ensure_parent(&target)?;

    if target.exists() {
        if !config.overwrite && !config.overwrite_invalid {
            return Ok(target);
        }
    }

    copy_file_with_progress(&record.source, &target, None)
        .with_context(|| format!("copying {:?} to {:?}", record.source, target))?;
    Ok(target)
}

// New wrappers that accept dats and perform actions using DAT-aware path resolution.
pub fn copy_record_with_dats(
    record: &FileRecord,
    config: &Config,
    dats: Option<&[crate::dat::DatRom]>,
    progress: Option<&ActionProgressHandle>,
) -> anyhow::Result<PathBuf> {
    let target = resolve_output_path_with_dats(record, config, dats);
    ensure_parent(&target)?;

    if target.exists() {
        if !config.overwrite && !config.overwrite_invalid {
            return Ok(target);
        }
    }

    copy_file_with_progress(&record.source, &target, progress)
        .with_context(|| format!("copying {:?} to {:?}", record.source, target))?;
    Ok(target)
}

pub fn move_record_with_dats(
    record: &FileRecord,
    config: &Config,
    dats: Option<&[crate::dat::DatRom]>,
    progress: Option<&ActionProgressHandle>,
) -> anyhow::Result<PathBuf> {
    let target = resolve_output_path_with_dats(record, config, dats);
    ensure_parent(&target)?;

    if target.exists() && !config.overwrite {
        return Ok(target);
    }

    if fs::rename(&record.source, &target).is_err() {
        // On cross-device moves fall back to copy + delete.
        copy_file_with_progress(&record.source, &target, progress)
            .with_context(|| format!("copying {:?} to {:?}", record.source, target))?;
        fs::remove_file(&record.source)
            .with_context(|| format!("removing source after move fallback: {:?}", record.source))?
    }

    if matches!(
        config.move_delete_dirs,
        crate::types::MoveDeleteDirsMode::Always | crate::types::MoveDeleteDirsMode::Auto
    ) {
        if let Some(parent) = record.source.parent() {
            let _ = fs::remove_dir(parent);
        }
    }

    Ok(target)
}

pub fn link_record_with_dats(
    record: &FileRecord,
    config: &Config,
    dats: Option<&[crate::dat::DatRom]>,
    progress: Option<&ActionProgressHandle>,
) -> anyhow::Result<PathBuf> {
    let target = resolve_output_path_with_dats(record, config, dats);
    ensure_parent(&target)?;

    match config.link_mode {
        LinkMode::Hardlink => {
            if target.exists() {
                fs::remove_file(&target)?;
            }
            fs::hard_link(&record.source, &target)?;
        }
        LinkMode::Symlink => {
            if target.exists() {
                fs::remove_file(&target)?;
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::symlink;
                let src = if config.symlink_relative {
                    pathdiff::diff_paths(
                        &record.source,
                        target.parent().unwrap_or_else(|| std::path::Path::new(".")),
                    )
                    .unwrap_or(record.source.clone())
                } else {
                    record.source.clone()
                };
                symlink(src, &target)?;
            }
            #[cfg(not(unix))]
            {
                fs::copy(&record.source, &target)?;
            }
        }
        LinkMode::Reflink => {
            copy_file_with_progress(&record.source, &target, progress)?;
        }
    }

    if let Some(handle) = progress {
        let total = if record.size > 0 {
            record.size
        } else {
            fs::metadata(&record.source).map(|m| m.len()).unwrap_or(0)
        };
        let total_hint = if total > 0 { Some(total) } else { None };
        if total > 0 {
            handle.report_bytes(total, total_hint);
        } else {
            handle.report_bytes(1, Some(1));
        }
    }

    Ok(target)
}

fn record_is_extractable_archive(record: &FileRecord) -> bool {
    let extension = record
        .source
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|s| s.to_ascii_lowercase());

    matches!(extension.as_deref(), Some("zip")) || looks_like_external_archive(&record.source)
}

fn record_should_zip(record: &FileRecord, dats: Option<&[crate::dat::DatRom]>) -> bool {
    record_is_cartridge_based(record, dats)
}

pub fn extract_record_with_dats(
    record: &FileRecord,
    config: &Config,
    dats: Option<&[crate::dat::DatRom]>,
    progress: Option<&ActionProgressHandle>,
) -> anyhow::Result<Vec<PathBuf>> {
    if let Some(extracted) = try_extract_zip(record, config, dats, progress)? {
        return Ok(extracted);
    }

    if let Some(extracted) = try_extract_with_7z(record, config, dats, progress)? {
        return Ok(extracted);
    }

    Ok(vec![copy_record_with_dats(record, config, dats, progress)?])
}

pub fn move_record(record: &FileRecord, config: &Config) -> anyhow::Result<PathBuf> {
    let target = resolve_output_path(record, config);
    ensure_parent(&target)?;

    if target.exists() && !config.overwrite {
        return Ok(target);
    }

    fs::rename(&record.source, &target).or_else(|_| {
        fs::copy(&record.source, &target)?;
        fs::remove_file(&record.source)
    })?;

    if matches!(
        config.move_delete_dirs,
        crate::types::MoveDeleteDirsMode::Always | crate::types::MoveDeleteDirsMode::Auto
    ) {
        if let Some(parent) = record.source.parent() {
            let _ = fs::remove_dir(parent);
        }
    }

    Ok(target)
}

pub fn link_record(record: &FileRecord, config: &Config) -> anyhow::Result<PathBuf> {
    let target = resolve_output_path(record, config);
    ensure_parent(&target)?;

    match config.link_mode {
        LinkMode::Hardlink => {
            if target.exists() {
                fs::remove_file(&target)?;
            }
            fs::hard_link(&record.source, &target)?;
        }
        LinkMode::Symlink => {
            if target.exists() {
                fs::remove_file(&target)?;
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::symlink;
                let src = if config.symlink_relative {
                    pathdiff::diff_paths(
                        &record.source,
                        target.parent().unwrap_or_else(|| std::path::Path::new(".")),
                    )
                    .unwrap_or(record.source.clone())
                } else {
                    record.source.clone()
                };
                symlink(src, &target)?;
            }
            #[cfg(not(unix))]
            {
                fs::copy(&record.source, &target)?;
            }
        }
        LinkMode::Reflink => {
            fs::copy(&record.source, &target)?;
        }
    }

    Ok(target)
}

pub fn extract_record(record: &FileRecord, config: &Config) -> anyhow::Result<Vec<PathBuf>> {
    if let Some(extracted) = try_extract_zip(record, config, None, None)? {
        return Ok(extracted);
    }

    if let Some(extracted) = try_extract_with_7z(record, config, None, None)? {
        return Ok(extracted);
    }

    Ok(vec![copy_record(record, config, None)?])
}

fn try_extract_zip(
    record: &FileRecord,
    config: &Config,
    dats: Option<&[crate::dat::DatRom]>,
    progress: Option<&ActionProgressHandle>,
) -> anyhow::Result<Option<Vec<PathBuf>>> {
    let extension = record
        .source
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();

    if extension != "zip" {
        return Ok(None);
    }

    let file = fs::File::open(&record.source)?;
    let mut archive = match zip::ZipArchive::new(file) {
        Ok(archive) => archive,
        Err(err) => {
            if config.verbose > 0 {
                eprintln!(
                    "warning: {:?} has .zip extension but is not a zip archive: {}",
                    record.source, err
                );
            }
            return Ok(None);
        }
    };

    let mut written = Vec::new();
    let mut aggregate = 0u64;
    let total_hint = if record.size > 0 {
        Some(record.size)
    } else {
        None
    };
    let mut buf = vec![0u8; 1 << 20];
    for i in 0..archive.len() {
        let mut file = archive.by_index(i)?;
        if file.is_dir() {
            continue;
        }

        let mut entry_record = FileRecord {
            source: record.source.clone(),
            relative: PathBuf::from(file.name()),
            size: file.size(),
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
        populate_locale_tokens(&mut entry_record);

        let out_path = resolve_output_path_with_dats(&entry_record, config, dats);
        ensure_parent(&out_path)?;

        let mut output = fs::File::create(&out_path)?;
        loop {
            let read = file.read(&mut buf)?;
            if read == 0 {
                break;
            }
            output.write_all(&buf[..read])?;
            aggregate = aggregate.saturating_add(read as u64);
            if let Some(handle) = progress {
                handle.report_bytes(aggregate, total_hint);
            }
        }
        output.flush()?;
        written.push(out_path);
    }

    Ok(Some(written))
}

fn try_extract_with_7z(
    record: &FileRecord,
    config: &Config,
    dats: Option<&[crate::dat::DatRom]>,
    progress: Option<&ActionProgressHandle>,
) -> anyhow::Result<Option<Vec<PathBuf>>> {
    if !looks_like_external_archive(&record.source) {
        return Ok(None);
    }

    let exe = match which("7z").or_else(|_| which("7za")) {
        Ok(path) => path,
        Err(_) => {
            if config.verbose > 0 {
                eprintln!(
                    "warning: {:?} appears to be an archive but 7z is not available on PATH",
                    record.source
                );
            }
            return Ok(None);
        }
    };

    let tmp = tempdir()?;
    let status = Command::new(&exe)
        .arg("x")
        .arg(record.source.as_os_str())
        .arg(format!("-o{}", tmp.path().to_string_lossy()))
        .arg("-y")
        .status()
        .with_context(|| format!("extracting archive {:?} via {:?}", record.source, exe))?;

    if !status.success() {
        if config.verbose > 0 {
            eprintln!(
                "warning: failed to extract {:?} via {:?} (status: {:?})",
                record.source, exe, status
            );
        }
        return Ok(None);
    }

    let mut written = Vec::new();
    let mut aggregate = 0u64;
    let total_hint = if record.size > 0 {
        Some(record.size)
    } else {
        None
    };
    let mut buf = vec![0u8; 1 << 20];
    for entry in WalkDir::new(tmp.path())
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_file())
    {
        let rel = entry
            .path()
            .strip_prefix(tmp.path())
            .unwrap_or_else(|_| entry.path())
            .to_path_buf();
        let metadata = entry.metadata()?;

        let mut entry_record = FileRecord {
            source: record.source.clone(),
            relative: rel.clone(),
            size: metadata.len(),
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
        populate_locale_tokens(&mut entry_record);

        let out_path = resolve_output_path_with_dats(&entry_record, config, dats);
        ensure_parent(&out_path)?;
        let mut reader = fs::File::open(entry.path())?;
        let mut writer = fs::File::create(&out_path)?;
        loop {
            let read = reader.read(&mut buf)?;
            if read == 0 {
                break;
            }
            writer.write_all(&buf[..read])?;
            aggregate = aggregate.saturating_add(read as u64);
            if let Some(handle) = progress {
                handle.report_bytes(aggregate, total_hint);
            }
        }
        writer.flush()?;
        written.push(out_path);
    }

    Ok(Some(written))
}

fn looks_like_external_archive(path: &Path) -> bool {
    let name = path
        .file_name()
        .and_then(|s| s.to_str())
        .map(|s| s.to_ascii_lowercase())
        .unwrap_or_default();

    const SUFFIXES: &[&str] = &[
        ".7z",
        ".rar",
        ".tar",
        ".tar.gz",
        ".tgz",
        ".tar.bz2",
        ".tbz",
        ".tbz2",
        ".tar.xz",
        ".txz",
        ".tar.zst",
        ".tzst",
        ".tar.lz",
        ".tar.lzma",
        ".tlz",
    ];

    SUFFIXES.iter().any(|suffix| name.ends_with(suffix))
}

pub fn zip_record(
    record: &FileRecord,
    config: &Config,
    dats: Option<&[crate::dat::DatRom]>,
    progress: Option<&ActionProgressHandle>,
) -> anyhow::Result<PathBuf> {
    let target = resolve_output_path_with_dats(record, config, dats).with_extension("zip");
    ensure_parent(&target)?;

    // choose implementation based on format
    if matches!(
        config.zip_format,
        ZipFormat::Torrentzip | ZipFormat::Rvzstd | ZipFormat::Deflate
    ) {
        // use our Zip64-capable TorrentZip writer for exact control over
        // headers and EOCD comment. The Zip64 writer delegates to the
        // single-file `torrentzip::write_torrentzip` when there's a single
        // entry, so this centralizes the archive-writing logic.
        let filename_in_zip = record
            .relative
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("rom.bin");
        let srcs: Vec<(&Path, &str)> = vec![(record.source.as_path(), filename_in_zip)];
        crate::torrentzip_zip64::write_torrentzip_zip64(
            &srcs,
            &target,
            config.zip_format.clone(),
            progress,
        )?;
        Ok(target)
    } else {
        // fallback: simple zip using zip crate
        let mut file = fs::File::create(&target)?;
        let mut zip = zip::ZipWriter::new(&mut file);
        let options: FileOptions<'_, zip::write::ExtendedFileOptions> =
            FileOptions::default().compression_method(zip::CompressionMethod::Stored);

        let mut input = fs::File::open(&record.source)?;
        zip.start_file(
            record
                .relative
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("rom.bin"),
            options,
        )?;
        let total = fs::metadata(&record.source).map(|m| m.len()).unwrap_or(record.size);
        let mut buf = vec![0u8; 1 << 20];
        let mut written = 0u64;
        loop {
            let n = input.read(&mut buf)?;
            if n == 0 {
                break;
            }
            zip.write_all(&buf[..n])?;
            written = written.saturating_add(n as u64);
            if let Some(handle) = progress {
                handle.report_bytes(written, Some(total));
            }
        }
        zip.finish()?;

        Ok(target)
    }
}

/// Write a zip for multiple records into a single archive using the manual TorrentZip/Zip64 writer.
pub fn zip_records(records: &[FileRecord], config: &Config) -> anyhow::Result<PathBuf> {
    let target = resolve_output_path_with_dats(&records[0], config, None).with_extension("zip");
    ensure_parent(&target)?;

    // build list of source path + filename_in_zip pairs (CP437 encoding is attempted inside writer)
    let srcs: Vec<(&Path, &str)> = records
        .iter()
        .map(|r| {
            (
                r.source.as_path(),
                r.relative
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("rom.bin"),
            )
        })
        .collect();

    crate::torrentzip_zip64::write_torrentzip_zip64(&srcs, &target, config.zip_format.clone(), None)?;
    Ok(target)
}

pub fn playlist(records: &[FileRecord], config: &Config) -> anyhow::Result<PathBuf> {
    let mut target = config
        .output
        .clone()
        .unwrap_or_else(|| PathBuf::from("output"));
    target.push("playlist.m3u");
    ensure_parent(&target)?;

    let mut file = fs::File::create(&target)?;
    for record in records {
        writeln!(file, "{}", record.relative.to_string_lossy())?;
    }

    Ok(target)
}

pub fn write_report(records: &[FileRecord], config: &Config) -> anyhow::Result<PathBuf> {
    let mut target = config
        .output
        .clone()
        .unwrap_or_else(|| PathBuf::from("output"));
    target.push("report.json");
    ensure_parent(&target)?;

    // Ensure checksums present: compute md5 and crc32 (and sha256) for each record if missing
    use crate::checksum::compute_all_checksums;
    use crate::dat::{OnlineMatch, query_hasheous, query_igdb};

    let mut enriched = Vec::new();
    let mut online_matches: Vec<OnlineMatch> = Vec::new();

    // Prepare raw output dirs
    let base_out = config
        .output
        .clone()
        .unwrap_or_else(|| PathBuf::from("output"));
    let hasheous_dir = base_out.join("hasheous_raw");
    let igdb_dir = base_out.join("igdb_raw");
    let _ = fs::create_dir_all(&hasheous_dir);
    let _ = fs::create_dir_all(&igdb_dir);

    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()?;
    let igdb_client_ref = if config.igdb_client_id.is_some() && config.igdb_token.is_some() {
        Some(&client)
    } else {
        None
    };

    for (idx, rec) in records.iter().enumerate() {
        let mut rec = rec.clone();
        let mut attempted_slug_lookups: HashSet<String> = HashSet::new();
        let all = compute_all_checksums(&rec.source).ok();
        if let Some(a) = all {
            if rec.checksums.crc32.is_none() {
                rec.checksums.crc32 = a.crc32.clone();
            }
            if rec.checksums.md5.is_none() {
                rec.checksums.md5 = a.md5.clone();
            }
            if rec.checksums.sha256.is_none() {
                rec.checksums.sha256 = a.sha256.clone();
            }
        }

        // Query hasheous (try sha1 then md5 then sha256) -- reuse dat:: logic
        let mut hasheous_res = None;
        if config.enable_hasheous {
            let max_retries = config.online_max_retries.unwrap_or(3);
            let throttle_ms = config.online_throttle_ms;
            if let Some(h) = rec.checksums.sha1.as_ref() {
                // Guess algorithm by length, else try common algs
                let mut hasheous_found = None;
                match h.len() {
                    40 => {
                        hasheous_found = query_hasheous(
                            &client,
                            "sha1",
                            h,
                            config.verbose,
                            max_retries,
                            throttle_ms,
                        )
                        .ok()
                        .flatten()
                    }
                    32 => {
                        hasheous_found = query_hasheous(
                            &client,
                            "md5",
                            h,
                            config.verbose,
                            max_retries,
                            throttle_ms,
                        )
                        .ok()
                        .flatten()
                    }
                    64 => {
                        hasheous_found = query_hasheous(
                            &client,
                            "sha256",
                            h,
                            config.verbose,
                            max_retries,
                            throttle_ms,
                        )
                        .ok()
                        .flatten()
                    }
                    8 => {
                        hasheous_found = query_hasheous(
                            &client,
                            "crc32",
                            h,
                            config.verbose,
                            max_retries,
                            throttle_ms,
                        )
                        .ok()
                        .flatten()
                    }
                    _ => {}
                }
                if hasheous_found.is_none() {
                    for alg in &["sha1", "md5", "sha256", "crc32"] {
                        if let Some(v) = query_hasheous(
                            &client,
                            alg,
                            h,
                            config.verbose,
                            max_retries,
                            throttle_ms,
                        )
                        .ok()
                        .flatten()
                        {
                            hasheous_found = Some(v);
                            break;
                        }
                    }
                }
                hasheous_res = hasheous_found;
            }
            if hasheous_res.is_none() {
                if let Some(h) = rec.checksums.md5.as_ref() {
                    let mut hasheous_found = None;
                    match h.len() {
                        40 => {
                            hasheous_found = query_hasheous(
                                &client,
                                "sha1",
                                h,
                                config.verbose,
                                max_retries,
                                throttle_ms,
                            )
                            .ok()
                            .flatten()
                        }
                        32 => {
                            hasheous_found = query_hasheous(
                                &client,
                                "md5",
                                h,
                                config.verbose,
                                max_retries,
                                throttle_ms,
                            )
                            .ok()
                            .flatten()
                        }
                        64 => {
                            hasheous_found = query_hasheous(
                                &client,
                                "sha256",
                                h,
                                config.verbose,
                                max_retries,
                                throttle_ms,
                            )
                            .ok()
                            .flatten()
                        }
                        8 => {
                            hasheous_found = query_hasheous(
                                &client,
                                "crc32",
                                h,
                                config.verbose,
                                max_retries,
                                throttle_ms,
                            )
                            .ok()
                            .flatten()
                        }
                        _ => {}
                    }
                    if hasheous_found.is_none() {
                        for alg in &["sha1", "md5", "sha256", "crc32"] {
                            if let Some(v) = query_hasheous(
                                &client,
                                alg,
                                h,
                                config.verbose,
                                max_retries,
                                throttle_ms,
                            )
                            .ok()
                            .flatten()
                            {
                                hasheous_found = Some(v);
                                break;
                            }
                        }
                    }
                    hasheous_res = hasheous_found;
                }
            }
            if hasheous_res.is_none() {
                if let Some(h) = rec.checksums.sha256.as_ref() {
                    let mut hasheous_found = None;
                    match h.len() {
                        40 => {
                            hasheous_found = query_hasheous(
                                &client,
                                "sha1",
                                h,
                                config.verbose,
                                max_retries,
                                throttle_ms,
                            )
                            .ok()
                            .flatten()
                        }
                        32 => {
                            hasheous_found = query_hasheous(
                                &client,
                                "md5",
                                h,
                                config.verbose,
                                max_retries,
                                throttle_ms,
                            )
                            .ok()
                            .flatten()
                        }
                        64 => {
                            hasheous_found = query_hasheous(
                                &client,
                                "sha256",
                                h,
                                config.verbose,
                                max_retries,
                                throttle_ms,
                            )
                            .ok()
                            .flatten()
                        }
                        8 => {
                            hasheous_found = query_hasheous(
                                &client,
                                "crc32",
                                h,
                                config.verbose,
                                max_retries,
                                throttle_ms,
                            )
                            .ok()
                            .flatten()
                        }
                        _ => {}
                    }
                    if hasheous_found.is_none() {
                        for alg in &["sha1", "md5", "sha256", "crc32"] {
                            if let Some(v) = query_hasheous(
                                &client,
                                alg,
                                h,
                                config.verbose,
                                max_retries,
                                throttle_ms,
                            )
                            .ok()
                            .flatten()
                            {
                                hasheous_found = Some(v);
                                break;
                            }
                        }
                    }
                    hasheous_res = hasheous_found;
                }
            }
        }

        // If we have a hasheous result, save raw JSON
        if let Some(ref j) = hasheous_res {
            let fname = hasheous_dir.join(format!(
                "{:03}_{}.json",
                idx,
                rec.relative.to_string_lossy()
            ));
            let _ = fs::write(&fname, serde_json::to_string_pretty(j)?);
            enrich_record_with_hasheous_igdb(
                &mut rec,
                j,
                config,
                None,
                igdb_client_ref,
                &mut attempted_slug_lookups,
            );
        }

        // Query IGDB by name (use filename as fallback)
        let mut igdb_res = None;
        if config.igdb_client_id.is_some() && config.should_attempt_igdb_lookup(&rec) {
            let name = rec
                .relative
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("")
                .to_string();
            igdb_res = query_igdb(&name, config, &client, rec.derived_platform.as_deref())
                .ok()
                .flatten();
            if let Some(ref j) = igdb_res {
                let fname = igdb_dir.join(format!(
                    "{:03}_{}.json",
                    idx,
                    rec.relative.to_string_lossy()
                ));
                let _ = fs::write(&fname, serde_json::to_string_pretty(j)?);
                capture_genres_from_igdb(&mut rec, j, "IGDB-report", config.verbose);
            }
        }

        if hasheous_res.is_some() || igdb_res.is_some() {
            online_matches.push(OnlineMatch {
                name: rec.relative.to_string_lossy().to_string(),
                source_dat: None,
                source_path: Some(rec.source.clone()),
                hasheous: hasheous_res,
                igdb: igdb_res,
            });
        }

        enriched.push(rec);
    }

    // write report and online_matches
    let json = serde_json::to_string_pretty(&enriched)?;
    fs::write(&target, json)?;

    // Transform online_matches into a compact mapping of filename -> extracted metadata IDs
    use serde_json::json;
    let mut compact: Vec<serde_json::Value> = Vec::new();
    for m in &online_matches {
        let mut entry = json!({
            "name": m.name,
            "source_dat": null,
            "hasheous": null,
            "igdb": null,
            "extracted_ids": {}
        });

        if let Some(dat_path) = &m.source_dat {
            entry["source_dat"] = serde_json::Value::String(dat_path.to_string_lossy().to_string());
        }

        if let Some(h) = &m.hasheous {
            entry["hasheous"] = h.clone();
            // attempt to extract common ids
            let mut ids = serde_json::Map::new();
            if let Some(id) = h.get("id") {
                ids.insert("hasheous_id".to_string(), id.clone());
            }
            if let Some(metadata) = h.get("metadata") {
                // metadata may be an array of objects with id fields referencing various services
                if let Some(arr) = metadata.as_array() {
                    for obj in arr {
                        if let Some(sid) = obj.get("id") {
                            // some metadata entries have 'id' strings like 'super-mario-all-stars' or numeric ids
                            if let Some(src) = obj.get("source") {
                                if let Some(src_str) = src.as_str() {
                                    let key = format!("meta_{}", src_str.to_lowercase());
                                    ids.insert(key, sid.clone());
                                }
                            }
                        }
                    }
                }
            }
            // attributes array may contain VIMMManualId
            if let Some(attributes) = h.get("attributes") {
                if let Some(arr) = attributes.as_array() {
                    for a in arr {
                        if let Some(name) = a.get("attributeName").and_then(|v| v.as_str()) {
                            if name.eq_ignore_ascii_case("VIMMManualId") {
                                if let Some(val) = a.get("value") {
                                    ids.insert("vimm_manual_id".to_string(), val.clone());
                                }
                            }
                        }
                    }
                }
            }

            entry["extracted_ids"] = serde_json::Value::Object(ids);
        }

        if let Some(i) = &m.igdb {
            entry["igdb"] = i.clone();
        }

        compact.push(entry);
    }

    let om_target = base_out.join("online_matches.json");
    let om_json = serde_json::to_string_pretty(&compact)?;
    fs::write(&om_target, om_json)?;

    Ok(target)
}

pub fn write_dir2dat(records: &[FileRecord], config: &Config) -> anyhow::Result<PathBuf> {
    let mut target = config
        .output
        .clone()
        .unwrap_or_else(|| PathBuf::from("output"));
    target.push("dir2dat.json");
    ensure_parent(&target)?;

    let json = serde_json::to_string_pretty(records)?;
    fs::write(&target, json)?;
    Ok(target)
}

pub fn write_fixdat(
    records: &[FileRecord],
    config: &Config,
    dats: Option<&[crate::dat::DatRom]>,
) -> anyhow::Result<PathBuf> {
    let mut target = config
        .output
        .clone()
        .unwrap_or_else(|| PathBuf::from("output"));
    target.push("fixdat.json");
    ensure_parent(&target)?;

    let mut missing = Vec::new();
    for record in records {
        if !resolve_output_path_with_dats(record, config, dats).exists() {
            missing.push(record);
        }
    }

    let json = serde_json::to_string_pretty(&missing)?;
    fs::write(&target, json)?;
    Ok(target)
}

pub fn clean_output(
    records: &[FileRecord],
    config: &Config,
    dats: Option<&[crate::dat::DatRom]>,
) -> anyhow::Result<Vec<PathBuf>> {
    let mut cleaned = Vec::new();
    let mut expected = HashMap::new();
    for record in records {
        expected.insert(resolve_output_path_with_dats(record, config, dats), ());
    }

    let exclude = build_globset(&config.clean_exclude)?;
    if let Some(output) = &config.output {
        for entry in WalkDir::new(output)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| e.file_type().is_file())
        {
            let path = entry.into_path();
            if expected.contains_key(&path) {
                continue;
            }

            if exclude
                .as_ref()
                .is_some_and(|set| set.is_match(path.to_string_lossy().as_ref()))
            {
                continue;
            }

            if config.clean_dry_run {
                cleaned.push(path);
                continue;
            }

            if let Some(backup) = &config.clean_backup {
                let target = backup.join(path.file_name().unwrap_or_default());
                ensure_parent(&target)?;
                fs::rename(&path, &target).or_else(|_| {
                    fs::copy(&path, &target)?;
                    fs::remove_file(&path)
                })?;
                cleaned.push(target);
            } else {
                fs::remove_file(&path)?;
                cleaned.push(path);
            }
        }
    }

    Ok(cleaned)
}

fn run_action_with_progress<F>(
    action: &Action,
    records: &[FileRecord],
    progress: Option<&ProgressReporter>,
    work: F,
) -> anyhow::Result<Duration>
where
    F: Fn(&FileRecord, usize, Option<ActionProgressHandle>) -> anyhow::Result<()> + Sync,
{
    if let Some(p) = progress {
        p.begin_action(action, records.len());
    }

    let start = Instant::now();
    if records.is_empty() {
        if let Some(p) = progress {
            p.finish_action(action);
        }
        return Ok(start.elapsed());
    }
    let (result_tx, result_rx) = mpsc::channel::<anyhow::Result<PathBuf>>();
    let (action_progress_tx, action_progress_rx) = mpsc::channel::<ActionProgress>();
    let work_ref = &work;
    let allow_progress_handles = progress.is_some();
    let total_records = records.len();
    let mut first_error: Option<anyhow::Error> = None;

    let drain_progress = || {
        loop {
            match action_progress_rx.try_recv() {
                Ok(ActionProgress::ItemBytes {
                    path,
                    bytes_done,
                    total_bytes,
                }) => {
                    if let Some(p) = progress {
                        p.update_action_item_bytes(&path, bytes_done, total_bytes);
                    }
                }
                Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
            }
        }
    };

    thread::scope(|thread_scope| {
        let result_producer = result_tx.clone();
        let action_progress_producer = action_progress_tx.clone();
        thread_scope.spawn(move || {
            rayon::scope(|scope| {
                for (idx, record) in records.iter().enumerate() {
                    let tx = result_producer.clone();
                    let progress_tx = action_progress_producer.clone();
                    let relative_hint = record.relative.clone();
                    scope.spawn(move |_| {
                        let progress_handle = if allow_progress_handles {
                            Some(ActionProgressHandle::new(progress_tx, relative_hint.clone()))
                        } else {
                            None
                        };
                        let result = work_ref(record, idx, progress_handle);
                        let _ = tx.send(result.map(|_| relative_hint));
                    });
                }
            });
            drop(result_producer);
            drop(action_progress_producer);
        });

        drop(result_tx);
        drop(action_progress_tx);

        let mut completed = 0usize;
        for _ in 0..total_records {
            drain_progress();
            match result_rx.recv() {
                Ok(Ok(hint)) => {
                    completed += 1;
                    if let Some(p) = progress {
                        p.advance_action(completed, Some(&hint));
                        p.finish_action_item(&hint);
                    }
                }
                Ok(Err(err)) => {
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                    break;
                }
                Err(err) => {
                    first_error = Some(anyhow!("action worker channel closed unexpectedly: {err}"));
                    break;
                }
            }
        }
        drain_progress();
    });

    if let Some(p) = progress {
        p.finish_action(action);
    }

    if let Some(err) = first_error {
        Err(err)
    } else {
        Ok(start.elapsed())
    }
}

pub fn perform_actions(config: &Config) -> anyhow::Result<ExecutionPlan> {
    let progress = ProgressReporter::maybe_new(config);
    let run_start = Instant::now();
    let mut scan_config = config.clone();
    if scan_config.input_checksum_max.is_none()
        && scan_config.input_checksum_min.rank() < Checksum::Sha1.rank()
    {
        scan_config.input_checksum_max = Some(Checksum::Sha1);
    }
    let mut diag_timings: Vec<(String, Duration)> = Vec::new();
    let collection = with_diag_timing(
        "collect_inputs",
        progress.as_ref(),
        config.diag,
        &mut diag_timings,
        || collect_files(&scan_config, progress.as_ref()),
    )?;
    let mut records = collection.records;
    let skipped = collection.skipped;
    log_diag_step(
        progress.as_ref(),
        config.diag,
        format!(
            "step=collect_inputs records={} skipped={}",
            records.len(),
            skipped.len(),
        ),
    );
    if let Some(p) = progress.as_ref() {
        p.hint_background_task_total(BackgroundTask::Cache, Some(records.len()));
    }
    let dat_roms = with_diag_timing(
        "load_dats",
        progress.as_ref(),
        config.diag,
        &mut diag_timings,
        || load_dat_roms(config, progress.as_ref()),
    )?;
    log_diag_step(
        progress.as_ref(),
        config.diag,
        format!("step=load_dats dats={}", dat_roms.len(),),
    );
    let dat_index = with_diag_timing(
        "index_dats",
        progress.as_ref(),
        config.diag,
        &mut diag_timings,
        || Ok(DatIndex::from_dats(&dat_roms)),
    )?;
    log_diag_step(
        progress.as_ref(),
        config.diag,
        format!("step=index_dats dats={}", dat_roms.len(),),
    );
    let (matched_dat_entries, unmatched_dat_entries) = with_diag_timing(
        "partition_matches",
        progress.as_ref(),
        config.diag,
        &mut diag_timings,
        || Ok(partition_dat_matches(&records, &dat_roms)),
    )?;
    log_diag_step(
        progress.as_ref(),
        config.diag,
        format!(
            "DAT matches: {} hits, {} misses",
            matched_dat_entries.len(),
            unmatched_dat_entries.len()
        ),
    );
    let matched = matched_dat_entries.len();
    let mut steps = Vec::new();
    let mut action_durations: Vec<Duration> = Vec::new();
    // Optional blocking HTTP client used for quick per-file Hasheous lookups when requested
    let client_opt: Option<reqwest::blocking::Client> = if config.enable_hasheous {
        let timeout = Duration::from_secs(config.online_timeout_secs.unwrap_or(5));
        Some(
            reqwest::blocking::Client::builder()
                .timeout(timeout)
                .build()?,
        )
    } else {
        None
    };
    // Separate client for IGDB lookups so IGDB can be enabled even when Hasheous is disabled
    let igdb_client_opt: Option<reqwest::blocking::Client> = if config.igdb_network_enabled() {
        let timeout = Duration::from_secs(config.online_timeout_secs.unwrap_or(5));
        Some(
            reqwest::blocking::Client::builder()
                .timeout(timeout)
                .build()?,
        )
    } else {
        None
    };

    // Precompute a match-source map for records so all action loops can report consistent progress.
    use crate::checksum::compute_all_checksums;
    use std::collections::HashMap as Map;
    // Open sqlite cache (optional). Accept explicit DB path via config.cache_db.
    let cache: Option<cache::Cache> =
        match cache::Cache::open(config.cache_db.as_ref(), config.output.as_ref()) {
            Ok(c) => Some(c),
            Err(e) => {
                if config.verbose > 0 {
                    eprintln!("warning: unable to open cache DB: {}", e);
                }
                None
            }
        };
    let mut match_map: Map<PathBuf, String> = Map::new();
    // Track content keys for which we've already attempted network Hasheous lookups
    // during the precompute phase so we can avoid redundant NET-LOOKUPs later.
    let mut attempted_hasheous_keys: std::collections::HashSet<String> =
        std::collections::HashSet::new();
    let mut attempted_igdb_titles: std::collections::HashSet<String> =
        std::collections::HashSet::new();
    let mut net_lookup_goal: usize = 0;
    let max_retries = config.online_max_retries.unwrap_or(3);
    let throttle_ms = config.online_throttle_ms;
    // Iterate mutably so we can enrich missing checksums from online lookups
    if config.diag {
        if let Some(p) = progress.as_ref() {
            p.begin_diag_phase("enrich_records");
        }
    }
    let precompute_start = Instant::now();
    if let Some(p) = progress.as_ref() {
        p.hint_background_task_total(BackgroundTask::Cache, Some(records.len()));
    }
    for record in records.iter_mut() {
        if let Some(p) = progress.as_ref() {
            p.tick_background_task(BackgroundTask::Cache, 1, Some(&record.relative));
        }
        let mut attempted_slug_lookups: HashSet<String> = HashSet::new();
        // Ensure we have a content key (prefer sha256, then sha1/md5/crc32). Only compute additional
        // hashes if none are available.
        let mut key_option = best_checksum_key(&record.checksums);
        if key_option.is_none() {
            if let Ok(all) = compute_all_checksums(&record.source) {
                merge_checksum_sets(&mut record.checksums, &all);
                key_option = best_checksum_key(&record.checksums);
            }
        }
        let has_content_key = key_option.is_some();
        let key = key_option
            .clone()
            .unwrap_or_else(|| record.source.to_string_lossy().to_string());

        // Prefer cache if available (content-keyed)
        if let Some(c) = cache.as_ref() {
            if let Ok(Some(cached)) = c.get_checksums_by_key(&key) {
                vprintln!(
                    config.verbose,
                    2,
                    "CACHE-HIT checksums: {} key={}",
                    record.relative.to_string_lossy(),
                    key
                );
                if record.checksums.crc32.is_none() {
                    record.checksums.crc32 = cached.crc32;
                }
                if record.checksums.md5.is_none() {
                    record.checksums.md5 = cached.md5;
                }
                if record.checksums.sha1.is_none() {
                    record.checksums.sha1 = cached.sha1;
                }
                if record.checksums.sha256.is_none() {
                    record.checksums.sha256 = cached.sha256;
                }
            }
        }

        // If we computed checksums above, persist them keyed by content
        if has_content_key {
            if let Some(c) = cache.as_ref() {
                let _ = c.set_checksums_by_key(
                    &key,
                    &record.source,
                    Some(record.size),
                    &record.checksums,
                );
            }
        }
        // If configured, try to fetch missing sha1/md5 from Hasheous using any available checksum
        if let Some(client) = client_opt.as_ref() {
            let need_sha1 = record.checksums.sha1.is_none();
            let need_md5 = record.checksums.md5.is_none();
            if need_sha1 || need_md5 {
                // Try each available checksum value in priority order (sha1, md5, sha256, crc32).
                // For each value, first guess the algorithm by length and query Hasheous; if that
                // fails, fall back to trying the other algorithms with the same value. This avoids
                // situations where only CRC32 is tried when a stronger hash is available.
                fn extract_hashes_from_json(
                    v: &serde_json::Value,
                    sha1_out: &mut Option<String>,
                    md5_out: &mut Option<String>,
                ) {
                    match v {
                        serde_json::Value::String(s) => {
                            let s = s.trim();
                            if sha1_out.is_none()
                                && s.len() == 40
                                && s.chars().all(|c| c.is_ascii_hexdigit())
                            {
                                *sha1_out = Some(s.to_ascii_lowercase());
                            }
                            if md5_out.is_none()
                                && s.len() == 32
                                && s.chars().all(|c| c.is_ascii_hexdigit())
                            {
                                *md5_out = Some(s.to_ascii_lowercase());
                            }
                        }
                        serde_json::Value::Array(arr) => {
                            for e in arr {
                                extract_hashes_from_json(e, sha1_out, md5_out);
                                if sha1_out.is_some() && md5_out.is_some() {
                                    return;
                                }
                            }
                        }
                        serde_json::Value::Object(map) => {
                            for (_k, v) in map {
                                extract_hashes_from_json(v, sha1_out, md5_out);
                                if sha1_out.is_some() && md5_out.is_some() {
                                    return;
                                }
                            }
                        }
                        _ => {}
                    }
                }

                // (platform extraction is handled by module-level helper)

                let algs = ["sha1", "md5", "sha256", "crc32"];
                let candidates: Vec<(&str, Option<String>)> = vec![
                    ("sha1", record.checksums.sha1.clone()),
                    ("md5", record.checksums.md5.clone()),
                    ("sha256", record.checksums.sha256.clone()),
                    ("crc32", record.checksums.crc32.clone()),
                ];

                let mut found_json: Option<serde_json::Value> = None;

                // Check cache first for hasheous raw JSON for this record (keyed by content)
                if let Some(c) = cache.as_ref() {
                    if let Ok(Some(j)) = c.get_hasheous_raw_by_key(&key) {
                        vprintln!(
                            config.verbose,
                            2,
                            "CACHE-HIT hasheous (precompute): {} key={}",
                            record.relative.to_string_lossy(),
                            key
                        );
                        // Try to extract platform hint from cached hasheous JSON and attach to record
                        if let Some(pname) = extract_platform_from_hasheous(&j) {
                            if let Some(tok) = crate::game_console::romm_from_platform_name(&pname)
                            {
                                if should_accept_platform_override(record, &tok) {
                                    record.derived_platform = Some(tok.clone());
                                    vprintln!(
                                        config.verbose,
                                        2,
                                        "CACHE-PLATFORM hasheous (precompute): {} platform={} token={}",
                                        record.relative.to_string_lossy(),
                                        pname,
                                        tok
                                    );
                                } else {
                                    let ext =
                                        crate::game_console::romm_from_extension(&record.relative)
                                            .unwrap_or_default();
                                    vprintln!(
                                        config.verbose,
                                        2,
                                        "CACHE-PLATFORM hasheous (precompute) ignored: {} platform={} token={} ext_token={}",
                                        record.relative.to_string_lossy(),
                                        pname,
                                        tok,
                                        ext
                                    );
                                }
                            } else {
                                vprintln!(
                                    config.verbose,
                                    2,
                                    "CACHE-PLATFORM hasheous (precompute): {} platform={} (unmapped)",
                                    record.relative.to_string_lossy(),
                                    pname
                                );
                            }
                        }
                        enrich_record_with_hasheous_igdb(
                            record,
                            &j,
                            config,
                            cache.as_ref(),
                            igdb_client_opt.as_ref(),
                            &mut attempted_slug_lookups,
                        );
                        found_json = Some(j);
                    }
                }

                let mut did_net_lookup = false;
                'outer: for (_named_alg, h_opt) in &candidates {
                    if let Some(h) = h_opt.as_ref() {
                        // First, guess algorithm by length
                        let guess = match h.len() {
                            40 => Some("sha1"),
                            32 => Some("md5"),
                            64 => Some("sha256"),
                            8 => Some("crc32"),
                            _ => None,
                        };

                        if let Some(g) = guess {
                            if !config.cache_only {
                                did_net_lookup = true;
                                vprintln!(
                                    config.verbose,
                                    3,
                                    "NET-LOOKUP hasheous (precompute): {} alg={} key={} h={}",
                                    record.relative.to_string_lossy(),
                                    g,
                                    key,
                                    h
                                );
                                if let Ok(Some(j)) = query_hasheous_with_progress(
                                    progress.as_ref(),
                                    &mut net_lookup_goal,
                                    &record.relative,
                                    client,
                                    g,
                                    h,
                                    config.verbose,
                                    max_retries,
                                    throttle_ms,
                                ) {
                                    found_json = Some(j);
                                    break 'outer;
                                }
                            }
                        }

                        // Fallback: try all algorithms with this value
                        for alg in &algs {
                            if Some(*alg) == guess {
                                continue;
                            }
                            if !config.cache_only {
                                did_net_lookup = true;
                                vprintln!(
                                    config.verbose,
                                    3,
                                    "NET-LOOKUP hasheous (precompute): {} alg={} key={} h={}",
                                    record.relative.to_string_lossy(),
                                    alg,
                                    key,
                                    h
                                );
                                if let Ok(Some(j)) = query_hasheous_with_progress(
                                    progress.as_ref(),
                                    &mut net_lookup_goal,
                                    &record.relative,
                                    client,
                                    alg,
                                    h,
                                    config.verbose,
                                    max_retries,
                                    throttle_ms,
                                ) {
                                    found_json = Some(j);
                                    break 'outer;
                                }
                            }
                        }
                    }
                }

                if let Some(j) = found_json {
                    let mut maybe_sha1: Option<String> = None;
                    let mut maybe_md5: Option<String> = None;
                    extract_hashes_from_json(&j, &mut maybe_sha1, &mut maybe_md5);
                    if record.checksums.sha1.is_none() {
                        if let Some(s) = maybe_sha1 {
                            record.checksums.sha1 = Some(s);
                        }
                    }
                    if record.checksums.md5.is_none() {
                        if let Some(m) = maybe_md5 {
                            record.checksums.md5 = Some(m);
                        }
                    }
                    // Try to extract platform hint from the returned hasheous JSON and attach to record
                    if let Some(pname) = extract_platform_from_hasheous(&j) {
                        if let Some(tok) = crate::game_console::romm_from_platform_name(&pname) {
                            if should_accept_platform_override(record, &tok) {
                                record.derived_platform = Some(tok.clone());
                                vprintln!(
                                    config.verbose,
                                    2,
                                    "HASHEOUS-PLATFORM found: {} platform={} token={}",
                                    record.relative.to_string_lossy(),
                                    pname,
                                    tok
                                );
                            } else {
                                let ext =
                                    crate::game_console::romm_from_extension(&record.relative)
                                        .unwrap_or_default();
                                vprintln!(
                                    config.verbose,
                                    2,
                                    "HASHEOUS-PLATFORM ignored (extension mismatch): {} platform={} token={} ext_token={}",
                                    record.relative.to_string_lossy(),
                                    pname,
                                    tok,
                                    ext
                                );
                            }
                        } else {
                            vprintln!(
                                config.verbose,
                                2,
                                "HASHEOUS-PLATFORM found: {} platform={} (unmapped)",
                                record.relative.to_string_lossy(),
                                pname
                            );
                        }
                    }
                    enrich_record_with_hasheous_igdb(
                        record,
                        &j,
                        config,
                        cache.as_ref(),
                        igdb_client_opt.as_ref(),
                        &mut attempted_slug_lookups,
                    );
                    // store hasheous JSON into cache (keyed by content)
                    if let Some(c) = cache.as_ref() {
                        vprintln!(
                            config.verbose,
                            3,
                            "CACHE-WRITE hasheous: {} key={}",
                            record.relative.to_string_lossy(),
                            key
                        );
                        let _ = c.set_hasheous_raw_by_key(&key, &record.source, &j);
                    }
                } else if did_net_lookup {
                    // We attempted network lookups during precompute and found nothing;
                    // record the key so later main-loop logic can skip re-querying.
                    attempted_hasheous_keys.insert(key.clone());
                    vprintln!(
                        config.verbose,
                        2,
                        "CACHE-PRECOMPUTE-MISS hasheous: {} key={}",
                        record.relative.to_string_lossy(),
                        key
                    );
                }
            }
        }

        // Default
        let mut source = "heuristic".to_string();

        // Try Hasheous if enabled and client available
        if let Some(client) = client_opt.as_ref() {
            // If cache contains a hasheous entry (by content key), use that
            if let Some(c) = cache.as_ref() {
                if let Ok(Some(j)) = c.get_hasheous_raw_by_key(&key) {
                    vprintln!(
                        config.verbose,
                        2,
                        "CACHE-HIT hasheous: {} key={}",
                        record.relative.to_string_lossy(),
                        key
                    );
                    // Extract platform hint and attach to record if possible
                    if let Some(pname) = extract_platform_from_hasheous(&j) {
                        if let Some(tok) = crate::game_console::romm_from_platform_name(&pname) {
                            if should_accept_platform_override(record, &tok) {
                                record.derived_platform = Some(tok.clone());
                                vprintln!(
                                    config.verbose,
                                    2,
                                    "CACHE-HIT platform: {} -> {}",
                                    record.relative.to_string_lossy(),
                                    tok
                                );
                            } else {
                                let ext =
                                    crate::game_console::romm_from_extension(&record.relative)
                                        .unwrap_or_default();
                                vprintln!(
                                    config.verbose,
                                    2,
                                    "CACHE-HIT platform ignored (extension mismatch): {} ext_token={} candidate={}",
                                    record.relative.to_string_lossy(),
                                    ext,
                                    tok
                                );
                            }
                        } else {
                            vprintln!(
                                config.verbose,
                                2,
                                "CACHE-HIT platform (unmapped): {} -> {}",
                                record.relative.to_string_lossy(),
                                pname
                            );
                        }
                    }
                    enrich_record_with_hasheous_igdb(
                        record,
                        &j,
                        config,
                        cache.as_ref(),
                        igdb_client_opt.as_ref(),
                        &mut attempted_slug_lookups,
                    );
                    source = "Hasheous".to_string();
                    match_map.insert(record.source.clone(), source);
                    continue;
                }
            }
            let already_tried = attempted_hasheous_keys.contains(&key);
            if already_tried {
                vprintln!(
                    config.verbose,
                    2,
                    "SKIPPED-NET-LOOKUP hasheous (already tried): {} key={}",
                    record.relative.to_string_lossy(),
                    key
                );
            }
            if config.cache_only && !already_tried {
                vprintln!(
                    config.verbose,
                    2,
                    "CACHE-MISS (cache-only): {} key={}",
                    record.relative.to_string_lossy(),
                    key
                );
            }
            if !config.cache_only && !already_tried {
                // Try checksums in preferred order: sha1, md5, sha256, crc32
                let mut did_net_lookup = false;
                if let Some(h) = record.checksums.sha1.as_ref() {
                    did_net_lookup = true;
                    vprintln!(
                        config.verbose,
                        3,
                        "NET-LOOKUP hasheous: {} alg=sha1 key={} h={}",
                        record.relative.to_string_lossy(),
                        key,
                        h
                    );
                    let lookup = query_hasheous_with_progress(
                        progress.as_ref(),
                        &mut net_lookup_goal,
                        &record.relative,
                        client,
                        "sha1",
                        h,
                        config.verbose,
                        max_retries,
                        throttle_ms,
                    )
                    .ok()
                    .flatten();
                    if let Some(j) = lookup {
                        if let Some(c) = cache.as_ref() {
                            vprintln!(
                                config.verbose,
                                3,
                                "CACHE-WRITE hasheous: {} key={}",
                                record.relative.to_string_lossy(),
                                key
                            );
                            let _ = c.set_hasheous_raw_by_key(&key, &record.source, &j);
                        }
                        enrich_record_with_hasheous_igdb(
                            record,
                            &j,
                            config,
                            cache.as_ref(),
                            igdb_client_opt.as_ref(),
                            &mut attempted_slug_lookups,
                        );
                        source = "Hasheous".to_string();
                        match_map.insert(record.source.clone(), source);
                        continue;
                    }
                }
                if let Some(h) = record.checksums.md5.as_ref() {
                    did_net_lookup = true;
                    vprintln!(
                        config.verbose,
                        3,
                        "NET-LOOKUP hasheous: {} alg=md5 key={} h={}",
                        record.relative.to_string_lossy(),
                        key,
                        h
                    );
                    let lookup = query_hasheous_with_progress(
                        progress.as_ref(),
                        &mut net_lookup_goal,
                        &record.relative,
                        client,
                        "md5",
                        h,
                        config.verbose,
                        max_retries,
                        throttle_ms,
                    )
                    .ok()
                    .flatten();
                    if let Some(j) = lookup {
                        if let Some(c) = cache.as_ref() {
                            vprintln!(
                                config.verbose,
                                3,
                                "CACHE-WRITE hasheous: {} key={}",
                                record.relative.to_string_lossy(),
                                key
                            );
                            let _ = c.set_hasheous_raw_by_key(&key, &record.source, &j);
                        }
                        enrich_record_with_hasheous_igdb(
                            record,
                            &j,
                            config,
                            cache.as_ref(),
                            igdb_client_opt.as_ref(),
                            &mut attempted_slug_lookups,
                        );
                        source = "Hasheous".to_string();
                        match_map.insert(record.source.clone(), source);
                        continue;
                    }
                }
                if let Some(h) = record.checksums.sha256.as_ref() {
                    did_net_lookup = true;
                    vprintln!(
                        config.verbose,
                        3,
                        "NET-LOOKUP hasheous: {} alg=sha256 key={} h={}",
                        record.relative.to_string_lossy(),
                        key,
                        h
                    );
                    let lookup = query_hasheous_with_progress(
                        progress.as_ref(),
                        &mut net_lookup_goal,
                        &record.relative,
                        client,
                        "sha256",
                        h,
                        config.verbose,
                        max_retries,
                        throttle_ms,
                    )
                    .ok()
                    .flatten();
                    if let Some(j) = lookup {
                        if let Some(c) = cache.as_ref() {
                            vprintln!(
                                config.verbose,
                                3,
                                "CACHE-WRITE hasheous: {} key={}",
                                record.relative.to_string_lossy(),
                                key
                            );
                            let _ = c.set_hasheous_raw_by_key(&key, &record.source, &j);
                        }
                        enrich_record_with_hasheous_igdb(
                            record,
                            &j,
                            config,
                            cache.as_ref(),
                            igdb_client_opt.as_ref(),
                            &mut attempted_slug_lookups,
                        );
                        source = "Hasheous".to_string();
                        match_map.insert(record.source.clone(), source);
                        continue;
                    }
                }
                if let Some(h) = record.checksums.crc32.as_ref() {
                    did_net_lookup = true;
                    vprintln!(
                        config.verbose,
                        3,
                        "NET-LOOKUP hasheous: {} alg=crc32 key={} h={}",
                        record.relative.to_string_lossy(),
                        key,
                        h
                    );
                    let lookup = query_hasheous_with_progress(
                        progress.as_ref(),
                        &mut net_lookup_goal,
                        &record.relative,
                        client,
                        "crc32",
                        h,
                        config.verbose,
                        max_retries,
                        throttle_ms,
                    )
                    .ok()
                    .flatten();
                    if let Some(j) = lookup {
                        if let Some(c) = cache.as_ref() {
                            vprintln!(
                                config.verbose,
                                3,
                                "CACHE-WRITE hasheous: {} key={}",
                                record.relative.to_string_lossy(),
                                key
                            );
                            let _ = c.set_hasheous_raw_by_key(&key, &record.source, &j);
                        }
                        enrich_record_with_hasheous_igdb(
                            record,
                            &j,
                            config,
                            cache.as_ref(),
                            igdb_client_opt.as_ref(),
                            &mut attempted_slug_lookups,
                        );
                        source = "Hasheous".to_string();
                        match_map.insert(record.source.clone(), source);
                        continue;
                    }
                }
                if did_net_lookup {
                    // We tried network lookups for this content key and found no result; mark
                    // it so subsequent records with the same content key will skip querying.
                    attempted_hasheous_keys.insert(key.clone());
                    vprintln!(
                        config.verbose,
                        2,
                        "CACHE-MISS hasheous (no result): {} key={}",
                        record.relative.to_string_lossy(),
                        key
                    );
                }
            }
        }

        // DAT fallback after Hasheous misses
        if let Some(dat_entry) =
            crate::dat::find_dat_for_record_with_index(record, &dat_roms, &dat_index)
        {
            if record.derived_platform.is_none() {
                if let Some(tok) = crate::game_console::romm_from_dat(&dat_entry) {
                    record.derived_platform = Some(tok.clone());
                    vprintln!(
                        config.verbose,
                        2,
                        "DAT-PLATFORM derived: {} platform_token={} source={}",
                        record.relative.to_string_lossy(),
                        tok,
                        dat_entry.source_dat.display()
                    );
                } else {
                    vprintln!(
                        config.verbose,
                        2,
                        "DAT-PLATFORM unmapped: {} dat_name={}",
                        record.relative.to_string_lossy(),
                        dat_entry.name
                    );
                }
            }
            source = format!("DAT:{}", dat_entry.source_dat.display());
        }

        if config.should_attempt_igdb_lookup(record) {
            if let Some(raw_name) = record.relative.file_name().and_then(|n| n.to_str()) {
                let normalized = crate::dat::normalize_name(raw_name);
                let key = normalized.to_ascii_lowercase();
                if !key.is_empty() {
                    let mut cache_entry_found = false;
                    let mut igdb_source_label: Option<&str> = None;

                    if let Some(c) = cache.as_ref() {
                        if let Ok(Some(entry)) = c.get_igdb_entry_by_key(&key) {
                            let mut cache_valid = true;
                            if let Some(derived) = record.derived_platform.as_deref() {
                                if !crate::dat::igdb_cache_entry_matches_platform(&entry, derived) {
                                    cache_valid = false;
                                    vprintln!(
                                        config.verbose,
                                        2,
                                        "CACHE-INVALID igdb: {} key={} derived_platform={} cached_platforms={:?}",
                                        record.relative.to_string_lossy(),
                                        key,
                                        derived,
                                        entry.platforms
                                    );
                                    let _ = c.delete_igdb_key(&key);
                                }
                            }

                            if cache_valid {
                                cache_entry_found = true;
                                vprintln!(
                                    config.verbose,
                                    2,
                                    "CACHE-HIT igdb: {} key={}",
                                    record.relative.to_string_lossy(),
                                    key
                                );
                                let platform_before_cache = record.derived_platform.clone();
                                apply_cached_igdb_entry(record, &entry, "IGDB-cache", config);
                                if platform_before_cache.is_none()
                                    && record.derived_platform.is_some()
                                {
                                    igdb_source_label = Some("IGDB-cache");
                                }
                            }
                        }
                    }

                    let needs_lookup = config.should_attempt_igdb_lookup(record);
                    if !needs_lookup {
                        if let Some(label) = igdb_source_label {
                            source = label.to_string();
                            match_map.insert(record.source.clone(), source.clone());
                            continue;
                        }
                    } else if config.cache_only {
                        let status = if cache_entry_found {
                            "CACHE-INCOMPLETE"
                        } else {
                            "CACHE-MISS"
                        };
                        vprintln!(
                            config.verbose,
                            2,
                            "{} igdb (cache-only): {} key={}",
                            status,
                            record.relative.to_string_lossy(),
                            key
                        );
                    } else if attempted_igdb_titles.contains(&key) {
                        vprintln!(
                            config.verbose,
                            2,
                            "SKIPPED-IGDB lookup (already tried): {} key={}",
                            record.relative.to_string_lossy(),
                            key
                        );
                    } else if let Some(client) = igdb_client_opt.as_ref() {
                        vprintln!(
                            config.verbose,
                            3,
                            "NET-LOOKUP igdb: {} key={}",
                            record.relative.to_string_lossy(),
                            key
                        );
                        let lookup = query_igdb_with_progress(
                            progress.as_ref(),
                            &mut net_lookup_goal,
                            &record.relative,
                            &normalized,
                            config,
                            client,
                            record.derived_platform.as_deref(),
                        )
                        .ok()
                        .flatten();
                        if let Some(mut j) = lookup {
                            let identifiers = extract_platform_from_igdb(&j);
                            let extension_hint =
                                crate::game_console::romm_from_extension(&record.relative);
                            if record.derived_platform.is_none() {
                                let preferred_token = record
                                    .derived_platform
                                    .as_deref()
                                    .or(extension_hint.as_deref());
                                if let Some((tok, identifier)) =
                                    resolve_igdb_platform_token(&identifiers, preferred_token)
                                {
                                    if should_accept_platform_override(record, &tok) {
                                        record.derived_platform = Some(tok.clone());
                                        vprintln!(
                                            config.verbose,
                                            2,
                                            "IGDB platform derived: {} platform_token={} identifier={}",
                                            record.relative.to_string_lossy(),
                                            tok,
                                            identifier
                                        );
                                        igdb_source_label.get_or_insert("IGDB");
                                    } else {
                                        let ext = extension_hint.clone().unwrap_or_default();
                                        vprintln!(
                                            config.verbose,
                                            2,
                                            "IGDB-PLATFORM ignored (extension mismatch): {} candidate={} ext_token={}",
                                            record.relative.to_string_lossy(),
                                            tok,
                                            ext
                                        );
                                    }
                                } else if let Some(first) = identifiers.first() {
                                    vprintln!(
                                        config.verbose,
                                        2,
                                        "IGDB-PLATFORM unmapped: {} -> {}",
                                        record.relative.to_string_lossy(),
                                        first
                                    );
                                } else {
                                    vprintln!(
                                        config.verbose,
                                        2,
                                        "IGDB-PLATFORM unmapped: {} -> <no-platforms>",
                                        record.relative.to_string_lossy()
                                    );
                                }
                            } else {
                                vprintln!(
                                    config.verbose,
                                    3,
                                    "IGDB platform skipped (already derived): {}",
                                    record.relative.to_string_lossy()
                                );
                            }
                            let genres_from_igdb = ensure_genres_from_igdb_sources(
                                record,
                                &mut j,
                                "IGDB",
                                Some(&key),
                                cache.as_ref(),
                                config,
                                igdb_client_opt.as_ref(),
                            );
                            if genres_from_igdb {
                                igdb_source_label.get_or_insert("IGDB");
                            }
                            if let Some(c) = cache.as_ref() {
                                vprintln!(
                                    config.verbose,
                                    3,
                                    "CACHE-WRITE igdb: {} key={}",
                                    record.relative.to_string_lossy(),
                                    key
                                );
                                let _ = c.set_igdb_raw_by_key(&key, &j);
                            }
                        } else {
                            vprintln!(
                                config.verbose,
                                2,
                                "IGDB lookup returned no result: {} key={}",
                                record.relative.to_string_lossy(),
                                key
                            );
                        }
                        attempted_igdb_titles.insert(key.clone());
                    } else {
                        vprintln!(
                            config.verbose,
                            2,
                            "SKIPPED igdb lookup (no credentials): {} key={}",
                            record.relative.to_string_lossy(),
                            key
                        );
                    }

                    if let Some(label) = igdb_source_label {
                        source = label.to_string();
                        match_map.insert(record.source.clone(), source.clone());
                        continue;
                    }
                }
            }
        }

        match_map.insert(record.source.clone(), source);
    }
    let enrich_elapsed = precompute_start.elapsed();
    if config.diag {
        if let Some(p) = progress.as_ref() {
            p.finish_diag_phase(
                "enrich_records",
                Some(format!("{:.2}s", enrich_elapsed.as_secs_f64())),
            );
        }
    }
    record_diag_duration(
        "enrich_records",
        enrich_elapsed,
        progress.as_ref(),
        config.diag,
        &mut diag_timings,
    );

    // With records enriched from cache/hasheous precompute, gather unmatched entries for online hints now.
    let unmatched_records = with_diag_timing(
        "records_without_dat_match",
        progress.as_ref(),
        config.diag,
        &mut diag_timings,
        || {
            Ok(records_without_dat_match_with_index(
                &records, &dat_roms, &dat_index,
            ))
        },
    )?;
    log_diag_step(
        progress.as_ref(),
        config.diag,
        format!(
            "DAT misses for records: {}/{} unmatched",
            unmatched_records.len(),
            records.len()
        ),
    );
    let mut cache_only_lookup_config: Option<Config> = None;
    if cache.is_some() && !config.cache_only {
        let mut cloned = config.clone();
        cloned.cache_only = true;
        cache_only_lookup_config = Some(cloned);
    }
    let online_lookup_config = cache_only_lookup_config.as_ref().unwrap_or(config);
    let online_matches = with_diag_timing(
        "online_lookup",
        progress.as_ref(),
        config.diag,
        &mut diag_timings,
        || online_lookup(&unmatched_records, online_lookup_config),
    )?;
    log_diag_step(
        progress.as_ref(),
        config.diag,
        format!("step=online_lookup matches={}", online_matches.len(),),
    );
    for action in &config.commands {
        log_diag_step(
            progress.as_ref(),
            config.diag,
            format!(
                "step=action_start action={:?} total_records={}",
                action,
                records.len(),
            ),
        );
        match action {
            Action::Copy => {
                let duration =
                    run_action_with_progress(action, &records, progress.as_ref(), |record, _, handle| {
                        let _match_source = match_map
                            .get(&record.source)
                            .cloned()
                            .unwrap_or_else(|| "heuristic".to_string());
                        let _target =
                            resolve_output_path_with_dats(record, config, Some(&dat_roms));
                        copy_record_with_dats(record, config, Some(&dat_roms), handle.as_ref())?;
                        Ok(())
                    })?;
                steps.push(ActionOutcome {
                    action: action.clone(),
                    status: "ok".to_string(),
                    note: "Copied input files to output".to_string(),
                });
                action_durations.push(duration);
                record_diag_duration(
                    "action_copy",
                    duration,
                    progress.as_ref(),
                    config.diag,
                    &mut diag_timings,
                );
            }
            Action::Move => {
                let duration =
                    run_action_with_progress(action, &records, progress.as_ref(), |record, _, handle| {
                        let _match_source = match_map
                            .get(&record.source)
                            .cloned()
                            .unwrap_or_else(|| "heuristic".to_string());
                        let _target =
                            resolve_output_path_with_dats(record, config, Some(&dat_roms));
                        move_record_with_dats(record, config, Some(&dat_roms), handle.as_ref())?;
                        Ok(())
                    })?;
                steps.push(ActionOutcome {
                    action: action.clone(),
                    status: "ok".to_string(),
                    note: "Moved input files to output".to_string(),
                });
                action_durations.push(duration);
                record_diag_duration(
                    "action_move",
                    duration,
                    progress.as_ref(),
                    config.diag,
                    &mut diag_timings,
                );
            }
            Action::Link => {
                let duration =
                    run_action_with_progress(action, &records, progress.as_ref(), |record, _, handle| {
                        let _match_source = match_map
                            .get(&record.source)
                            .cloned()
                            .unwrap_or_else(|| "heuristic".to_string());
                        let _target =
                            resolve_output_path_with_dats(record, config, Some(&dat_roms));
                        link_record_with_dats(record, config, Some(&dat_roms), handle.as_ref())?;
                        Ok(())
                    })?;
                steps.push(ActionOutcome {
                    action: action.clone(),
                    status: "ok".to_string(),
                    note: format!("Linked files using {:?}", config.link_mode),
                });
                action_durations.push(duration);
                record_diag_duration(
                    "action_link",
                    duration,
                    progress.as_ref(),
                    config.diag,
                    &mut diag_timings,
                );
            }
            Action::Extract => {
                let extractable: Vec<FileRecord> = records
                    .iter()
                    .filter(|record| record_is_extractable_archive(record))
                    .cloned()
                    .collect();

                let duration = run_action_with_progress(
                    action,
                    &extractable,
                    progress.as_ref(),
                    |record, _, handle| {
                        let _match_source = match_map
                            .get(&record.source)
                            .cloned()
                            .unwrap_or_else(|| "heuristic".to_string());
                        let _target =
                            resolve_output_path_with_dats(record, config, Some(&dat_roms));
                        extract_record_with_dats(record, config, Some(&dat_roms), handle.as_ref())?;
                        Ok(())
                    },
                )?;
                steps.push(ActionOutcome {
                    action: action.clone(),
                    status: "ok".to_string(),
                    note: if extractable.is_empty() {
                        "No archives required extraction".to_string()
                    } else {
                        format!(
                            "Extracted {} archive{}",
                            extractable.len(),
                            if extractable.len() == 1 { "" } else { "s" }
                        )
                    },
                });
                action_durations.push(duration);
                record_diag_duration(
                    "action_extract",
                    duration,
                    progress.as_ref(),
                    config.diag,
                    &mut diag_timings,
                );
            }
            Action::Zip => {
                let mut zip_targets: Vec<FileRecord> = Vec::new();
                let mut skipped: Vec<&FileRecord> = Vec::new();
                for record in records.iter() {
                    if record_should_zip(record, Some(&dat_roms)) {
                        zip_targets.push(record.clone());
                    } else {
                        skipped.push(record);
                    }
                }

                if !skipped.is_empty() {
                    vprintln!(
                        config.verbose,
                        1,
                        "Skipping zip for {} non-cartridge file(s)",
                        skipped.len()
                    );
                    for record in &skipped {
                        vprintln!(
                            config.verbose,
                            2,
                            "Skipping zip for {} (non-cartridge)",
                            record.relative.to_string_lossy()
                        );
                    }
                }

                if zip_targets.is_empty() {
                    steps.push(ActionOutcome {
                        action: action.clone(),
                        status: "ok".to_string(),
                        note: "No cartridge ROMs required zipping".to_string(),
                    });
                    continue;
                }

                let duration = run_action_with_progress(
                    action,
                    &zip_targets,
                    progress.as_ref(),
                    |record, _, handle| {
                        let _match_source = match_map
                            .get(&record.source)
                            .cloned()
                            .unwrap_or_else(|| "heuristic".to_string());
                        let _target =
                            resolve_output_path_with_dats(record, config, Some(&dat_roms))
                                .with_extension("zip");
                        let created = zip_record(record, config, Some(&dat_roms), handle.as_ref())?;
                        let unzipped =
                            resolve_output_path_with_dats(record, config, Some(&dat_roms));
                        if unzipped.exists() {
                            match fs::remove_file(&unzipped) {
                                Ok(_) => {
                                    vprintln!(
                                        config.verbose,
                                        1,
                                        "Removed unzipped output: {}",
                                        unzipped.to_string_lossy()
                                    )
                                }
                                Err(err) => eprintln!(
                                    "Failed to remove unzipped output {}: {}",
                                    unzipped.to_string_lossy(),
                                    err
                                ),
                            }
                        }
                        let _ = created;
                        Ok(())
                    },
                )?;
                let zipped_count = zip_targets.len();
                steps.push(ActionOutcome {
                    action: action.clone(),
                    status: "ok".to_string(),
                    note: format!(
                        "Zipped {} cartridge ROM{}{}",
                        zipped_count,
                        if zipped_count == 1 { "" } else { "s" },
                        if skipped.is_empty() {
                            String::new()
                        } else {
                            format!(
                                ", left {} non-cartridge file{} raw",
                                skipped.len(),
                                if skipped.len() == 1 { "" } else { "s" }
                            )
                        }
                    ),
                });
                action_durations.push(duration);
                record_diag_duration(
                    "action_zip",
                    duration,
                    progress.as_ref(),
                    config.diag,
                    &mut diag_timings,
                );
            }
            Action::Playlist => {
                let start = Instant::now();
                let _ = playlist(&records, config)?;
                steps.push(ActionOutcome {
                    action: action.clone(),
                    status: "ok".to_string(),
                    note: "Generated playlist".to_string(),
                });
                let elapsed = start.elapsed();
                action_durations.push(elapsed);
                record_diag_duration(
                    "action_playlist",
                    elapsed,
                    progress.as_ref(),
                    config.diag,
                    &mut diag_timings,
                );
            }
            Action::Report => {
                let start = Instant::now();
                let _ = write_report(&records, config)?;
                steps.push(ActionOutcome {
                    action: action.clone(),
                    status: "ok".to_string(),
                    note: "Wrote report".to_string(),
                });
                let elapsed = start.elapsed();
                action_durations.push(elapsed);
                record_diag_duration(
                    "action_report",
                    elapsed,
                    progress.as_ref(),
                    config.diag,
                    &mut diag_timings,
                );
            }
            Action::Dir2dat => {
                let start = Instant::now();
                let _ = write_dir2dat(&records, config)?;
                steps.push(ActionOutcome {
                    action: action.clone(),
                    status: "ok".to_string(),
                    note: "Generated dir2dat JSON".to_string(),
                });
                let elapsed = start.elapsed();
                action_durations.push(elapsed);
                record_diag_duration(
                    "action_dir2dat",
                    elapsed,
                    progress.as_ref(),
                    config.diag,
                    &mut diag_timings,
                );
            }
            Action::Fixdat => {
                let start = Instant::now();
                let _ = write_fixdat(&records, config, Some(&dat_roms))?;
                steps.push(ActionOutcome {
                    action: action.clone(),
                    status: "ok".to_string(),
                    note: "Generated fixdat JSON".to_string(),
                });
                let elapsed = start.elapsed();
                action_durations.push(elapsed);
                record_diag_duration(
                    "action_fixdat",
                    elapsed,
                    progress.as_ref(),
                    config.diag,
                    &mut diag_timings,
                );
            }
            Action::Clean => {
                let start = Instant::now();
                let cleaned = clean_output(&records, config, Some(&dat_roms))?;
                steps.push(ActionOutcome {
                    action: action.clone(),
                    status: "ok".to_string(),
                    note: format!("Cleaned {} files", cleaned.len()),
                });
                let elapsed = start.elapsed();
                action_durations.push(elapsed);
                record_diag_duration(
                    "action_clean",
                    elapsed,
                    progress.as_ref(),
                    config.diag,
                    &mut diag_timings,
                );
            }
            Action::Test => {
                let start = Instant::now();
                steps.push(ActionOutcome {
                    action: action.clone(),
                    status: "ok".to_string(),
                    note: "Validated configuration only".to_string(),
                });
                let elapsed = start.elapsed();
                action_durations.push(elapsed);
                record_diag_duration(
                    "action_test",
                    elapsed,
                    progress.as_ref(),
                    config.diag,
                    &mut diag_timings,
                );
            }
        }
        log_diag_step(
            progress.as_ref(),
            config.diag,
            format!("step=action_complete action={:?} status=ok", action,),
        );
    }

    if !dat_roms.is_empty() {
        steps.push(ActionOutcome {
            action: Action::Fixdat,
            status: "info".to_string(),
            note: format!(
                "Matched {} DAT roms, {} unmatched{}",
                matched,
                unmatched_dat_entries.len(),
                if !online_matches.is_empty() {
                    format!("; {} online hints", online_matches.len())
                } else {
                    String::new()
                }
            ),
        });
    }

    if let Err(err) =
        maybe_emit_unknown_genre_report(&records, config, cache.as_ref(), progress.as_ref())
    {
        log_diag_step(
            progress.as_ref(),
            config.diag,
            format!("unknown-genre-report error={}", err),
        );
    }

    if let Some(p) = progress.as_ref() {
        p.finish_background_task(BackgroundTask::Cache);
        p.finish_background_task(BackgroundTask::NetLookup);
        p.finalize();
    }

    let summary = build_run_summary(
        config,
        records.len(),
        &skipped,
        &steps,
        unmatched_dat_entries.len(),
    );
    let total_duration = run_start.elapsed();
    emit_summary(
        &summary,
        &skipped,
        &steps,
        config,
        &matched_dat_entries,
        &action_durations,
        total_duration,
    );

    if config.diag && !diag_timings.is_empty() {
        eprintln!("\nDiag timings:");
        for (phase, duration) in &diag_timings {
            eprintln!(
                "  - {:<24} {} ({:.2} ms)",
                phase,
                format_duration(*duration),
                duration.as_secs_f64() * 1000.0
            );
        }
    }

    Ok(ExecutionPlan {
        config: config.clone(),
        steps,
        files_processed: records.len(),
        dat_matched: matched_dat_entries,
        dat_unmatched: if config.list_unmatched_dats {
            unmatched_dat_entries
        } else {
            Vec::new()
        },
        online_matches,
        skipped,
        summary,
    })
}

fn build_run_summary(
    config: &Config,
    processed: usize,
    skipped: &[SkippedFile],
    steps: &[ActionOutcome],
    dat_unmatched_count: usize,
) -> RunSummary {
    let mut counts: std::collections::HashMap<SkipReason, usize> = std::collections::HashMap::new();
    for entry in skipped {
        *counts.entry(entry.reason.clone()).or_insert(0) += 1;
    }

    let mut breakdown: Vec<SkipSummary> = counts
        .into_iter()
        .map(|(reason, count)| SkipSummary { reason, count })
        .collect();
    breakdown.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.reason.cmp(&b.reason)));

    let files_copied = if steps.iter().any(|s| matches!(s.action, Action::Copy)) {
        Some(processed)
    } else {
        None
    };

    RunSummary {
        total_inputs: config.input.len(),
        input_roots: config.input.clone(),
        files_processed: processed,
        files_skipped: skipped.len(),
        files_copied,
        dat_unmatched: dat_unmatched_count,
        skip_breakdown: breakdown,
        actions_run: config.commands.clone(),
        filters: FilterSummary {
            region: config.filter_region.clone(),
            language: config.filter_language.clone(),
            include_regex: config.filter_regex.clone(),
            exclude_regex: config.filter_regex_exclude.clone(),
        },
    }
}

#[derive(Serialize)]
struct UnknownGenreCacheInfo {
    slug: Option<String>,
    name: Option<String>,
    platforms: Vec<String>,
    has_genres: bool,
}

#[derive(Serialize)]
struct UnknownGenreQueryInfo {
    body: String,
    keywords: Vec<String>,
    keyword_strategy: &'static str,
    platform_hint: Option<String>,
}

#[derive(Serialize)]
struct UnknownGenreEntry {
    source: String,
    relative: String,
    derived_platform: Option<String>,
    normalized_name: Option<String>,
    cache_key: Option<String>,
    igdb_query: Option<UnknownGenreQueryInfo>,
    igdb_mode: IgdbLookupMode,
    igdb_client_configured: bool,
    igdb_network_enabled: bool,
    reason: String,
    cache: Option<UnknownGenreCacheInfo>,
}

fn maybe_emit_unknown_genre_report(
    records: &[FileRecord],
    config: &Config,
    cache: Option<&cache::Cache>,
    progress: Option<&ProgressReporter>,
) -> anyhow::Result<()> {
    if !config.diag {
        return Ok(());
    }
    let misses: Vec<&FileRecord> = records
        .iter()
        .filter(|record| record.derived_genres.is_empty())
        .collect();
    if misses.is_empty() {
        return Ok(());
    }

    let mut entries: Vec<UnknownGenreEntry> = Vec::new();
    for record in misses {
        let query_context = record
            .relative
            .file_name()
            .and_then(|n| n.to_str())
            .map(crate::dat::normalize_name_with_keywords)
            .and_then(|ctx| {
                if ctx.normalized.is_empty() {
                    None
                } else {
                    Some(ctx)
                }
            });
        let normalized_name = query_context.as_ref().map(|ctx| ctx.normalized.clone());
        let cache_key = normalized_name
            .as_ref()
            .map(|name| name.to_ascii_lowercase());
        let igdb_query = query_context.as_ref().map(|ctx| UnknownGenreQueryInfo {
            body: format!(
                "search \"{}\"; fields {}; limit 5;",
                ctx.normalized,
                crate::dat::IGDB_QUERY_FIELDS
            ),
            keywords: ctx.keywords.clone(),
            keyword_strategy: crate::dat::IGDB_KEYWORD_STRATEGY,
            platform_hint: record.derived_platform.clone(),
        });
        let mut reason = None;
        let mut cache_info = None;

        if !config.igdb_lookup_enabled() {
            reason = Some("igdb-disabled".to_string());
        } else if config.igdb_client_id.is_none() {
            reason = Some("missing-igdb-client-id".to_string());
        } else if config.igdb_token.is_none() {
            reason = Some("missing-igdb-token".to_string());
        }

        if reason.is_none() {
            match (cache, cache_key.as_deref()) {
                (Some(db), Some(key)) => match db.get_igdb_entry_by_key(key) {
                    Ok(Some(entry)) => {
                        cache_info = Some(UnknownGenreCacheInfo {
                            slug: entry.slug.clone(),
                            name: entry.name.clone(),
                            platforms: entry.platforms.clone(),
                            has_genres: !entry.genres.is_empty(),
                        });
                        reason = Some(if entry.genres.is_empty() {
                            "igdb-entry-missing-genres".to_string()
                        } else {
                            "igdb-entry-has-genres".to_string()
                        });
                    }
                    Ok(None) => {
                        reason = Some("igdb-cache-miss".to_string());
                    }
                    Err(err) => {
                        reason = Some(format!("igdb-cache-error: {}", err));
                    }
                },
                (Some(_), None) => {
                    reason = Some("normalized-name-empty".to_string());
                }
                (None, _) => {
                    reason = Some("igdb-cache-unavailable".to_string());
                }
            }
        }

        entries.push(UnknownGenreEntry {
            source: record.source.to_string_lossy().to_string(),
            relative: record.relative.to_string_lossy().to_string(),
            derived_platform: record.derived_platform.clone(),
            normalized_name,
            cache_key,
            igdb_query,
            igdb_mode: config.igdb_mode.clone(),
            igdb_client_configured: config.igdb_client_configured(),
            igdb_network_enabled: config.igdb_network_enabled(),
            reason: reason.unwrap_or_else(|| "unknown".to_string()),
            cache: cache_info,
        });
    }

    let mut report_path = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    report_path.push("igir_unknown_genres.json");
    let json = serde_json::to_string_pretty(&entries)?;
    fs::write(&report_path, json)?;
    log_diag_step(
        progress,
        config.diag,
        format!(
            "unknown-genre-report path={} total={}",
            report_path.display(),
            entries.len()
        ),
    );
    Ok(())
}

fn format_duration(duration: Duration) -> String {
    format!("{:.2}s", duration.as_secs_f64())
}

fn emit_summary(
    summary: &RunSummary,
    skipped: &[SkippedFile],
    steps: &[ActionOutcome],
    config: &Config,
    matched_dat: &[crate::dat::DatRom],
    action_durations: &[Duration],
    total_duration: Duration,
) {
    eprintln!("\n=== IGIR Summary ===");

    if summary.input_roots.is_empty() {
        eprintln!("Inputs: (none specified)");
    } else {
        let preview: Vec<String> = summary
            .input_roots
            .iter()
            .take(3)
            .map(|p| p.to_string_lossy().to_string())
            .collect();
        let remainder = summary.input_roots.len().saturating_sub(preview.len());
        if remainder > 0 {
            eprintln!(
                "Inputs ({}): {} (+{} more)",
                summary.input_roots.len(),
                preview.join(", "),
                remainder
            );
        } else {
            eprintln!(
                "Inputs ({}): {}",
                summary.input_roots.len(),
                preview.join(", ")
            );
        }
    }

    let copied_display = summary
        .files_copied
        .map(|n| n.to_string())
        .unwrap_or_else(|| "n/a".to_string());
    eprintln!(
        "Files -> processed: {} | skipped: {} | copied: {} | dat unmatched: {}",
        summary.files_processed, summary.files_skipped, copied_display, summary.dat_unmatched
    );

    if !steps.is_empty() {
        eprintln!("Actions executed:");
        for (idx, step) in steps.iter().enumerate() {
            let duration_label = action_durations
                .get(idx)
                .map(|d| format!(" [{}]", format_duration(*d)))
                .unwrap_or_default();
            eprintln!(
                "  - {:<9} {:<4} {}{}",
                format!("{:?}", step.action),
                step.status,
                step.note,
                duration_label
            );
        }
    }

    if summary.filters.region.is_some()
        || summary.filters.language.is_some()
        || summary.filters.include_regex.is_some()
        || summary.filters.exclude_regex.is_some()
    {
        eprintln!(
            "Filters -> region: {:?}, language: {:?}, include: {:?}, exclude: {:?}",
            summary.filters.region,
            summary.filters.language,
            summary.filters.include_regex,
            summary.filters.exclude_regex
        );
    }

    if !summary.skip_breakdown.is_empty() {
        eprintln!("Skip reasons:");
        for stat in &summary.skip_breakdown {
            eprintln!("  - {}: {}", stat.reason, stat.count);
        }

        if config.verbose >= 1 {
            let max_entries = 10usize;
            for entry in skipped.iter().take(max_entries) {
                eprintln!(
                    "    {} -> {}{}",
                    entry.path.to_string_lossy(),
                    entry.reason,
                    entry
                        .detail
                        .as_deref()
                        .map(|d| format!(" ({d})"))
                        .unwrap_or_default()
                );
            }
            if skipped.len() > max_entries {
                eprintln!("    ... and {} more", skipped.len() - max_entries);
            }
        }
    }

    if config.verbose == 0 {
        eprintln!("(increase --verbose for per-file diagnostics)");
    }

    eprintln!("Total runtime: {}", format_duration(total_duration));

    // Display a matched DAT summary (per-file details only when requested).
    if !matched_dat.is_empty() {
        if config.show_match_reasons || config.verbose >= 1 {
            eprintln!("\nMatched DAT entries (showing match reasons):");
            if config.verbose < 1 {
                eprintln!("(increase --verbose for per-file diagnostics to see entries)");
            } else {
                let max = 50usize;
                for dat in matched_dat.iter().take(max) {
                    let reasons = dat
                        .match_reasons
                        .as_ref()
                        .map(|v| v.join(", "))
                        .unwrap_or_else(|| "<unknown>".to_string());
                    eprintln!(
                        "  - {} ({}) -> reasons: {}",
                        dat.name,
                        dat.source_dat.display(),
                        reasons
                    );
                }
                if matched_dat.len() > max {
                    eprintln!(
                        "  ... and {} more matched dat entries",
                        matched_dat.len() - max
                    );
                }
            }
        } else {
            eprintln!("\nMatched DAT entries:");
            eprintln!("(increase --verbose for per-file diagnostics to see entries)");
        }
    }
}
