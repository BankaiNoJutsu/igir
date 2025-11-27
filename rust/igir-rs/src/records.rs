use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Context;
use glob::glob;
use walkdir::WalkDir;

use crate::checksum::compute_checksums;
use crate::config::Config;
use crate::types::{ChecksumSet, DirGameSubdirMode, FileRecord};
use crate::utils::build_globset;
use regex::Regex;

pub fn collect_files(config: &Config) -> anyhow::Result<Vec<FileRecord>> {
    let exclude = build_globset(&config.input_exclude)?;
    let mut records = Vec::new();

    for input in &config.input {
        let mut matched_inputs = Vec::new();
        if has_glob(input) {
            for entry in glob(input.to_string_lossy().as_ref())? {
                let path = entry?;
                matched_inputs.push(path);
            }
        } else {
            matched_inputs.push(input.clone());
        }

        for matched in matched_inputs {
            let metadata =
                fs::metadata(&matched).with_context(|| format!("reading input: {matched:?}"))?;
            if metadata.is_file() {
                if exclude
                    .as_ref()
                    .is_some_and(|set| set.is_match(matched.to_string_lossy().as_ref()))
                {
                    continue;
                }

                let checksums = compute_checksums(&matched, config)?;
                records.push(FileRecord {
                    source: matched.clone(),
                    relative: matched
                        .file_name()
                        .map(PathBuf::from)
                        .unwrap_or_else(|| PathBuf::from("unknown")),
                    size: metadata.len(),
                    checksums,
                    letter_dir: None,
                });
                continue;
            }

            for entry in WalkDir::new(&matched)
                .into_iter()
                .filter_map(Result::ok)
                .filter(|e| e.file_type().is_file())
            {
                let path = entry.into_path();
                if exclude
                    .as_ref()
                    .is_some_and(|set| set.is_match(path.to_string_lossy().as_ref()))
                {
                    continue;
                }

                let checksums = compute_checksums(&path, config)?;
                let relative = path.strip_prefix(&matched).unwrap_or(&path).to_path_buf();

                records.push(FileRecord {
                    size: fs::metadata(&path)?.len(),
                    source: path,
                    relative,
                    checksums,
                    letter_dir: None,
                });
            }
        }
    }

    records = apply_filters(records, config)?;

    if config.dir_letter {
        assign_letter_dirs(&mut records, config)?;
    }

    Ok(records)
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

fn apply_filters(records: Vec<FileRecord>, config: &Config) -> anyhow::Result<Vec<FileRecord>> {
    let mut filtered = records;

    filtered = filter_by_regex(filtered, config)?;
    filtered = filter_by_region_and_language(filtered, config);

    Ok(filtered)
}

fn filter_by_regex(records: Vec<FileRecord>, config: &Config) -> anyhow::Result<Vec<FileRecord>> {
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

    let filtered = records
        .into_iter()
        .filter(|record| {
            let name = record.relative.to_string_lossy();
            let included = include.as_ref().map_or(true, |regex| regex.is_match(&name));
            let excluded = exclude
                .as_ref()
                .map_or(false, |regex| regex.is_match(&name));
            included && !excluded
        })
        .collect();

    Ok(filtered)
}

#[derive(Clone)]
struct CandidateRecord {
    record: FileRecord,
    region: Option<String>,
    languages: Vec<String>,
    title: String,
}

fn filter_by_region_and_language(records: Vec<FileRecord>, config: &Config) -> Vec<FileRecord> {
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
        let title = normalize_title(name);

        grouped
            .entry(title.clone())
            .or_default()
            .push(CandidateRecord {
                record,
                region,
                languages,
                title,
            });
    }

    let mut kept = Vec::new();

    for (_title, mut candidates) in grouped {
        candidates
            .sort_by(|a, b| compare_candidates(a, b, &region_preferences, &language_preferences));

        if let Some(best) = candidates.into_iter().next() {
            let region_match = best
                .region
                .as_ref()
                .and_then(|r| region_preferences.iter().position(|pref| pref == r))
                .is_some();
            let language_match = best
                .languages
                .iter()
                .any(|lang| language_preferences.iter().any(|pref| pref == lang));

            if (!region_preferences.is_empty() && region_match)
                || (!language_preferences.is_empty() && language_match)
            {
                kept.push(best.record);
            }
        }
    }

    kept
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

    lang_rank_a.cmp(&lang_rank_b)
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

fn parse_list(raw: Option<&String>) -> Vec<String> {
    raw.map(|r| {
        r.split(',')
            .map(|entry| entry.trim().to_uppercase())
            .filter(|entry| !entry.is_empty())
            .collect()
    })
    .unwrap_or_default()
}

fn extract_tags(name: &str) -> Vec<String> {
    let mut tags = Vec::new();
    let mut current = String::new();
    let mut depth = 0usize;

    for ch in name.chars() {
        match ch {
            '(' | '[' => {
                if depth == 0 {
                    current.clear();
                }
                depth += 1;
            }
            ')' | ']' => {
                if depth > 0 {
                    depth -= 1;
                    if depth == 0 && !current.trim().is_empty() {
                        tags.push(current.trim().to_string());
                        current.clear();
                    }
                }
            }
            _ => {
                if depth > 0 {
                    current.push(ch);
                }
            }
        }
    }

    tags
}

fn normalize_title(name: &str) -> String {
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

fn detect_region(tags: &[String]) -> Option<String> {
    for tag in tags {
        for token in tag_tokens(tag) {
            match token.as_str() {
                "EUROPE" | "EURO" | "EUR" | "EU" => return Some("EUR".to_string()),
                "USA" | "US" => return Some("USA".to_string()),
                "WORLD" => return Some("WORLD".to_string()),
                _ => {}
            }
        }
    }

    None
}

fn detect_languages(tags: &[String]) -> Vec<String> {
    let mut langs = Vec::new();

    for tag in tags {
        for token in tag_tokens(tag) {
            let language = match token.as_str() {
                "EN" | "ENG" | "ENGLISH" => Some("EN".to_string()),
                "FR" | "FRE" | "FRENCH" => Some("FR".to_string()),
                "DE" | "GER" | "GERMAN" => Some("DE".to_string()),
                "ES" | "SPA" | "SPANISH" => Some("ES".to_string()),
                _ => None,
            };

            if let Some(lang) = language {
                if !langs.contains(&lang) {
                    langs.push(lang);
                }
            }
        }
    }

    langs
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
    let mut base = config
        .output
        .as_ref()
        .cloned()
        .unwrap_or_else(|| PathBuf::from("output"));

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
        Action, ArchiveChecksumMode, Checksum, DirGameSubdirMode, FixExtensionMode, LinkMode,
        MergeMode, MoveDeleteDirsMode, ZipFormat,
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
            enable_hasheous: false,
            igdb_client_id: None,
            igdb_token: None,
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

        let filtered = filter_by_region_and_language(records, &config);

        assert_eq!(filtered.len(), 1);
        assert_eq!(
            filtered[0].relative,
            PathBuf::from("Super Mario World (Europe).sfc")
        );
    }

    #[test]
    fn skips_titles_without_preferred_regions_or_languages() {
        let config = test_config(Some("EUR,USA"), Some("EN"));
        let records = vec![
            dummy_record("Donkey Kong Country (Japan).sfc"),
            dummy_record("Donkey Kong Country (Korea).sfc"),
        ];

        let filtered = filter_by_region_and_language(records, &config);

        assert!(filtered.is_empty());
    }
}
