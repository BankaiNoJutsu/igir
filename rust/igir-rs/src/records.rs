use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Context;
use glob::glob;
use walkdir::WalkDir;

use crate::checksum::compute_checksums;
use crate::config::Config;
use crate::types::{DirGameSubdirMode, FileRecord};
use crate::utils::build_globset;

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
