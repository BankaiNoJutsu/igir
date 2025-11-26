use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Context;
use walkdir::WalkDir;

use crate::checksum::compute_checksums;
use crate::config::Config;
use crate::types::{DirGameSubdirMode, FileRecord};
use crate::utils::build_globset;

pub fn collect_files(config: &Config) -> anyhow::Result<Vec<FileRecord>> {
    let exclude = build_globset(&config.input_exclude)?;
    let mut records = Vec::new();

    for input in &config.input {
        let metadata = fs::metadata(input).with_context(|| format!("reading input: {input:?}"))?;
        if metadata.is_file() {
            if exclude
                .as_ref()
                .is_some_and(|set| set.is_match(input.to_string_lossy().as_ref()))
            {
                continue;
            }

            let checksums = compute_checksums(input, config)?;
            records.push(FileRecord {
                source: input.clone(),
                relative: input
                    .file_name()
                    .map(PathBuf::from)
                    .unwrap_or_else(|| PathBuf::from("unknown")),
                size: metadata.len(),
                checksums,
            });
            continue;
        }

        for entry in WalkDir::new(input)
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
            let relative = path.strip_prefix(input).unwrap_or(&path).to_path_buf();

            records.push(FileRecord {
                size: fs::metadata(&path)?.len(),
                source: path,
                relative,
                checksums,
            });
        }
    }

    Ok(records)
}

pub fn select_letter_dir(name: &str, count: usize) -> String {
    let mut chars = name.chars();
    let mut dir = String::new();
    for _ in 0..count {
        if let Some(ch) = chars.next() {
            if ch.is_alphabetic() {
                dir.push(ch.to_ascii_uppercase());
            }
        }
    }

    if dir.is_empty() {
        "_misc".to_string()
    } else {
        dir
    }
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
        let letter = select_letter_dir(
            record
                .relative
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown"),
            config.dir_letter_count.unwrap_or(1),
        );
        base = base.join(letter);
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
