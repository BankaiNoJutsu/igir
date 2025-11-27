use std::collections::HashMap;
use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;

use anyhow::Context;
use zip::write::FileOptions;

use crate::config::Config;
use crate::dat::{dat_unmatched, load_dat_roms, online_lookup};
use crate::records::{collect_files, ensure_parent, resolve_output_path};
use crate::types::{
    Action, ActionOutcome, ChecksumSet, ExecutionPlan, FileRecord, LinkMode, ZipFormat,
};
use crate::utils::build_globset;
use walkdir::WalkDir;

pub fn copy_record(record: &FileRecord, config: &Config) -> anyhow::Result<PathBuf> {
    let target = resolve_output_path(record, config);
    ensure_parent(&target)?;

    if target.exists() {
        if !config.overwrite && !config.overwrite_invalid {
            return Ok(target);
        }
    }

    fs::copy(&record.source, &target)
        .with_context(|| format!("copying {:?} to {:?}", record.source, target))?;
    Ok(target)
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
    let mut written = Vec::new();
    let extension = record
        .source
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();

    if extension == "zip" {
        let file = fs::File::open(&record.source)?;
        let mut archive = zip::ZipArchive::new(file)?;
        for i in 0..archive.len() {
            let mut file = archive.by_index(i)?;
            if file.is_dir() {
                continue;
            }

            let out_path = resolve_output_path(
                &FileRecord {
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
                },
                config,
            );
            ensure_parent(&out_path)?;

            let mut output = fs::File::create(&out_path)?;
            io::copy(&mut file, &mut output)?;
            written.push(out_path);
        }
    } else {
        written.push(copy_record(record, config)?);
    }

    Ok(written)
}

pub fn zip_record(record: &FileRecord, config: &Config) -> anyhow::Result<PathBuf> {
    let target = resolve_output_path(record, config).with_extension("zip");
    ensure_parent(&target)?;

    let mut file = fs::File::create(&target)?;
    let mut zip = zip::ZipWriter::new(&mut file);
    let options = match config.zip_format {
        ZipFormat::Torrentzip => {
            FileOptions::default().compression_method(zip::CompressionMethod::Stored)
        }
        ZipFormat::Rvzstd => {
            FileOptions::default().compression_method(zip::CompressionMethod::Zstd)
        }
    };

    let mut input = fs::File::open(&record.source)?;
    zip.start_file(
        record
            .relative
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("rom.bin"),
        options,
    )?;
    io::copy(&mut input, &mut zip)?;
    zip.finish()?;

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

    let json = serde_json::to_string_pretty(records)?;
    fs::write(&target, json)?;
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

pub fn write_fixdat(records: &[FileRecord], config: &Config) -> anyhow::Result<PathBuf> {
    let mut target = config
        .output
        .clone()
        .unwrap_or_else(|| PathBuf::from("output"));
    target.push("fixdat.json");
    ensure_parent(&target)?;

    let mut missing = Vec::new();
    for record in records {
        if !resolve_output_path(record, config).exists() {
            missing.push(record);
        }
    }

    let json = serde_json::to_string_pretty(&missing)?;
    fs::write(&target, json)?;
    Ok(target)
}

pub fn clean_output(records: &[FileRecord], config: &Config) -> anyhow::Result<Vec<PathBuf>> {
    let mut cleaned = Vec::new();
    let mut expected = HashMap::new();
    for record in records {
        expected.insert(resolve_output_path(record, config), ());
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

pub fn perform_actions(config: &Config) -> anyhow::Result<ExecutionPlan> {
    let records = collect_files(config)?;
    let dat_roms = load_dat_roms(config)?;
    let (unmatched, matched) = dat_unmatched(&records, &dat_roms);
    let online_matches = online_lookup(&unmatched, config)?;
    let mut steps = Vec::new();

    for action in &config.commands {
        match action {
            Action::Copy => {
                for record in &records {
                    let _ = copy_record(record, config)?;
                }
                steps.push(ActionOutcome {
                    action: action.clone(),
                    status: "ok".to_string(),
                    note: "Copied input files to output".to_string(),
                });
            }
            Action::Move => {
                for record in &records {
                    let _ = move_record(record, config)?;
                }
                steps.push(ActionOutcome {
                    action: action.clone(),
                    status: "ok".to_string(),
                    note: "Moved input files to output".to_string(),
                });
            }
            Action::Link => {
                for record in &records {
                    let _ = link_record(record, config)?;
                }
                steps.push(ActionOutcome {
                    action: action.clone(),
                    status: "ok".to_string(),
                    note: format!("Linked files using {:?}", config.link_mode),
                });
            }
            Action::Extract => {
                for record in &records {
                    let _ = extract_record(record, config)?;
                }
                steps.push(ActionOutcome {
                    action: action.clone(),
                    status: "ok".to_string(),
                    note: "Extracted archives and copied loose files".to_string(),
                });
            }
            Action::Zip => {
                for record in &records {
                    let _ = zip_record(record, config)?;
                }
                steps.push(ActionOutcome {
                    action: action.clone(),
                    status: "ok".to_string(),
                    note: format!("Zipped files using {:?}", config.zip_format),
                });
            }
            Action::Playlist => {
                let _ = playlist(&records, config)?;
                steps.push(ActionOutcome {
                    action: action.clone(),
                    status: "ok".to_string(),
                    note: "Generated playlist".to_string(),
                });
            }
            Action::Report => {
                let _ = write_report(&records, config)?;
                steps.push(ActionOutcome {
                    action: action.clone(),
                    status: "ok".to_string(),
                    note: "Wrote report".to_string(),
                });
            }
            Action::Dir2dat => {
                let _ = write_dir2dat(&records, config)?;
                steps.push(ActionOutcome {
                    action: action.clone(),
                    status: "ok".to_string(),
                    note: "Generated dir2dat JSON".to_string(),
                });
            }
            Action::Fixdat => {
                let _ = write_fixdat(&records, config)?;
                steps.push(ActionOutcome {
                    action: action.clone(),
                    status: "ok".to_string(),
                    note: "Generated fixdat JSON".to_string(),
                });
            }
            Action::Clean => {
                let cleaned = clean_output(&records, config)?;
                steps.push(ActionOutcome {
                    action: action.clone(),
                    status: "ok".to_string(),
                    note: format!("Cleaned {} files", cleaned.len()),
                });
            }
            Action::Test => {
                steps.push(ActionOutcome {
                    action: action.clone(),
                    status: "ok".to_string(),
                    note: "Validated configuration only".to_string(),
                });
            }
        }
    }

    if !dat_roms.is_empty() {
        steps.push(ActionOutcome {
            action: Action::Fixdat,
            status: "info".to_string(),
            note: format!(
                "Matched {} DAT roms, {} unmatched{}",
                matched,
                unmatched.len(),
                if !online_matches.is_empty() {
                    format!("; {} online hints", online_matches.len())
                } else {
                    String::new()
                }
            ),
        });
    }

    Ok(ExecutionPlan {
        config: config.clone(),
        steps,
        files_processed: records.len(),
        dat_unmatched: unmatched,
        online_matches,
    })
}
