use anyhow::Context;
use std::collections::HashSet;
use std::fs::File;
use std::path::Path;
use std::process::Command;
use std::sync::mpsc::Sender;

#[cfg(test)]
use std::sync::mpsc;
use zip::read::ZipArchive;

use crate::checksum::compute_checksums_stream;
use crate::config::Config;
use crate::progress::ProgressEvent;
use crate::types::FileRecord;

/// Scan a local zip archive and return in-archive FileRecords (checksums computed from extracted bytes)
pub fn scan_zip_entries(
    path: &Path,
    config: &Config,
    progress: Option<Sender<ProgressEvent>>,
) -> anyhow::Result<Vec<FileRecord>> {
    let f = File::open(path).with_context(|| format!("opening archive: {:?}", path))?;
    let mut zip = ZipArchive::new(f)?;
    let mut out = Vec::new();

    for i in 0..zip.len() {
        let mut entry = zip.by_index(i)?;
        if entry.is_file() {
            let name = entry.name().to_string();
            let (checksums, size) = compute_checksums_stream(&mut entry, config)?;
            let rec = FileRecord {
                source: path.to_path_buf(),
                relative: Path::new(&name).to_path_buf(),
                size,
                checksums,
                letter_dir: None,
                derived_platform: None,
                derived_genres: Vec::new(),
                derived_region: None,
                derived_languages: Vec::new(),
                scan_info: None,
            };
            out.push(rec);

            if let Some(tx) = progress.as_ref() {
                let hint = path.join(Path::new(&name));
                let _ = tx.send(ProgressEvent::hashing(hint, size, Some(size)));
            }
        }
    }

    Ok(out)
}

/// Try to list entries from a 7z archive and extract a specific entry to bytes using the system 7z binary.
/// This is a pragmatic approach when no native crate is available.
pub fn scan_7z_entries(
    path: &Path,
    config: &Config,
    progress: Option<Sender<ProgressEvent>>,
) -> anyhow::Result<Vec<FileRecord>> {
    // check for 7z or 7za
    let exe = match which::which("7z").or_else(|_| which::which("7za")) {
        Ok(path) => path,
        Err(_) => return Ok(Vec::new()),
    };
    // list entries
    let output = Command::new(&exe)
        .arg("l")
        .arg(path.as_os_str())
        .output()
        .with_context(|| format!("running 7z to list archive: {:?}", path))?;

    if !output.status.success() {
        // listing failed; attempt to extract to a temp dir as a fallback
        return extract_7z_to_temp_and_scan(&exe, path, None, config, progress.clone());
    }

    let text = String::from_utf8_lossy(&output.stdout).to_string();
    let mut entry_names: HashSet<String> = HashSet::new();
    // Attempt robust parsing: locate the header line that contains 'Name' and read filenames from that column
    if let Some(header_line) = text.lines().find(|l| l.contains("Name")) {
        if let Some(name_idx) = header_line.find("Name") {
            // find the block of lines under the listing table: starts after a dashed line that precedes header
            let mut in_table = false;
            for line in text.lines() {
                if !in_table {
                    if line.trim_start().starts_with("----") {
                        // next non-dash line might be header; continue to find header row
                        in_table = true;
                        continue;
                    }
                    continue;
                }
                // stop on footer dashes
                if line.trim_start().starts_with("----") {
                    break;
                }
                // skip header itself
                if line.contains("Name") && line.contains("Size") {
                    continue;
                }
                // If the line is long enough, take the substring starting at name_idx
                if line.len() > name_idx {
                    let name = line[name_idx..].trim().to_string();
                    if !name.is_empty() {
                        entry_names.insert(name);
                    }
                }
            }
        }
    }

    // If robust parsing didn't find anything, fall back to the previous heuristic
    if entry_names.is_empty() {
        for line in text.lines() {
            // naive parse: file lines typically have date time attr size name; try to capture lines with a name
            if line.contains("      ") && !line.starts_with("---------") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 6 {
                    let name = parts[5..].join(" ");
                    if !name.is_empty() {
                        entry_names.insert(name);
                    }
                }
            }
        }
    }

    // If no names were discovered, fall back to extracting everything
    if entry_names.is_empty() {
        return extract_7z_to_temp_and_scan(&exe, path, None, config, progress);
    }

    let mut names: Vec<String> = entry_names.into_iter().collect();
    names.sort();

    extract_7z_to_temp_and_scan(&exe, path, Some(&names), config, progress)
}

fn extract_7z_to_temp_and_scan(
    exe: &std::path::PathBuf,
    path: &Path,
    selection: Option<&[String]>,
    config: &Config,
    progress: Option<Sender<ProgressEvent>>,
) -> anyhow::Result<Vec<FileRecord>> {
    use tempfile::tempdir;

    let tmp = tempdir()?;
    let tmp_path = tmp.path();

    // x <archive> -o<dir> -y
    let mut cmd = Command::new(exe);
    cmd.arg("x")
        .arg(path.as_os_str())
        .arg(format!("-o{}", tmp_path.to_string_lossy()))
        .arg("-y");

    if let Some(files) = selection {
        if !files.is_empty() {
            cmd.args(files);
        }
    }

    let status = cmd
        .status()
        .with_context(|| format!("extracting 7z archive to tempdir: {:?}", path))?;

    if !status.success() {
        return Ok(Vec::new());
    }

    let mut out = Vec::new();
    for entry in walkdir::WalkDir::new(tmp_path)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_file())
    {
        let p = entry.into_path();
        if let Ok(mut file) = File::open(&p) {
            let rel = p.strip_prefix(tmp_path).unwrap_or(&p).to_path_buf();
            let rel_hint = rel.clone();
            let (checksums, size) = compute_checksums_stream(&mut file, config)?;
            out.push(FileRecord {
                source: path.to_path_buf(),
                relative: rel,
                size,
                checksums,
                letter_dir: None,
                derived_platform: None,
                derived_genres: Vec::new(),
                derived_region: None,
                derived_languages: Vec::new(),
                scan_info: None,
            });

            if let Some(tx) = progress.as_ref() {
                let hint = path.join(&rel_hint);
                let _ = tx.send(ProgressEvent::hashing(hint, size, Some(size)));
            }
        }
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::progress::ProgressEvent;
    use std::io::Write;
    use std::sync::mpsc;
    use tempfile::NamedTempFile;
    use zip::write::FileOptions;

    #[test]
    fn scan_zip_entries_basic() {
        let f = NamedTempFile::new().unwrap();
        {
            let mut zipw = zip::ZipWriter::new(f.reopen().unwrap());
            zipw.start_file::<_, ()>("a.txt", FileOptions::default())
                .unwrap();
            zipw.write_all(b"hello").unwrap();
            zipw.finish().unwrap();
        }

        // construct a minimal Config for testing; only checksum range matters here
        let cfg = crate::config::Config {
            commands: Vec::new(),
            input: Vec::new(),
            input_exclude: Vec::new(),
            input_checksum_quick: false,
            input_checksum_min: crate::types::Checksum::Crc32,
            input_checksum_max: Some(crate::types::Checksum::Sha256),
            input_checksum_archives: crate::types::ArchiveChecksumMode::Auto,
            dat: Vec::new(),
            dat_exclude: Vec::new(),
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
            patch: Vec::new(),
            patch_exclude: Vec::new(),
            output: None,
            dir_mirror: false,
            dir_dat_mirror: false,
            dir_dat_name: false,
            dir_dat_description: false,
            dir_letter: false,
            dir_letter_count: None,
            dir_letter_limit: None,
            dir_letter_group: false,
            dir_game_subdir: crate::types::DirGameSubdirMode::Never,
            fix_extension: crate::types::FixExtensionMode::Never,
            overwrite: false,
            overwrite_invalid: false,
            move_delete_dirs: crate::types::MoveDeleteDirsMode::Never,
            clean_exclude: Vec::new(),
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
            cache_only: false,
            cache_db: None,
            hash_threads: None,
            scan_threads: None,
            show_match_reasons: false,
            online_timeout_secs: Some(5),
            online_max_retries: Some(3),
            online_throttle_ms: None,
        };
        let recs = scan_zip_entries(f.path(), &cfg, None).unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].relative.to_string_lossy(), "a.txt");

        // Ensure progress events are emitted when requested.
        let (tx, rx) = mpsc::channel();
        let _ = scan_zip_entries(f.path(), &cfg, Some(tx)).unwrap();
        let events: Vec<ProgressEvent> = rx.into_iter().collect();
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.bytes_done(), recs[0].size);
        assert_eq!(event.total_bytes(), Some(recs[0].size));
        let filename = event
            .path()
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("");
        assert_eq!(filename, "a.txt");
    }
}

#[test]
fn scan_7z_entries_if_available() {
    // Only run this test if 7z or 7za is present in PATH
    let exe = which::which("7z").or_else(|_| which::which("7za"));
    if exe.is_err() {
        eprintln!("skipping 7z integration test; 7z not found");
        return;
    }
    // Create a simple zip first and then convert with 7z if possible
    use std::io::Write;
    use tempfile::NamedTempFile;
    use zip::write::FileOptions;

    let f = NamedTempFile::new().unwrap();
    {
        let mut zipw = zip::ZipWriter::new(f.reopen().unwrap());
        zipw.start_file::<_, ()>("x.txt", FileOptions::default())
            .unwrap();
        zipw.write_all(b"world").unwrap();
        zipw.finish().unwrap();
    }

    // Use 7z to create a .7z archive from the zip content
    let out7 = NamedTempFile::new().unwrap();
    let status = Command::new(exe.unwrap())
        .arg("a")
        .arg(out7.path())
        .arg(f.path())
        .status();
    if status.is_err() || !status.unwrap().success() {
        eprintln!("skipping 7z integration test; failed to create 7z archive");
        return;
    }

    // construct minimal config
    let cfg = crate::config::Config {
        commands: Vec::new(),
        input: Vec::new(),
        input_exclude: Vec::new(),
        input_checksum_quick: false,
        input_checksum_min: crate::types::Checksum::Crc32,
        input_checksum_max: Some(crate::types::Checksum::Sha256),
        input_checksum_archives: crate::types::ArchiveChecksumMode::Auto,
        dat: Vec::new(),
        dat_exclude: Vec::new(),
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
        patch: Vec::new(),
        patch_exclude: Vec::new(),
        output: None,
        dir_mirror: false,
        dir_dat_mirror: false,
        dir_dat_name: false,
        dir_dat_description: false,
        dir_letter: false,
        dir_letter_count: None,
        dir_letter_limit: None,
        dir_letter_group: false,
        dir_game_subdir: crate::types::DirGameSubdirMode::Never,
        fix_extension: crate::types::FixExtensionMode::Never,
        overwrite: false,
        overwrite_invalid: false,
        move_delete_dirs: crate::types::MoveDeleteDirsMode::Never,
        clean_exclude: Vec::new(),
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
        cache_only: false,
        cache_db: None,
        hash_threads: None,
        scan_threads: None,
        show_match_reasons: false,
        online_timeout_secs: Some(5),
        online_max_retries: Some(3),
        online_throttle_ms: None,
    };

    let recs = scan_7z_entries(out7.path(), &cfg, None).unwrap();
    assert!(recs.len() >= 1);

    let (tx, rx) = mpsc::channel();
    let _ = scan_7z_entries(out7.path(), &cfg, Some(tx)).unwrap();
    let events: Vec<ProgressEvent> = rx.into_iter().collect();
    assert!(!events.is_empty());
    let first = &events[0];
    assert!(first.bytes_done() > 0);
    assert_eq!(first.total_bytes(), Some(first.bytes_done()));
}

#[test]
fn scan_7z_nested_dirs_if_available() {
    let exe = which::which("7z").or_else(|_| which::which("7za"));
    if exe.is_err() {
        eprintln!("skipping 7z nested test; 7z not found");
        return;
    }
    let exe = exe.unwrap();

    use std::io::Write;
    use tempfile::NamedTempFile;
    use zip::write::FileOptions;

    // create a zip with nested directories
    let f = NamedTempFile::new().unwrap();
    {
        let mut zipw = zip::ZipWriter::new(f.reopen().unwrap());
        zipw.start_file::<_, ()>("dir/x.txt", FileOptions::default())
            .unwrap();
        zipw.write_all(b"deep").unwrap();
        zipw.finish().unwrap();
    }

    let out7 = NamedTempFile::new().unwrap();
    let status = Command::new(&exe)
        .arg("a")
        .arg(out7.path())
        .arg(f.path())
        .status();
    if status.is_err() || !status.unwrap().success() {
        eprintln!("skipping 7z nested test; failed to create 7z");
        return;
    }

    let cfg = crate::config::Config {
        commands: Vec::new(),
        input: Vec::new(),
        input_exclude: Vec::new(),
        input_checksum_quick: false,
        input_checksum_min: crate::types::Checksum::Crc32,
        input_checksum_max: Some(crate::types::Checksum::Sha256),
        input_checksum_archives: crate::types::ArchiveChecksumMode::Auto,
        dat: Vec::new(),
        dat_exclude: Vec::new(),
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
        patch: Vec::new(),
        patch_exclude: Vec::new(),
        output: None,
        dir_mirror: false,
        dir_dat_mirror: false,
        dir_dat_name: false,
        dir_dat_description: false,
        dir_letter: false,
        dir_letter_count: None,
        dir_letter_limit: None,
        dir_letter_group: false,
        dir_game_subdir: crate::types::DirGameSubdirMode::Never,
        fix_extension: crate::types::FixExtensionMode::Never,
        overwrite: false,
        overwrite_invalid: false,
        move_delete_dirs: crate::types::MoveDeleteDirsMode::Never,
        clean_exclude: Vec::new(),
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
        cache_only: false,
        cache_db: None,
        hash_threads: None,
        scan_threads: None,
        show_match_reasons: false,
        online_timeout_secs: Some(5),
        online_max_retries: Some(3),
        online_throttle_ms: None,
    };
    let cfg = crate::config::Config {
        commands: Vec::new(),
        input: Vec::new(),
        input_exclude: Vec::new(),
        input_checksum_quick: false,
        input_checksum_min: crate::types::Checksum::Crc32,
        input_checksum_max: Some(crate::types::Checksum::Sha256),
        input_checksum_archives: crate::types::ArchiveChecksumMode::Auto,
        dat: Vec::new(),
        dat_exclude: Vec::new(),
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
        patch: Vec::new(),
        patch_exclude: Vec::new(),
        output: None,
        dir_mirror: false,
        dir_dat_mirror: false,
        dir_dat_name: false,
        dir_dat_description: false,
        dir_letter: false,
        dir_letter_count: None,
        dir_letter_limit: None,
        dir_letter_group: false,
        dir_game_subdir: crate::types::DirGameSubdirMode::Never,
        fix_extension: crate::types::FixExtensionMode::Never,
        overwrite: false,
        overwrite_invalid: false,
        move_delete_dirs: crate::types::MoveDeleteDirsMode::Never,
        clean_exclude: Vec::new(),
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
        cache_only: false,
        cache_db: None,
        hash_threads: None,
        scan_threads: None,
        show_match_reasons: false,
        online_timeout_secs: Some(5),
        online_max_retries: Some(3),
        online_throttle_ms: None,
    };

    let recs = scan_7z_entries(out7.path(), &cfg, None).unwrap();
    assert!(
        recs.iter()
            .any(|r| r.relative.to_string_lossy().ends_with("dir/x.txt"))
    );
}

#[test]
fn scan_7z_large_archive_if_available() {
    let exe = which::which("7z").or_else(|_| which::which("7za"));
    if exe.is_err() {
        eprintln!("skipping 7z large test; 7z not found");
        return;
    }
    let exe = exe.unwrap();

    use std::io::Write;
    use tempfile::NamedTempFile;
    use zip::write::FileOptions;

    let f = NamedTempFile::new().unwrap();
    {
        let mut zipw = zip::ZipWriter::new(f.reopen().unwrap());
        for i in 0..50 {
            let name = format!("f{}.txt", i);
            zipw.start_file::<_, ()>(&name, FileOptions::default())
                .unwrap();
            zipw.write_all(b"data").unwrap();
        }
        zipw.finish().unwrap();
    }

    let out7 = NamedTempFile::new().unwrap();
    let status = Command::new(&exe)
        .arg("a")
        .arg(out7.path())
        .arg(f.path())
        .status();
    if status.is_err() || !status.unwrap().success() {
        eprintln!("skipping 7z large test; failed to create 7z");
        return;
    }

    let cfg = crate::config::Config {
        commands: Vec::new(),
        input: Vec::new(),
        input_exclude: Vec::new(),
        input_checksum_quick: false,
        input_checksum_min: crate::types::Checksum::Crc32,
        input_checksum_max: Some(crate::types::Checksum::Sha256),
        input_checksum_archives: crate::types::ArchiveChecksumMode::Auto,
        dat: Vec::new(),
        dat_exclude: Vec::new(),
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
        patch: Vec::new(),
        patch_exclude: Vec::new(),
        output: None,
        dir_mirror: false,
        dir_dat_mirror: false,
        dir_dat_name: false,
        dir_dat_description: false,
        dir_letter: false,
        dir_letter_count: None,
        dir_letter_limit: None,
        dir_letter_group: false,
        dir_game_subdir: crate::types::DirGameSubdirMode::Never,
        fix_extension: crate::types::FixExtensionMode::Never,
        overwrite: false,
        overwrite_invalid: false,
        move_delete_dirs: crate::types::MoveDeleteDirsMode::Never,
        clean_exclude: Vec::new(),
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
        cache_only: false,
        cache_db: None,
        hash_threads: None,
        scan_threads: None,
        show_match_reasons: false,
        online_timeout_secs: Some(5),
        online_max_retries: Some(3),
        online_throttle_ms: None,
    };

    let recs = scan_7z_entries(out7.path(), &cfg, None).unwrap();
    assert!(recs.len() >= 50);
}

#[test]
fn scan_7z_edge_case_filenames_if_available() {
    let exe = which::which("7z").or_else(|_| which::which("7za"));
    if exe.is_err() {
        eprintln!("skipping 7z edge-case test; 7z not found");
        return;
    }
    let exe = exe.unwrap();

    use std::io::Write;
    use tempfile::NamedTempFile;
    use zip::write::FileOptions;

    // filenames with spaces, non-ASCII and weird characters
    let f = NamedTempFile::new().unwrap();
    {
        let mut zipw = zip::ZipWriter::new(f.reopen().unwrap());
        zipw.start_file::<_, ()>("file with spaces.txt", FileOptions::default())
            .unwrap();
        zipw.write_all(b"one").unwrap();
        zipw.start_file::<_, ()>("unicodé-文件.bin", FileOptions::default())
            .unwrap();
        zipw.write_all(b"two").unwrap();
        zipw.start_file::<_, ()>("weird_#%&[]{}.txt", FileOptions::default())
            .unwrap();
        zipw.write_all(b"three").unwrap();
        zipw.finish().unwrap();
    }

    let out7 = NamedTempFile::new().unwrap();
    let status = Command::new(&exe)
        .arg("a")
        .arg(out7.path())
        .arg(f.path())
        .status();
    if status.is_err() || !status.unwrap().success() {
        eprintln!("skipping 7z edge-case test; failed to create 7z");
        return;
    }

    let cfg = crate::config::Config {
        commands: Vec::new(),
        input: Vec::new(),
        input_exclude: Vec::new(),
        input_checksum_quick: false,
        input_checksum_min: crate::types::Checksum::Crc32,
        input_checksum_max: Some(crate::types::Checksum::Sha256),
        input_checksum_archives: crate::types::ArchiveChecksumMode::Auto,
        dat: Vec::new(),
        dat_exclude: Vec::new(),
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
        patch: Vec::new(),
        patch_exclude: Vec::new(),
        output: None,
        dir_mirror: false,
        dir_dat_mirror: false,
        dir_dat_name: false,
        dir_dat_description: false,
        dir_letter: false,
        dir_letter_count: None,
        dir_letter_limit: None,
        dir_letter_group: false,
        dir_game_subdir: crate::types::DirGameSubdirMode::Never,
        fix_extension: crate::types::FixExtensionMode::Never,
        overwrite: false,
        overwrite_invalid: false,
        move_delete_dirs: crate::types::MoveDeleteDirsMode::Never,
        clean_exclude: Vec::new(),
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
        cache_only: false,
        cache_db: None,
        hash_threads: None,
        scan_threads: None,
        show_match_reasons: false,
        online_timeout_secs: Some(5),
        online_max_retries: Some(3),
        online_throttle_ms: None,
    };
    let cfg = crate::config::Config {
        commands: Vec::new(),
        input: Vec::new(),
        input_exclude: Vec::new(),
        input_checksum_quick: false,
        input_checksum_min: crate::types::Checksum::Crc32,
        input_checksum_max: Some(crate::types::Checksum::Sha256),
        input_checksum_archives: crate::types::ArchiveChecksumMode::Auto,
        dat: Vec::new(),
        dat_exclude: Vec::new(),
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
        patch: Vec::new(),
        patch_exclude: Vec::new(),
        output: None,
        dir_mirror: false,
        dir_dat_mirror: false,
        dir_dat_name: false,
        dir_dat_description: false,
        dir_letter: false,
        dir_letter_count: None,
        dir_letter_limit: None,
        dir_letter_group: false,
        dir_game_subdir: crate::types::DirGameSubdirMode::Never,
        fix_extension: crate::types::FixExtensionMode::Never,
        overwrite: false,
        overwrite_invalid: false,
        move_delete_dirs: crate::types::MoveDeleteDirsMode::Never,
        clean_exclude: Vec::new(),
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
        cache_only: false,
        cache_db: None,
        hash_threads: None,
        scan_threads: None,
        show_match_reasons: false,
        online_timeout_secs: Some(5),
        online_max_retries: Some(3),
        online_throttle_ms: None,
    };

    let recs = scan_7z_entries(out7.path(), &cfg, None).unwrap();
    let names: Vec<String> = recs
        .iter()
        .map(|r| r.relative.to_string_lossy().to_string())
        .collect();
    assert!(names.iter().any(|n| n == "file with spaces.txt"));
    assert!(names.iter().any(|n| n == "unicodé-文件.bin"));
    assert!(names.iter().any(|n| n == "weird_#%&[]{}.txt"));
}
