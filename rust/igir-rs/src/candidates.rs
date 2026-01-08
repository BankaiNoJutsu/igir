use serde::Serialize;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::config::Config;
use crate::types::FileRecord;
use crate::write_candidate::WriteCandidate;
use rayon::prelude::*;

#[derive(Debug, Clone, Serialize)]
pub struct Candidate {
    pub name: String,
    pub matches: Vec<FileRecord>,
}

/// Group FileRecords by normalized title into candidate groups.
#[allow(dead_code)]
pub fn group_candidates(records: &[FileRecord]) -> HashMap<String, Vec<FileRecord>> {
    let mut map: HashMap<String, Vec<FileRecord>> = HashMap::new();

    for rec in records {
        let base_name = rec
            .relative
            .file_stem()
            .and_then(|s| s.to_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| rec.relative.to_string_lossy().to_string());
        let normalized = crate::records::normalize_title(&base_name);
        let key = if normalized.is_empty() {
            base_name
        } else {
            normalized
        };
        map.entry(key).or_default().push(rec.clone());
    }

    map
}

fn tokenize_title(input: &str) -> Vec<String> {
    input
        .split(|c: char| !c.is_alphanumeric())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_lowercase())
        .collect()
}

fn compare_match(a: &(FileRecord, f64), b: &(FileRecord, f64)) -> Ordering {
    let score_ord = b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal);
    if score_ord != Ordering::Equal {
        return score_ord;
    }

    let ka = format!(
        "{}::{}",
        a.0.source.to_string_lossy(),
        a.0.relative.to_string_lossy()
    );
    let kb = format!(
        "{}::{}",
        b.0.source.to_string_lossy(),
        b.0.relative.to_string_lossy()
    );
    ka.cmp(&kb)
}

/// Produce ranked candidate matches for each DAT ROM entry.
pub fn generate_candidates(
    dat_roms: &[(
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<u64>,
    )],
    records: &[FileRecord],
) -> Vec<Candidate> {
    const MIN_SCORE: f64 = 25.0;
    const SCORE_SIZE_EXACT: f64 = 700.0;
    const SCORE_SIZE_ONLY: f64 = 20.0;
    const SCORE_TITLE_EQUAL: f64 = 300.0;
    const SCORE_TOKEN_SCALE: f64 = 300.0;
    const SCORE_CRC32: f64 = 800.0;
    const SCORE_MD5: f64 = 850.0;
    const SCORE_SHA1: f64 = 900.0;

    // Parallelize across DAT ROM entries; preserve input order by using `par_iter()`
    // on the slice and collecting the results. Each DAT entry's candidate
    // generation remains deterministic: we compute scores and then sort.
    dat_roms
        .par_iter()
        .map(|(name, crc32, md5, sha1, size)| {
            let dat_stem = Path::new(name)
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("");
            let dat_norm = crate::records::normalize_title(dat_stem);
            let dat_tokens = tokenize_title(&dat_norm);

            let mut matches = Vec::new();

            for record in records {
                let mut score = 0.0;
                let mut checksum_matched = false;

                // Only consider a CRC32 match if the DAT also specifies a size
                // and the sizes are equal. This avoids false positives where
                // CRC32 collisions or truncated files could otherwise match.
                if let (Some(dat_crc), Some(dat_size)) = (crc32.as_deref(), size) {
                    if record
                        .checksums
                        .crc32
                        .as_deref()
                        .is_some_and(|c| c.eq_ignore_ascii_case(dat_crc))
                        && record.size == *dat_size
                    {
                        score += SCORE_CRC32;
                        checksum_matched = true;
                    }
                }

                if let Some(dat_md5) = md5 {
                    if record
                        .checksums
                        .md5
                        .as_deref()
                        .is_some_and(|c| c.eq_ignore_ascii_case(dat_md5))
                    {
                        score += SCORE_MD5;
                        checksum_matched = true;
                    }
                }

                if let Some(dat_sha1) = sha1 {
                    if record
                        .checksums
                        .sha1
                        .as_deref()
                        .is_some_and(|c| c.eq_ignore_ascii_case(dat_sha1))
                    {
                        score += SCORE_SHA1;
                        checksum_matched = true;
                    }
                }

                if let Some(dat_size) = size {
                    if record.size == *dat_size {
                        if let Some(name_str) = record.relative.file_name().and_then(|n| n.to_str())
                        {
                            if name_str == name {
                                score += SCORE_SIZE_EXACT;
                            } else {
                                score += SCORE_SIZE_ONLY;
                            }
                        }
                    }
                }

                if let Some(rec_stem) = record.relative.file_stem().and_then(|s| s.to_str()) {
                    let rec_norm = crate::records::normalize_title(rec_stem);
                    if !dat_norm.is_empty() && rec_norm == dat_norm {
                        score += SCORE_TITLE_EQUAL;
                    } else if !dat_tokens.is_empty() {
                        let rec_tokens = tokenize_title(&rec_norm);
                        if !rec_tokens.is_empty() {
                            let dat_set: HashSet<_> = dat_tokens.iter().collect();
                            let rec_set: HashSet<_> = rec_tokens.iter().collect();
                            let inter = dat_set.intersection(&rec_set).count() as f64;
                            let union = dat_set.union(&rec_set).count() as f64;
                            if union > 0.0 {
                                score += (inter / union) * SCORE_TOKEN_SCALE;
                            }
                        }
                    }
                }

                if score >= MIN_SCORE {
                    matches.push((record.clone(), score, checksum_matched));
                }
            }

            let mut checksum_matches: Vec<(FileRecord, f64)> = matches
                .iter()
                .filter(|(_, _, chk)| *chk)
                .map(|(rec, score, _)| (rec.clone(), *score))
                .collect();
            let mut fallback_matches: Vec<(FileRecord, f64)> = matches
                .into_iter()
                .filter(|(_, _, chk)| !*chk)
                .map(|(rec, score, _)| (rec, score))
                .collect();

            checksum_matches.sort_by(compare_match);
            fallback_matches.sort_by(compare_match);

            let ordered = if checksum_matches.is_empty() {
                fallback_matches
            } else {
                checksum_matches
            };

            Candidate {
                name: name.clone(),
                matches: ordered.into_iter().map(|(rec, _)| rec).collect(),
            }
        })
        .collect()
}

/// Build write-ready candidates by combining dat multi-file sets with available FileRecords.
/// - `dat_sets` : map of set name -> Vec<dat rom names belonging to the set>
/// - `dat_roms` : list of all dat roms as tuples (name, crc, md5, sha1, size)
/// - `records` : scanned input file records
pub fn build_write_candidates(
    dat_sets: &std::collections::HashMap<String, Vec<String>>,
    dat_roms: &[(
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<u64>,
    )],
    records: &[FileRecord],
    config: &Config,
) -> Vec<WriteCandidate> {
    let mut out = Vec::new();
    // Track which physical records have already been assigned to a part so we
    // don't reuse the same file for multiple dat entries (unless the user
    // explicitly wants excess/incomplete sets). The key is the record's
    // absolute source path joined with its relative path to be unique for
    // in-archive entries.
    let mut used_records: std::collections::HashSet<String> = std::collections::HashSet::new();

    // Build a lookup map for quick dat rom access by name
    let mut dat_map: std::collections::HashMap<
        String,
        (Option<String>, Option<String>, Option<String>, Option<u64>),
    > = std::collections::HashMap::new();
    for (name, crc, md5, sha1, size) in dat_roms {
        dat_map.insert(
            name.clone(),
            (crc.clone(), md5.clone(), sha1.clone(), *size),
        );
    }

    // For each set, attempt to find matching records for all parts
    for (set_name, parts) in dat_sets {
        let mut matched_files: Vec<FileRecord> = Vec::new();
        let mut files_map: std::collections::HashMap<String, FileRecord> =
            std::collections::HashMap::new();
        let mut all_found = true;
        for part in parts {
            if let Some((crc, md5, sha1, size)) = dat_map.get(part) {
                // Build candidate list prioritizing checksums including CHD-provided sha1/md5
                let mut candidates = generate_candidates(
                    &[(part.clone(), crc.clone(), md5.clone(), sha1.clone(), *size)],
                    records,
                );
                // Run conservative post-processing steps that may correct extensions
                // or inspect archives. These are conditional on config flags so
                // default tests and behavior are unchanged.
                candidates = crate::candidate_extension::postprocess_candidates(candidates, config);
                candidates =
                    crate::candidate_archive_hasher::process_archive_hashes(candidates, config);
                // Try to pick the highest-ranked candidate that hasn't already
                // been used for another part. This prevents a single file from
                // being assigned to multiple parts within the same run.
                let mut chosen_opt: Option<FileRecord> = None;
                if let Some(c) = candidates.into_iter().next() {
                    for cand in c.matches.iter() {
                        let key = format!(
                            "{}::{}",
                            cand.source.to_string_lossy(),
                            cand.relative.to_string_lossy()
                        );
                        if !used_records.contains(&key) {
                            chosen_opt = Some(cand.clone());
                            used_records.insert(key);
                            break;
                        }
                    }
                }
                if let Some(chosen) = chosen_opt {
                    matched_files.push(chosen.clone());
                    files_map.insert(part.clone(), chosen);
                    continue;
                }
                // If no direct candidate found, decide based on config
                if config.allow_incomplete_sets {
                    // skip this part but continue building partial set
                    continue;
                }
                all_found = false;
                break;
            } else {
                all_found = false;
                break;
            }
        }

        if (!matched_files.is_empty() && (all_found || config.allow_incomplete_sets))
            || (matched_files.is_empty() && config.allow_excess_sets)
        {
            let mut wc = WriteCandidate::new(set_name.clone(), matched_files);
            wc.files_map = files_map;
            out.push(wc);
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::FileRecord;
    use std::path::PathBuf;

    fn make_rec(name: &str) -> FileRecord {
        FileRecord {
            source: PathBuf::from(name),
            relative: PathBuf::from(name),
            size: 0,
            checksums: crate::types::ChecksumSet {
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
    fn groups_similar_titles() {
        let records = vec![
            make_rec("Game (USA).bin"),
            make_rec("Game (Europe) (En).bin"),
            make_rec("Game (Japan).bin"),
            make_rec("Other Game.bin"),
        ];

        let grouped = group_candidates(&records);
        assert!(grouped.contains_key("Game"));
        assert!(grouped.contains_key("Other Game"));
        assert_eq!(grouped.get("Game").unwrap().len(), 3);
        assert_eq!(grouped.get("Other Game").unwrap().len(), 1);
    }

    #[test]
    fn generate_candidates_matches_checksums_and_size() {
        let rec1 = FileRecord {
            source: PathBuf::from("a.bin"),
            relative: PathBuf::from("a.bin"),
            size: 100,
            checksums: crate::types::ChecksumSet {
                crc32: Some("ABCD1234".to_string()),
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
        let rec2 = FileRecord {
            source: PathBuf::from("b.bin"),
            relative: PathBuf::from("b.bin"),
            size: 200,
            checksums: crate::types::ChecksumSet {
                crc32: None,
                md5: Some("d41d8cd98f00b204e9800998ecf8427e".to_string()),
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

        let dat_roms = vec![
            (
                "a.bin".to_string(),
                Some("ABCD1234".to_string()),
                None,
                None,
                Some(100u64),
            ),
            (
                "b.bin".to_string(),
                None,
                Some("d41d8cd98f00b204e9800998ecf8427e".to_string()),
                None,
                Some(200u64),
            ),
        ];

        let candidates = generate_candidates(&dat_roms, &[rec1.clone(), rec2.clone()]);
        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0].matches.len(), 1);
        assert_eq!(candidates[1].matches.len(), 1);
    }

    #[test]
    fn generate_candidates_title_fallback() {
        let mut rec1 = make_rec("Game (USA).bin");
        rec1.size = 123;

        let dat_roms = vec![("Game.bin".to_string(), None, None, None, Some(123u64))];

        // even if filename differs (bracketed region), title normalization should match
        let candidates = generate_candidates(&dat_roms, &[rec1.clone()]);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].matches.len(), 1);
        assert_eq!(
            candidates[0].matches[0].relative,
            PathBuf::from("Game (USA).bin")
        );
    }

    #[test]
    fn generate_candidates_scoring_orders_matches() {
        let mut rec_a = make_rec("Super Mario (USA).bin");
        rec_a.size = 100;

        let mut rec_b = make_rec("Super Mario World (Japan).bin");
        rec_b.size = 100;

        let dat_roms = vec![(
            "Super Mario World.bin".to_string(),
            None,
            None,
            None,
            Some(100u64),
        )];

        let candidates = generate_candidates(&dat_roms, &[rec_a.clone(), rec_b.clone()]);
        assert_eq!(candidates.len(), 1);
        // rec_b should be preferred because its title tokens overlap more with dat
        assert_eq!(
            candidates[0].matches[0].relative,
            PathBuf::from("Super Mario World (Japan).bin")
        );
    }

    #[test]
    fn checksum_preferred_over_title() {
        // rec_title better matches title, rec_checksum has checksum match
        let mut rec_title = make_rec("Game Deluxe (Europe).bin");
        rec_title.size = 100;

        let mut rec_checksum = make_rec("Game.bin");
        rec_checksum.size = 100;
        rec_checksum.checksums.crc32 = Some("DEADBEEF".to_string());

        let dat_roms = vec![(
            "Game Deluxe.bin".to_string(),
            Some("DEADBEEF".to_string()),
            None,
            None,
            Some(100u64),
        )];

        let candidates = generate_candidates(&dat_roms, &[rec_title.clone(), rec_checksum.clone()]);
        assert_eq!(candidates.len(), 1);
        // Ensure checksum-matching record is chosen before title-matching record
        assert_eq!(candidates[0].matches[0].relative, PathBuf::from("Game.bin"));
    }

    #[test]
    fn normalize_strips_noise_and_years() {
        let name = "Super Mario (USA) (1995) [Rev 1] (En)";
        let norm = crate::records::normalize_title(name);
        assert_eq!(norm, "Super Mario");
    }

    #[test]
    fn build_write_candidates_combines_multi_file_set() {
        use std::collections::HashMap;

        // create two file records representing two discs
        let mut rec1 = make_rec("game (disc 1).bin");
        rec1.size = 100;
        rec1.checksums.crc32 = Some("AAA".to_string());

        let mut rec2 = make_rec("game (disc 2).bin");
        rec2.size = 200;
        rec2.checksums.crc32 = Some("BBB".to_string());

        // dat roms: two parts
        let dat_roms = vec![
            (
                "game (disc 1).bin".to_string(),
                Some("AAA".to_string()),
                None,
                None,
                Some(100u64),
            ),
            (
                "game (disc 2).bin".to_string(),
                Some("BBB".to_string()),
                None,
                None,
                Some(200u64),
            ),
        ];

        let mut sets: HashMap<String, Vec<String>> = HashMap::new();
        sets.insert(
            "Game (Multi)".to_string(),
            vec![
                "game (disc 1).bin".to_string(),
                "game (disc 2).bin".to_string(),
            ],
        );

        let cfg = crate::config::Config {
            commands: vec![],
            input: vec![],
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
            cache_only: false,
            cache_db: None,
            hash_threads: None,
            scan_threads: None,
            show_match_reasons: false,
        };

        let out = build_write_candidates(&sets, &dat_roms, &[rec1.clone(), rec2.clone()], &cfg);
        assert_eq!(out.len(), 1);
        let wc = &out[0];
        assert_eq!(wc.files.len(), 2);
        assert_eq!(wc.name, "Game (Multi)");
    }

    #[test]
    fn build_write_candidates_prefers_checksum_matches() {
        use std::collections::HashMap;

        let rec_title = FileRecord {
            source: PathBuf::from("Game Deluxe (Europe).bin"),
            relative: PathBuf::from("Game Deluxe (Europe).bin"),
            size: 100,
            checksums: crate::types::ChecksumSet {
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
        let rec_checksum = FileRecord {
            source: PathBuf::from("Game.bin"),
            relative: PathBuf::from("Game.bin"),
            size: 100,
            checksums: crate::types::ChecksumSet {
                crc32: Some("DEADBEEF".to_string()),
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

        let dat_roms = vec![(
            "Game Deluxe (disc 1).bin".to_string(),
            Some("DEADBEEF".to_string()),
            None,
            None,
            Some(100u64),
        )];

        let mut sets: HashMap<String, Vec<String>> = HashMap::new();
        sets.insert(
            "Game Deluxe".to_string(),
            vec!["Game Deluxe (disc 1).bin".to_string()],
        );

        let cfg = crate::config::Config {
            commands: vec![],
            input: vec![],
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
            cache_only: false,
            cache_db: None,
            hash_threads: None,
            scan_threads: None,
            show_match_reasons: false,
        };

        let out = build_write_candidates(
            &sets,
            &dat_roms,
            &[rec_title.clone(), rec_checksum.clone()],
            &cfg,
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].files[0].relative, PathBuf::from("Game.bin"));
    }

    #[test]
    fn build_write_candidates_does_not_reuse_same_file_for_multiple_parts() {
        use std::collections::HashMap;

        // single physical file that could match two parts
        let rec = FileRecord {
            source: PathBuf::from("disc.bin"),
            relative: PathBuf::from("disc.bin"),
            size: 100,
            checksums: crate::types::ChecksumSet {
                crc32: Some("AAA".to_string()),
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

        // dat declares two parts both with same checksum
        let dat_roms = vec![
            (
                "part1.bin".to_string(),
                Some("AAA".to_string()),
                None,
                None,
                Some(100u64),
            ),
            (
                "part2.bin".to_string(),
                Some("AAA".to_string()),
                None,
                None,
                Some(100u64),
            ),
        ];

        let mut sets: HashMap<String, Vec<String>> = HashMap::new();
        sets.insert(
            "Multi".to_string(),
            vec!["part1.bin".to_string(), "part2.bin".to_string()],
        );

        let cfg = crate::config::Config {
            commands: vec![],
            input: vec![],
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
            allow_incomplete_sets: true,
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
            cache_only: false,
            cache_db: None,
            hash_threads: None,
            scan_threads: None,
            show_match_reasons: false,
        };

        let out = build_write_candidates(&sets, &dat_roms, &[rec.clone()], &cfg);
        // we should get a candidate, but it must not include the same file twice
        assert_eq!(out.len(), 1);
        let wc = &out[0];
        // matched files should be <= number of parts and unique
        let uniques: std::collections::HashSet<_> = wc
            .files
            .iter()
            .map(|f| f.relative.to_string_lossy().to_string())
            .collect();
        assert_eq!(uniques.len(), wc.files.len());
    }

    #[test]
    fn generate_candidates_case_insensitive_checksums() {
        let rec_md5 = FileRecord {
            source: PathBuf::from("m.bin"),
            relative: PathBuf::from("m.bin"),
            size: 10,
            checksums: crate::types::ChecksumSet {
                crc32: None,
                md5: Some("D41D8CD98F00B204E9800998ECF8427E".to_string()),
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

        let rec_sha1 = FileRecord {
            source: PathBuf::from("s.bin"),
            relative: PathBuf::from("s.bin"),
            size: 10,
            checksums: crate::types::ChecksumSet {
                crc32: None,
                md5: None,
                sha1: Some("DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF".to_string()),
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
            (
                "m.bin".to_string(),
                None,
                Some("d41d8cd98f00b204e9800998ecf8427e".to_string()),
                None,
                Some(10u64),
            ),
            (
                "s.bin".to_string(),
                None,
                None,
                Some("deadbeefdeadbeefdeadbeefdeadbeefdeadbeef".to_string()),
                Some(10u64),
            ),
        ];

        let candidates = generate_candidates(&dats, &[rec_md5.clone(), rec_sha1.clone()]);
        assert_eq!(candidates.len(), 2);
        // ensure md5 matched despite case differences
        assert_eq!(candidates[0].matches[0].relative, PathBuf::from("m.bin"));
        // ensure sha1 matched despite case differences
        assert_eq!(candidates[1].matches[0].relative, PathBuf::from("s.bin"));
    }

    #[test]
    fn fuzzy_token_overlap_precedence() {
        // two records where one contains more overlapping tokens with dat title
        let rec1 = FileRecord {
            source: PathBuf::from("Alpha Beta Gamma.bin"),
            relative: PathBuf::from("Alpha Beta Gamma.bin"),
            size: 100,
            checksums: crate::types::ChecksumSet {
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
        let rec2 = FileRecord {
            source: PathBuf::from("Alpha Gamma.bin"),
            relative: PathBuf::from("Alpha Gamma.bin"),
            size: 100,
            checksums: crate::types::ChecksumSet {
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

        let dats = vec![(
            "Alpha Beta Gamma Deluxe.bin".to_string(),
            None,
            None,
            None,
            Some(100u64),
        )];

        let candidates = generate_candidates(&dats, &[rec1.clone(), rec2.clone()]);
        assert_eq!(candidates.len(), 1);
        // rec1 should be preferred because it shares more tokens with the dat title
        assert_eq!(
            candidates[0].matches[0].relative,
            PathBuf::from("Alpha Beta Gamma.bin")
        );
    }

    #[test]
    fn deterministic_tie_breaker_for_equal_scores() {
        // Two physical files with identical metadata and checksums but
        // different source paths should sort deterministically.
        let rec_a = FileRecord {
            source: PathBuf::from("/path/A/disc.bin"),
            relative: PathBuf::from("disc.bin"),
            size: 100,
            checksums: crate::types::ChecksumSet {
                crc32: Some("AAA".to_string()),
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
        let rec_b = FileRecord {
            source: PathBuf::from("/path/B/disc.bin"),
            relative: PathBuf::from("disc.bin"),
            size: 100,
            checksums: crate::types::ChecksumSet {
                crc32: Some("AAA".to_string()),
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

        let dats = vec![(
            "disc.bin".to_string(),
            Some("AAA".to_string()),
            None,
            None,
            Some(100u64),
        )];

        let candidates = generate_candidates(&dats, &[rec_a.clone(), rec_b.clone()]);
        assert_eq!(candidates.len(), 1);
        // Expect deterministic ordering: A before B because "/path/A" < "/path/B"
        assert_eq!(
            candidates[0].matches[0].source,
            PathBuf::from("/path/A/disc.bin")
        );
    }

    #[test]
    fn identical_filenames_in_different_sources_are_distinct() {
        // Two files with the same relative path but different sources should
        // be treated as distinct candidates and not collapsed by dedupe.
        let rec1 = FileRecord {
            source: PathBuf::from("C:/store1/game.bin"),
            relative: PathBuf::from("game.bin"),
            size: 50,
            checksums: crate::types::ChecksumSet {
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
        let rec2 = FileRecord {
            source: PathBuf::from("D:/store2/game.bin"),
            relative: PathBuf::from("game.bin"),
            size: 50,
            checksums: crate::types::ChecksumSet {
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

        let dats = vec![("game.bin".to_string(), None, None, None, Some(50u64))];

        let candidates = generate_candidates(&dats, &[rec1.clone(), rec2.clone()]);
        assert_eq!(candidates.len(), 1);
        // Both should be present as potential matches (ordered deterministically)
        assert_eq!(candidates[0].matches.len(), 2);
        let sources: Vec<String> = candidates[0]
            .matches
            .iter()
            .map(|r| r.source.to_string_lossy().to_string())
            .collect();
        assert!(sources.contains(&"C:/store1/game.bin".to_string()));
        assert!(sources.contains(&"D:/store2/game.bin".to_string()));
    }

    #[test]
    fn many_duplicate_checksums_prevent_reuse_across_parts() {
        use std::collections::HashMap;

        // Create many physical files with the same checksum
        let mut records = Vec::new();
        for i in 0..5 {
            records.push(FileRecord {
                source: PathBuf::from(format!("/dup/{}.bin", i)),
                relative: PathBuf::from(format!("dup{}.bin", i)),
                size: 100,
                checksums: crate::types::ChecksumSet {
                    crc32: Some("DUPCHK".to_string()),
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
            });
        }

        // dat declares three parts with same checksum
        let dat_roms = vec![
            (
                "p1.bin".to_string(),
                Some("DUPCHK".to_string()),
                None,
                None,
                Some(100u64),
            ),
            (
                "p2.bin".to_string(),
                Some("DUPCHK".to_string()),
                None,
                None,
                Some(100u64),
            ),
            (
                "p3.bin".to_string(),
                Some("DUPCHK".to_string()),
                None,
                None,
                Some(100u64),
            ),
        ];

        let mut sets: HashMap<String, Vec<String>> = HashMap::new();
        sets.insert(
            "MultiDup".to_string(),
            vec![
                "p1.bin".to_string(),
                "p2.bin".to_string(),
                "p3.bin".to_string(),
            ],
        );

        let cfg = crate::config::Config {
            commands: vec![],
            input: vec![],
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
            allow_incomplete_sets: true,
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
            cache_only: false,
            cache_db: None,
            hash_threads: None,
            scan_threads: None,
            show_match_reasons: false,
        };

        let out = build_write_candidates(&sets, &dat_roms, &records, &cfg);
        // We should get a candidate and it should have up to 3 unique files assigned
        assert_eq!(out.len(), 1);
        let wc = &out[0];
        let uniques: std::collections::HashSet<_> = wc
            .files
            .iter()
            .map(|f| f.source.to_string_lossy().to_string())
            .collect();
        assert_eq!(uniques.len(), wc.files.len());
        assert!(wc.files.len() <= 3);
    }

    #[test]
    fn chd_provided_checksums_precedence() {
        // CHD images sometimes embed checksums (sha1/md5) which should be
        // preferred when matching dat entries. This test ensures entries with
        // CHD-provided sha1 are matched before title-based candidates.
        let rec_title = FileRecord {
            source: PathBuf::from("/store/title_game.bin"),
            relative: PathBuf::from("Game (USA).bin"),
            size: 150,
            checksums: crate::types::ChecksumSet {
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
        let rec_chd = FileRecord {
            source: PathBuf::from("/store/chd_game.chd"),
            relative: PathBuf::from("Game.chd"),
            size: 150,
            checksums: crate::types::ChecksumSet {
                crc32: None,
                md5: Some("cafebabecafebabecafebabecafebab".to_string()),
                sha1: Some("1111111111111111111111111111111111111111".to_string()),
                sha256: None,
            },
            letter_dir: None,
            derived_platform: None,
            derived_genres: Vec::new(),
            derived_region: None,
            derived_languages: Vec::new(),
            scan_info: None,
        };

        let dats = vec![(
            "Game.chd".to_string(),
            None,
            Some("cafebabecafebabecafebabecafebab".to_string()),
            Some("1111111111111111111111111111111111111111".to_string()),
            Some(150u64),
        )];

        let candidates = generate_candidates(&dats, &[rec_title.clone(), rec_chd.clone()]);
        assert_eq!(candidates.len(), 1);
        // CHD record has matching md5+sha1 and should be selected first
        assert_eq!(
            candidates[0].matches[0].source,
            PathBuf::from("/store/chd_game.chd")
        );
    }

    #[test]
    fn node_golden_case_small_example() {
        // A compact golden-case inspired by Node behavior: title match vs checksum
        // prefer checksum even if title token overlap is higher for the other file.
        let rec_title = FileRecord {
            source: PathBuf::from("/node/A/Game Deluxe (Europe).bin"),
            relative: PathBuf::from("Game Deluxe (Europe).bin"),
            size: 200,
            checksums: crate::types::ChecksumSet {
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
        let rec_checksum = FileRecord {
            source: PathBuf::from("/node/B/Game.bin"),
            relative: PathBuf::from("Game.bin"),
            size: 200,
            checksums: crate::types::ChecksumSet {
                crc32: Some("BEEFCAFE".to_string()),
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

        let dats = vec![(
            "Game Deluxe.bin".to_string(),
            Some("BEEFCAFE".to_string()),
            None,
            None,
            Some(200u64),
        )];

        let candidates = generate_candidates(&dats, &[rec_title.clone(), rec_checksum.clone()]);
        assert_eq!(candidates.len(), 1);
        assert_eq!(
            candidates[0].matches[0].source,
            PathBuf::from("/node/B/Game.bin")
        );
    }
}
