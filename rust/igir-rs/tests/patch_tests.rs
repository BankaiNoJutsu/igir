use std::fs::File;
use std::io::Write;
use tempfile::NamedTempFile;

use igir::config::Config;
use igir::patch::{guess_patch_type, load_patches};

#[test]
fn discovers_patch_files_by_glob_and_detects_type() {
    let mut f1 = NamedTempFile::new().unwrap();
    writeln!(f1, "dummy ips content").unwrap();
    let p1 = f1.path().with_extension("ips");
    std::fs::rename(f1.path(), &p1).unwrap();

    let mut f2 = NamedTempFile::new().unwrap();
    writeln!(f2, "dummy bps content").unwrap();
    let p2 = f2.path().with_extension("bps");
    std::fs::rename(f2.path(), &p2).unwrap();

    let cfg = Config {
        commands: vec![],
        input: vec![],
        input_exclude: vec![],
        input_checksum_quick: false,
        input_checksum_min: igir::types::Checksum::Crc32,
        input_checksum_max: None,
        input_checksum_archives: igir::types::ArchiveChecksumMode::Auto,
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
        igdb_token: None,
        igdb_client_secret: None,
        igdb_token_expires_at: None,
        igdb_mode: igir::types::IgdbLookupMode::BestEffort,
        patch: vec![p1.clone(), p2.clone()],
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
        dir_game_subdir: igir::types::DirGameSubdirMode::Multiple,
        fix_extension: igir::types::FixExtensionMode::Auto,
        overwrite: false,
        overwrite_invalid: false,
        move_delete_dirs: igir::types::MoveDeleteDirsMode::Auto,
        clean_exclude: vec![],
        clean_backup: None,
        clean_dry_run: false,
        zip_format: igir::types::ZipFormat::Torrentzip,
        zip_exclude: None,
        zip_dat_name: false,
        link_mode: igir::types::LinkMode::Hardlink,
        symlink_relative: false,
        header: None,
        remove_headers: None,
        trimmed_glob: None,
        trim_scan_archives: false,
        merge_roms: igir::types::MergeMode::Fullnonmerged,
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

    let patches = load_patches(&cfg).unwrap();
    assert_eq!(patches.len(), 2);

    let types: Vec<Option<&str>> = patches.iter().map(|p| guess_patch_type(p)).collect();
    assert!(types.contains(&Some("ips")));
    assert!(types.contains(&Some("bps")));
}
