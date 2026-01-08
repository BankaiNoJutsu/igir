use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use tempfile::tempdir;

use igir::config::Config;
use igir::types::{
    ArchiveChecksumMode, Checksum, DirGameSubdirMode, FixExtensionMode, LinkMode, MergeMode,
    MoveDeleteDirsMode, ZipFormat,
};

fn config_with_dats(dat_paths: Vec<PathBuf>, output: Option<PathBuf>) -> Config {
    Config {
        commands: vec![],
        input: vec![],
        input_exclude: vec![],
        input_checksum_quick: false,
        input_checksum_min: Checksum::Crc32,
        input_checksum_max: None,
        input_checksum_archives: ArchiveChecksumMode::Auto,
        dat: dat_paths,
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
        patch: vec![],
        patch_exclude: vec![],
        output,
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
    }
}

// Adding show_match_reasons to Config

#[test]
fn dat_load_variants() {
    let dir = tempdir().unwrap();
    // empty <rom ... /> style
    let dat1 = dir.path().join("d1.xml");
    let mut f1 = File::create(&dat1).unwrap();
    f1.write_all(br#"<?xml version="1.0"?><datafile><game name="G1"><description>Game One</description><rom name="rom1.bin" size="5" crc="ABCDEF12"/></game></datafile>"#).unwrap();

    // <rom ...> style with uppercase attrs
    let dat2 = dir.path().join("d2.xml");
    let mut f2 = File::create(&dat2).unwrap();
    f2.write_all(br#"<?xml version="1.0"?><datafile><machine name="G2"><rom NAME="rom2.bin" SIZE="6" CRC="1234ABCD"/></machine></datafile>"#).unwrap();

    // start/end with text child
    let dat3 = dir.path().join("d3.xml");
    let mut f3 = File::create(&dat3).unwrap();
    f3.write_all(br#"<?xml version="1.0"?><datafile><game name="G3"><description>Game Three</description><rom name="rom3.bin">ignored</rom></game></datafile>"#).unwrap();

    let cfg = config_with_dats(
        vec![dat1.clone(), dat2.clone(), dat3.clone()],
        Some(dir.path().to_path_buf()),
    );

    let roms = igir::dat::load_dat_roms(&cfg, None).unwrap();
    // Expect at least 3 ROM entries (one per dat)
    assert!(roms.len() >= 3);
    // Basic assertions that names parsed
    assert!(roms.iter().any(|r| r.name == "rom1.bin"));
    assert!(roms.iter().any(|r| r.name == "rom2.bin"));
    assert!(roms.iter().any(|r| r.name == "rom3.bin"));
}

#[test]
fn missing_explicit_path_errors() {
    let dir = tempdir().unwrap();
    let missing = dir.path().join("nope").join("missing.dat");
    let cfg = config_with_dats(vec![missing.clone()], Some(dir.path().to_path_buf()));
    let err = igir::dat::load_dat_roms(&cfg, None).unwrap_err();
    assert!(
        err.to_string().contains("DAT path(s) not found"),
        "unexpected error: {err:?}"
    );
    let missing_str = missing.to_string_lossy().to_string();
    assert!(err.to_string().contains(&missing_str));
}
