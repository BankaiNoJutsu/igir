use std::fs::File;
use std::io::Write;
use tempfile::tempdir;

use igir::actions::zip_record;
use igir::config::Config;
use igir::types::{
    ArchiveChecksumMode, Checksum, DirGameSubdirMode, FixExtensionMode, LinkMode, MergeMode,
    MoveDeleteDirsMode, ZipFormat,
};

#[test]
fn torrentzip_cp437_nonascii_and_fallback() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("rom.bin");
    let mut f = File::create(&src).unwrap();
    f.write_all(b"hello").unwrap();

    let cfg = Config {
        commands: vec![],
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
        output: Some(dir.path().to_path_buf()),
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
    };

    use igir::types::ChecksumSet;
    use igir::types::FileRecord;

    // A filename that is CP437-encodable: e.g., 'Ç' (U+00C7) is in CP437 at 0x80
    let rec1 = FileRecord {
        source: src.clone(),
        relative: std::path::PathBuf::from("rom_Ç.bin"),
        size: 5,
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
    let out1 = zip_record(&rec1, &cfg, None, None).unwrap();
    let data1 = std::fs::read(out1).unwrap();
    let s1 = String::from_utf8_lossy(&data1);
    assert!(s1.contains("TORRENTZIPPED-"));

    // A filename that is not CP437-encodable: use a character outside the CP437 table, e.g., Euro sign U+20AC is not in CP437
    let rec2 = FileRecord {
        source: src.clone(),
        relative: std::path::PathBuf::from("rom_€_utf8.bin"),
        size: 5,
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
    let out2 = zip_record(&rec2, &cfg, None, None).unwrap();
    let data2 = std::fs::read(out2).unwrap();
    let s2 = String::from_utf8_lossy(&data2);
    // EOCD comment should still be present (zip crate fallback path)
    assert!(s2.contains("TORRENTZIPPED-") || s2.contains("RVZSTD-"));
}
