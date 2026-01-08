use std::fs::File;
use std::io::Write;
use tempfile::tempdir;

use crc32fast::Hasher as Crc32;

use igir::actions::zip_record;
use igir::config::Config;
use igir::types::{
    ArchiveChecksumMode, Checksum, DirGameSubdirMode, FixExtensionMode, LinkMode, MergeMode,
    MoveDeleteDirsMode, ZipFormat,
};

fn extract_eocd_comment(bytes: &[u8]) -> Option<String> {
    let eocd_sig = b"PK\x05\x06";
    let pos = bytes.windows(4).rposition(|w| w == eocd_sig)?;
    if bytes.len() < pos + 22 {
        return None;
    }
    let comment_len = u16::from_le_bytes([bytes[pos + 20], bytes[pos + 21]]) as usize;
    let comment_start = pos + 22;
    if bytes.len() < comment_start + comment_len {
        return None;
    }
    Some(String::from_utf8_lossy(&bytes[comment_start..comment_start + comment_len]).to_string())
}

fn compute_central_dir_crc(bytes: &[u8]) -> Option<u32> {
    let eocd_sig = b"PK\x05\x06";
    let pos = bytes.windows(4).rposition(|w| w == eocd_sig)?;
    if bytes.len() < pos + 22 {
        return None;
    }
    let cd_size = u32::from_le_bytes([
        bytes[pos + 12],
        bytes[pos + 13],
        bytes[pos + 14],
        bytes[pos + 15],
    ]) as usize;
    let cd_offset = u32::from_le_bytes([
        bytes[pos + 16],
        bytes[pos + 17],
        bytes[pos + 18],
        bytes[pos + 19],
    ]) as usize;
    if bytes.len() < cd_offset + cd_size {
        return None;
    }
    let central_dir = &bytes[cd_offset..cd_offset + cd_size];
    let mut hasher = Crc32::new();
    hasher.update(central_dir);
    Some(hasher.finalize())
}

#[test]
fn torrentzip_multi_deflate_crc_check() {
    let dir = tempdir().unwrap();
    let src1 = dir.path().join("rom1.bin");
    let mut f1 = File::create(&src1).unwrap();
    f1.write_all(b"first file").unwrap();
    let src2 = dir.path().join("rom2.bin");
    let mut f2 = File::create(&src2).unwrap();
    f2.write_all(b"second file").unwrap();

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
        zip_format: ZipFormat::Deflate,
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

    use igir::types::{ChecksumSet, FileRecord};
    let rec1 = FileRecord {
        source: src1.clone(),
        relative: std::path::PathBuf::from("rom1.bin"),
        size: 10,
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
    let rec2 = FileRecord {
        source: src2.clone(),
        relative: std::path::PathBuf::from("rom2.bin"),
        size: 11,
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

    // create zip for first file
    let out1 = zip_record(&rec1, &cfg, None, None).unwrap();
    // create zip for second file (will be a separate zip file) — to exercise multiple entries separately
    let out2 = zip_record(&rec2, &cfg, None, None).unwrap();

    let bytes1 = std::fs::read(out1).unwrap();
    let bytes2 = std::fs::read(out2).unwrap();

    let comment1 = extract_eocd_comment(&bytes1).expect("extract comment 1");
    let computed1 = compute_central_dir_crc(&bytes1).expect("compute cd crc 1");
    let parsed1 = if comment1.starts_with("TORRENTZIPPED-") {
        u32::from_str_radix(&comment1["TORRENTZIPPED-".len()..], 16).unwrap()
    } else {
        u32::from_str_radix(&comment1["RVZSTD-".len()..], 16).unwrap()
    };
    assert_eq!(parsed1, computed1);

    let comment2 = extract_eocd_comment(&bytes2).expect("extract comment 2");
    let computed2 = compute_central_dir_crc(&bytes2).expect("compute cd crc 2");
    let parsed2 = if comment2.starts_with("TORRENTZIPPED-") {
        u32::from_str_radix(&comment2["TORRENTZIPPED-".len()..], 16).unwrap()
    } else {
        u32::from_str_radix(&comment2["RVZSTD-".len()..], 16).unwrap()
    };
    assert_eq!(parsed2, computed2);
}
