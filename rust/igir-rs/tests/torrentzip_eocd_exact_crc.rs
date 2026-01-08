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
fn torrentzip_eocd_comment_exact_crc() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("rom.bin");
    let mut f = File::create(&src).unwrap();
    f.write_all(b"hello").unwrap();

    // Config for manual path (Torrentzip)
    let cfg_manual = Config {
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

    // CP437-encodable filename -> manual writer
    let rec_manual = FileRecord {
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
    let out_manual = zip_record(&rec_manual, &cfg_manual, None, None).unwrap();
    let bytes_manual = std::fs::read(out_manual).unwrap();
    let comment_manual = extract_eocd_comment(&bytes_manual).expect("extract comment");
    assert!(comment_manual.starts_with("TORRENTZIPPED-"));
    let cdfh_hex_manual = &comment_manual["TORRENTZIPPED-".len()..];
    let parsed_manual = u32::from_str_radix(cdfh_hex_manual, 16).expect("parse hex");
    let computed_manual = compute_central_dir_crc(&bytes_manual).expect("compute cd crc");
    assert_eq!(parsed_manual, computed_manual);

    // Non-CP437 filename -> fallback path via zip crate
    let cfg_fb = Config {
        zip_format: ZipFormat::Torrentzip,
        ..cfg_manual
    };
    let rec_fb = FileRecord {
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
    let out_fb = zip_record(&rec_fb, &cfg_fb, None, None).unwrap();
    let bytes_fb = std::fs::read(out_fb).unwrap();
    let comment_fb = extract_eocd_comment(&bytes_fb).expect("extract comment fb");
    // fallback also writes TORRENTZIPPED-<hex>
    assert!(comment_fb.starts_with("TORRENTZIPPED-"));
    let cdfh_hex_fb = &comment_fb["TORRENTZIPPED-".len()..];
    let parsed_fb = u32::from_str_radix(cdfh_hex_fb, 16).expect("parse hex fb");
    let computed_fb = compute_central_dir_crc(&bytes_fb).expect("compute cd crc fb");
    assert_eq!(parsed_fb, computed_fb);
}
