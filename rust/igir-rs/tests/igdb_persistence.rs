use std::env;
use std::ffi::OsStr;
use std::fs;
use std::path::PathBuf;

use httpmock::Method::POST;
use httpmock::MockServer;
use igir::cli::Cli;
use igir::config::Config;
use igir::types::{
    Action, ArchiveChecksumMode, Checksum, DirGameSubdirMode, FixExtensionMode, LinkMode,
    MergeMode, MoveDeleteDirsMode, ZipFormat,
};
use once_cell::sync::Lazy;
use std::sync::Mutex;

static TEST_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

fn set_env_var<K: AsRef<OsStr>, V: AsRef<OsStr>>(key: K, value: V) {
    unsafe { env::set_var(key, value) }
}

fn restore_env_var(key: &str, previous: Option<String>) {
    unsafe {
        match previous {
            Some(val) => env::set_var(key, val),
            None => env::remove_var(key),
        }
    }
}

#[test]
fn saves_and_loads_igdb_credentials() {
    let _guard = TEST_LOCK.lock().unwrap();
    // Create a temp dir to act as APPDATA / XDG_CONFIG_HOME
    let tmp = tempfile::tempdir().expect("tempdir");
    let tmp_path = tmp.path().to_path_buf();

    // Set IGIR_CONFIG_DIR so persisted_config_path will return a predictable location
    let prev_cfg = env::var("IGIR_CONFIG_DIR").ok();
    set_env_var("IGIR_CONFIG_DIR", &tmp_path);

    // Build CLI with creds and save flag
    let cli_save = Cli {
        commands: vec![Action::Test],
        input: vec![PathBuf::from("/tmp/file.bin")],
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
        enable_hasheous: false,
        igdb_client_id: Some("TEST_CLIENT_ID".to_string()),
        igdb_client_secret: Some("TEST_SECRET".to_string()),
        igdb_token: Some("TEST_TOKEN".to_string()),
        igdb_mode: igir::types::IgdbLookupMode::BestEffort,
        cache_only: false,
        cache_db: None,
        hash_threads: None,
        scan_threads: None,
        show_match_reasons: false,
        save_igdb_creds: true,
        patch: vec![],
        patch_exclude: vec![],
        output: Some(PathBuf::from("out")),
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
        print_plan: false,
    };

    // Create and persist config via TryFrom
    let cfg = Config::try_from(cli_save).expect("should create config");
    assert_eq!(cfg.igdb_client_id.as_deref(), Some("TEST_CLIENT_ID"));
    assert_eq!(cfg.igdb_token.as_deref(), Some("TEST_TOKEN"));

    // Ensure file exists at IGIR_CONFIG_DIR/config.json
    let cfg_file = tmp_path.join("config.json");
    assert!(cfg_file.exists(), "config file should be written");

    // Read contents and check
    let s = fs::read_to_string(&cfg_file).expect("read saved config");
    let v: serde_json::Value = serde_json::from_str(&s).expect("parse saved json");
    assert_eq!(
        v.get("igdb_client_id").and_then(|x| x.as_str()),
        Some("TEST_CLIENT_ID")
    );
    assert_eq!(
        v.get("igdb_token").and_then(|x| x.as_str()),
        Some("TEST_TOKEN")
    );

    // Now create a new Cli without creds and ensure load picks them up (refreshing the stale token)
    let server = MockServer::start();
    let token_mock = server.mock(|when, then| {
        when.method(POST).path("/token");
        then.status(200)
            .header("content-type", "application/json")
            .body(r#"{"access_token":"REFRESHED_LOAD","expires_in":3600}"#);
    });
    let prev_token_base = env::var("IGDB_TOKEN_BASE").ok();
    set_env_var("IGDB_TOKEN_BASE", server.url(""));

    let cli_load = Cli {
        commands: vec![Action::Test],
        input: vec![PathBuf::from("/tmp/file.bin")],
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
        enable_hasheous: false,
        igdb_client_id: None,
        igdb_client_secret: None,
        igdb_token: None,
        igdb_mode: igir::types::IgdbLookupMode::BestEffort,
        cache_only: false,
        cache_db: None,
        hash_threads: None,
        scan_threads: None,
        show_match_reasons: false,
        save_igdb_creds: false,
        patch: vec![],
        patch_exclude: vec![],
        output: Some(PathBuf::from("out")),
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
        print_plan: false,
    };

    let cfg2 = Config::try_from(cli_load).expect("should create config with loaded creds");
    assert_eq!(cfg2.igdb_client_id.as_deref(), Some("TEST_CLIENT_ID"));
    assert_eq!(cfg2.igdb_token.as_deref(), Some("REFRESHED_LOAD"));
    assert!(cfg2.igdb_token_expires_at.is_some());
    token_mock.assert();

    restore_env_var("IGDB_TOKEN_BASE", prev_token_base);
    restore_env_var("IGIR_CONFIG_DIR", prev_cfg);
}

#[test]
fn auto_fetches_token_when_secret_available() {
    let _guard = TEST_LOCK.lock().unwrap();
    let tmp = tempfile::tempdir().expect("tempdir");
    let tmp_path = tmp.path().to_path_buf();
    let prev_cfg = env::var("IGIR_CONFIG_DIR").ok();
    set_env_var("IGIR_CONFIG_DIR", &tmp_path);

    let server = MockServer::start();
    let token_mock = server.mock(|when, then| {
        when.method(POST).path("/token");
        then.status(200)
            .header("content-type", "application/json")
            .body(r#"{"access_token":"AUTO_TOKEN","expires_in":3600}"#);
    });
    let prev_token_base = env::var("IGDB_TOKEN_BASE").ok();
    set_env_var("IGDB_TOKEN_BASE", server.url(""));

    let cli = Cli {
        commands: vec![Action::Test],
        input: vec![PathBuf::from("/tmp/file.bin")],
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
        enable_hasheous: false,
        igdb_client_id: Some("AUTO_ID".to_string()),
        igdb_client_secret: Some("AUTO_SECRET".to_string()),
        igdb_token: None,
        igdb_mode: igir::types::IgdbLookupMode::BestEffort,
        cache_db: None,
        hash_threads: None,
        scan_threads: None,
        show_match_reasons: false,
        cache_only: false,
        save_igdb_creds: true,
        patch: vec![],
        patch_exclude: vec![],
        output: Some(PathBuf::from("out")),
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
        print_plan: false,
    };

    let cfg = Config::try_from(cli).expect("config should be created");
    assert_eq!(cfg.igdb_token.as_deref(), Some("AUTO_TOKEN"));
    assert!(cfg.igdb_token_expires_at.is_some());
    token_mock.assert();

    let saved = tmp_path.join("config.json");
    assert!(saved.exists());
    let contents = fs::read_to_string(&saved).expect("read persisted config");
    let v: serde_json::Value = serde_json::from_str(&contents).expect("parse persisted json");
    assert_eq!(
        v.get("igdb_client_id").and_then(|x| x.as_str()),
        Some("AUTO_ID")
    );
    assert_eq!(
        v.get("igdb_client_secret").and_then(|x| x.as_str()),
        Some("AUTO_SECRET")
    );
    assert_eq!(
        v.get("igdb_token").and_then(|x| x.as_str()),
        Some("AUTO_TOKEN")
    );
    assert!(v.get("igdb_token_expires_at").is_some());

    restore_env_var("IGDB_TOKEN_BASE", prev_token_base);
    restore_env_var("IGIR_CONFIG_DIR", prev_cfg);
}

#[test]
fn refreshes_stale_token_without_expiry() {
    let _guard = TEST_LOCK.lock().unwrap();
    let tmp = tempfile::tempdir().expect("tempdir");
    let tmp_path = tmp.path().to_path_buf();
    let prev_cfg = env::var("IGIR_CONFIG_DIR").ok();
    set_env_var("IGIR_CONFIG_DIR", &tmp_path);

    let server = MockServer::start();
    let token_mock = server.mock(|when, then| {
        when.method(POST).path("/token");
        then.status(200)
            .header("content-type", "application/json")
            .body(r#"{"access_token":"REFRESHED","expires_in":7200}"#);
    });
    let prev_token_base = env::var("IGDB_TOKEN_BASE").ok();
    set_env_var("IGDB_TOKEN_BASE", server.url(""));

    // Seed persisted config with a stale token lacking expiry metadata
    let persisted = serde_json::json!({
        "igdb_client_id": "PERSISTED_ID",
        "igdb_client_secret": "PERSISTED_SECRET",
        "igdb_token": "STALE",
    });
    let cfg_path = tmp_path.join("config.json");
    let serialized = serde_json::to_string_pretty(&persisted).expect("serialize seed config");
    fs::write(&cfg_path, serialized).expect("write seed config");

    let cli = Cli {
        commands: vec![Action::Test],
        input: vec![PathBuf::from("/tmp/file.bin")],
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
        enable_hasheous: false,
        igdb_client_id: None,
        igdb_client_secret: None,
        igdb_token: None,
        igdb_mode: igir::types::IgdbLookupMode::BestEffort,
        cache_db: None,
        hash_threads: None,
        scan_threads: None,
        show_match_reasons: false,
        cache_only: false,
        save_igdb_creds: false,
        patch: vec![],
        patch_exclude: vec![],
        output: Some(PathBuf::from("out")),
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
        print_plan: false,
    };

    let cfg = Config::try_from(cli).expect("config should load");
    assert_eq!(cfg.igdb_client_id.as_deref(), Some("PERSISTED_ID"));
    assert_eq!(cfg.igdb_token.as_deref(), Some("REFRESHED"));
    assert!(cfg.igdb_token_expires_at.is_some());
    token_mock.assert();

    restore_env_var("IGDB_TOKEN_BASE", prev_token_base);
    restore_env_var("IGIR_CONFIG_DIR", prev_cfg);
}
