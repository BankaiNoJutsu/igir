use httpmock::Method::GET;
use httpmock::Method::POST;
use httpmock::MockServer;

use reqwest::blocking::Client;

use igir::config::Config;
use igir::dat::online_lookup_with_client;
use igir::types::{ChecksumSet, FileRecord};

use once_cell::sync::Lazy;
use std::sync::Mutex;

static TEST_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

fn record_with_checksums(sha1: Option<&str>, md5: Option<&str>) -> FileRecord {
    FileRecord {
        source: std::path::PathBuf::from("input/r.bin"),
        relative: std::path::PathBuf::from("r.bin"),
        size: 0,
        checksums: ChecksumSet {
            crc32: None,
            md5: md5.map(|s| s.to_string()),
            sha1: sha1.map(|s| s.to_string()),
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
fn hasheous_match_skips_igdb_lookup() {
    let _guard = TEST_LOCK.lock().unwrap();
    // start mock server
    let server = MockServer::start();

    // mock hasheous GET (new Lookup/ByHash path) - returns a hit
    let hasheous_mock = server.mock(|when, then| {
        when.method(GET).path("/api/v1/Lookup/ByHash/sha1/sha1val");
        then.status(200)
            .header("content-type", "application/json")
            .body(r#"{"result":"ok"}"#);
    });

    // mock IGDB POST - if called, it will satisfy the mock, but we won't assert it.
    let igdb_mock = server.mock(|when, then| {
        when.method(POST)
            .path("/games")
            .header("Accept", "application/json")
            .header("Content-Type", "text/plain");
        then.status(200)
            .header("content-type", "application/json")
            .body(r#"[{"name":"Test Game"}]"#);
    });

    // point overrides to the mock server using internal test hooks
    igir::dat::test_hooks::set_hasheous_base_override(&server.url(""));
    igir::dat::test_hooks::set_igdb_base_override(&server.url(""));

    // Build a client that will talk to the mock server
    let client = Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();

    // prepare an input record that has sha1
    let mut record = record_with_checksums(Some("sha1val"), None);
    record.derived_platform = Some("gba".to_string());
    let cfg = Config {
        dat: Vec::new(),
        enable_hasheous: true,
        igdb_client_id: Some("id".to_string()),
        igdb_token: Some("tok".to_string()),
        ..Default::default()
    };

    let results = online_lookup_with_client(&[record], &cfg, &client, None).expect("lookup failed");

    assert_eq!(results.len(), 1);
    // ensure the hasheous mock was called and IGDB may be skipped
    hasheous_mock.assert();
    assert!(results[0].hasheous.is_some());
    assert!(results[0].igdb.is_none());
    // optional: ensure IGDB was not called
    assert_eq!(igdb_mock.calls(), 0);
    // clear overrides so other tests can set their own mock servers safely
    igir::dat::test_hooks::clear_hasheous_base_override();
    igir::dat::test_hooks::clear_igdb_base_override();
}

#[test]
fn md5_is_preferred_over_sha1() {
    let _guard = TEST_LOCK.lock().unwrap();
    let server = MockServer::start();

    let md5_mock = server.mock(|when, then| {
        when.method(GET).path("/api/v1/Lookup/ByHash/md5/md5val");
        then.status(200)
            .header("content-type", "application/json")
            .body(r#"{"result":"md5"}"#);
    });

    let sha1_mock = server.mock(|when, then| {
        when.method(GET).path("/api/v1/Lookup/ByHash/sha1/sha1val");
        then.status(500);
    });

    igir::dat::test_hooks::set_hasheous_base_override(&server.url(""));

    let client = Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();

    let mut record = record_with_checksums(Some("sha1val"), Some("md5val"));
    record.derived_platform = Some("gba".to_string());
    let cfg = Config {
        dat: Vec::new(),
        enable_hasheous: true,
        igdb_client_id: None,
        igdb_token: None,
        ..Default::default()
    };

    let results = online_lookup_with_client(&[record], &cfg, &client, None).expect("lookup failed");
    assert_eq!(results.len(), 1);
    md5_mock.assert();
    assert_eq!(sha1_mock.calls(), 0);

    igir::dat::test_hooks::clear_hasheous_base_override();
}

#[test]
fn igdb_used_when_no_checksum_available() {
    let _guard = TEST_LOCK.lock().unwrap();
    let server = MockServer::start();

    // hasheous won't be called because there are no checksums; but set a mock that would fail if called
    let _hasheous_mock = server.mock(|when, then| {
        when.method(GET)
            .path("/api/v1/Lookup/ByHash/md5/doesnotmatter");
        then.status(404);
    });

    // mock IGDB POST
    let igdb_mock = server.mock(|when, then| {
        when.method(POST)
            .path("/games")
            .header("Accept", "application/json")
            .header("Content-Type", "text/plain");
        then.status(200)
            .header("content-type", "application/json")
            .body(r#"[{"name":"Test Game"}]"#);
    });

    igir::dat::test_hooks::set_hasheous_base_override(&server.url(""));
    igir::dat::test_hooks::set_igdb_base_override(&server.url(""));

    let client = Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();

    // FileRecord without any checksums -> should trigger IGDB
    let record = record_with_checksums(None, None);
    let cfg = Config {
        dat: Vec::new(),
        enable_hasheous: true,
        igdb_client_id: Some("id".to_string()),
        igdb_token: Some("tok".to_string()),
        ..Default::default()
    };

    let results = online_lookup_with_client(&[record], &cfg, &client, None).expect("lookup failed");
    assert_eq!(results.len(), 1);
    igdb_mock.assert();
    assert!(results[0].igdb.is_some());
    igir::dat::test_hooks::clear_hasheous_base_override();
    igir::dat::test_hooks::clear_igdb_base_override();
}
