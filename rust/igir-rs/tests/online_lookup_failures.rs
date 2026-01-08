use httpmock::Method::{GET, POST};
use httpmock::MockServer;

use reqwest::blocking::Client;
use std::time::Duration;

use igir::config::Config;
use igir::dat::online_lookup_with_client;
use igir::types::{ChecksumSet, FileRecord};

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
fn hasheous_404_and_missing_igdb_credentials_returns_no_results() {
    let server = MockServer::start();

    // hasheous returns 404
    let _hasheous_mock = server.mock(|when, then| {
        when.method(GET).path("/hash/notfound");
        then.status(404);
    });

    // point overrides
    igir::dat::test_hooks::set_hasheous_base_override(&server.url(""));
    // leave IGDB creds missing to simulate missing auth

    let client = Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();

    let record = record_with_checksums(Some("notfound"), None);
    let cfg = Config {
        dat: Vec::new(),
        enable_hasheous: true,
        igdb_client_id: None,
        igdb_token: None,
        ..Default::default()
    };

    let results = online_lookup_with_client(&[record], &cfg, &client, None).expect("lookup failed");
    assert_eq!(results.len(), 0);
}

#[test]
fn igdb_timeout_is_handled_gracefully() {
    let server = MockServer::start();

    // IGDB endpoint delays response
    let _igdb_mock = server.mock(|when, then| {
        when.method(POST)
            .path("/games")
            .header("Accept", "application/json")
            .header("Content-Type", "text/plain");
        then.status(200)
            .header("content-type", "application/json")
            .delay(Duration::from_millis(200))
            .body(r#"[{"name":"Slow Game"}]"#);
    });

    // set IGDB override to mock server
    igir::dat::test_hooks::set_igdb_base_override(&server.url(""));

    // client with short timeout
    let client = Client::builder()
        .timeout(Duration::from_millis(50))
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();

    let record = record_with_checksums(None, None);
    let cfg = Config {
        dat: Vec::new(),
        enable_hasheous: false,
        igdb_client_id: Some("id".to_string()),
        igdb_token: Some("tok".to_string()),
        ..Default::default()
    };

    // The lookup should not panic; either it returns zero or empty igdb result depending on timeout handling.
    let results = online_lookup_with_client(&[record], &cfg, &client, None).expect("lookup failed");
    // If timeout occurred, results may be empty; assert no panic and results is Vec
    assert!(results.is_empty() || results[0].igdb.is_some());
}
