use httpmock::Method::GET;
use httpmock::MockServer;

use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

use igir::cache::Cache;
use igir::config::Config;
use igir::types::Action;

/// Integration test: when cache contains Hasheous JSON and `cache_only` is enabled,
/// perform_actions should not perform network requests to Hasheous (mock server).
#[test]
fn cache_only_uses_cached_hasheous_and_skips_network() -> anyhow::Result<()> {
    // start mock server which would receive requests if network lookups occurred
    let server = MockServer::start();

    // Prepare temporary workspace with one input file
    let tmp = tempfile::tempdir()?;
    let input_dir = tmp.path().join("input");
    std::fs::create_dir_all(&input_dir)?;
    let file_path = input_dir.join("example.sfc");
    let mut f = File::create(&file_path)?;
    f.write_all(b"dummy rom content for cache-only test")?;
    f.flush()?;

    // compute the content key (sha256) for the file using the library helper
    let checks = igir::checksum::compute_all_checksums(&file_path)?;
    let key = checks.sha256.clone().expect("sha256 present");

    // create mocks for the various hash algorithms that would be attempted
    let sha1_mock = server.mock(|when, then| {
        when.method(GET).path(format!(
            "/api/v1/Lookup/ByHash/sha1/{}",
            checks.sha1.as_ref().unwrap()
        ));
        then.status(404);
    });
    let md5_mock = server.mock(|when, then| {
        when.method(GET).path(format!(
            "/api/v1/Lookup/ByHash/md5/{}",
            checks.md5.as_ref().unwrap()
        ));
        then.status(404);
    });
    let sha256_mock = server.mock(|when, then| {
        when.method(GET).path(format!(
            "/api/v1/Lookup/ByHash/sha256/{}",
            checks.sha256.as_ref().unwrap()
        ));
        then.status(404);
    });
    let crc_mock = server.mock(|when, then| {
        when.method(GET).path(format!(
            "/api/v1/Lookup/ByHash/crc32/{}",
            checks.crc32.as_ref().unwrap()
        ));
        then.status(404);
    });

    // open cache DB and seed hasheous JSON by content key
    let db_path = tmp.path().join("cache.sqlite");
    let cache = Cache::open(Some(&db_path), None)?;
    let hasheous_json = serde_json::json!({ "platform": { "name": "Super Nintendo Entertainment System" }, "title": "Example Game" });
    cache.set_hasheous_raw_by_key(&key, &file_path, &hasheous_json)?;

    // override hasheous base to point to the mock server (should not be used)
    igir::dat::test_hooks::set_hasheous_base_override(&server.url(""));

    // Build config that points at our input and enables hasheous but uses cache-only
    let mut cfg = Config::default();
    cfg.commands = vec![Action::Test];
    cfg.input = vec![input_dir.clone()];
    cfg.enable_hasheous = true;
    cfg.cache_only = true;
    cfg.cache_db = Some(db_path.clone());

    // Run perform_actions; this should observe the cache hit and not call the mock server
    let plan = igir::actions::perform_actions(&cfg)?;

    // ensure none of the hasheous mocks were called
    assert_eq!(
        sha1_mock.calls(),
        0,
        "sha1 lookup called despite cache-only"
    );
    assert_eq!(md5_mock.calls(), 0, "md5 lookup called despite cache-only");
    assert_eq!(
        sha256_mock.calls(),
        0,
        "sha256 lookup called despite cache-only"
    );
    assert_eq!(
        crc_mock.calls(),
        0,
        "crc32 lookup called despite cache-only"
    );

    // cleanup override
    igir::dat::test_hooks::clear_hasheous_base_override();

    // plan should have processed one file
    assert_eq!(plan.files_processed, 1);

    Ok(())
}
