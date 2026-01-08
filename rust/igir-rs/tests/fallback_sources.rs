use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;

use anyhow::Result;
use httpmock::Method::POST;
use httpmock::MockServer;
use once_cell::sync::Lazy;
use std::sync::Mutex;
use tempfile::tempdir;

use igir::actions::perform_actions;
use igir::config::Config;
use igir::dat::test_hooks;
use igir::types::{Action, IgdbLookupMode};

static TEST_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

fn write_input_file(dir: &PathBuf, name: &str, contents: &[u8]) -> Result<PathBuf> {
    fs::create_dir_all(dir)?;
    let path = dir.join(name);
    let mut file = File::create(&path)?;
    file.write_all(contents)?;
    file.flush()?;
    Ok(path)
}

#[test]
fn igdb_fallback_populates_cache_and_serves_cache_only_runs() -> Result<()> {
    let _guard = TEST_LOCK.lock().unwrap();

    let tmp = tempdir()?;
    let input_dir = tmp.path().join("input");
    let first_output = tmp.path().join("out_net");
    let second_output = tmp.path().join("out_cache");
    let cache_db = tmp.path().join("igir_cache.sqlite");

    let file_name = "Mystery Quest.rom";
    write_input_file(&input_dir, file_name, b"mystery rom bytes")?;

    let server = MockServer::start();
    let igdb_mock = server.mock(|when, then| {
        when.method(POST)
            .path("/games")
            .header("Accept", "application/json")
            .header("Content-Type", "text/plain");
        then.status(200)
            .header("content-type", "application/json")
            .body(r#"[{"platforms":[{"name":"Super Nintendo Entertainment System"}]}]"#);
    });

    test_hooks::set_igdb_base_override(&server.url(""));

    let mut cfg_first = Config::default();
    cfg_first.commands = vec![Action::Copy];
    cfg_first.input = vec![input_dir.clone()];
    cfg_first.output = Some(first_output.join("{platform}"));
    cfg_first.enable_hasheous = false;
    cfg_first.igdb_client_id = Some("client".to_string());
    cfg_first.igdb_token = Some("token".to_string());
    cfg_first.cache_db = Some(cache_db.clone());
    cfg_first.overwrite = true;

    perform_actions(&cfg_first)?;
    assert_eq!(
        igdb_mock.calls(),
        1,
        "expected one IGDB request on first run"
    );
    assert!(first_output.join("snes").join(file_name).exists());

    let mut cfg_second = cfg_first.clone();
    cfg_second.output = Some(second_output.join("{platform}"));
    cfg_second.cache_only = true;
    cfg_second.enable_hasheous = true; // ensure cache-only path still allows IGDB fallback

    perform_actions(&cfg_second)?;
    assert_eq!(
        igdb_mock.calls(),
        1,
        "second run should reuse cache without new IGDB calls"
    );
    assert!(second_output.join("snes").join(file_name).exists());

    test_hooks::clear_igdb_base_override();
    Ok(())
}

#[test]
fn igdb_mode_always_enriches_dat_matches() -> Result<()> {
    let _guard = TEST_LOCK.lock().unwrap();

    let tmp = tempdir()?;
    let input_dir = tmp.path().join("input");
    let output_dir = tmp.path().join("out");
    let cache_db = tmp.path().join("igir_cache.sqlite");
    let dat_path = tmp.path().join("sample.dat");

    let file_name = "Adventure Quest.bin";
    let contents = b"quest bytes";
    let file_path = write_input_file(&input_dir, file_name, contents)?;
    let size = contents.len();

    let dat_xml = format!(
        r#"<?xml version="1.0"?>
<datafile>
  <game name="{name}">
    <description>Adventure Quest</description>
    <rom name="{name}" size="{size}" />
  </game>
</datafile>"#,
        name = file_name,
        size = size,
    );
    fs::write(&dat_path, dat_xml)?;

    let server = MockServer::start();
    let igdb_mock = server.mock(|when, then| {
        when.method(POST)
            .path("/games")
            .header("Accept", "application/json")
            .header("Content-Type", "text/plain");
        then.status(200)
            .header("content-type", "application/json")
            .body(r#"[{"genres":[{"name":"Action"}]}]"#);
    });

    test_hooks::set_igdb_base_override(&server.url(""));

    let mut cfg = Config::default();
    cfg.commands = vec![Action::Copy];
    cfg.input = vec![input_dir.clone()];
    cfg.output = Some(output_dir.join("{genre}"));
    cfg.dat = vec![dat_path];
    cfg.enable_hasheous = false;
    cfg.igdb_client_id = Some("client".to_string());
    cfg.igdb_token = Some("token".to_string());
    cfg.igdb_mode = IgdbLookupMode::Always;
    cfg.cache_db = Some(cache_db);
    cfg.overwrite = true;

    perform_actions(&cfg)?;

    assert_eq!(igdb_mock.calls(), 1, "expected IGDB lookup to run");
    assert!(
        output_dir
            .join("Action")
            .join(file_path.file_name().unwrap())
            .exists()
    );

    test_hooks::clear_igdb_base_override();
    Ok(())
}
