use std::fs::read_to_string;
use std::path::PathBuf;

#[test]
fn compare_candidates_with_golden_if_present() {
    let golden = PathBuf::from("tests/golden/candidates_golden.json");
    if !golden.exists() {
        eprintln!("candidates golden not present; skipping test");
        return;
    }

    // produce current output using same sample in generator
    let dats = vec![(
        "Game Deluxe.bin".to_string(),
        Some("BEEFCAFE".to_string()),
        None,
        None,
        Some(200u64),
    )];
    let rec_checksum = igir::types::FileRecord {
        source: std::path::PathBuf::from("/node/B/Game.bin"),
        relative: std::path::PathBuf::from("Game.bin"),
        size: 200,
        checksums: igir::types::ChecksumSet {
            crc32: Some("BEEFCAFE".to_string()),
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
    let got = igir::candidates::generate_candidates(&dats, &[rec_checksum]);
    let got_json = serde_json::to_string_pretty(&got).expect("serialize");

    let want = read_to_string(&golden).expect("read golden");
    assert_eq!(
        got_json, want,
        "candidate output differs from golden; regenerate or inspect differences"
    );
}
