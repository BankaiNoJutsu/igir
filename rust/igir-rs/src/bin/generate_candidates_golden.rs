use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

fn main() -> anyhow::Result<()> {
    // Sample minimal dataset — users can replace with Node-produced dats and records
    let dats = vec![(
        "Game Deluxe.bin".to_string(),
        Some("BEEFCAFE".to_string()),
        None,
        None,
        Some(200u64),
    )];

    let rec_checksum = igir::types::FileRecord {
        source: PathBuf::from("/node/B/Game.bin"),
        relative: PathBuf::from("Game.bin"),
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

    let candidates = igir::candidates::generate_candidates(&dats, &[rec_checksum]);
    let out_path = PathBuf::from("tests/golden/candidates_golden.json");
    if let Some(p) = out_path.parent() {
        std::fs::create_dir_all(p)?;
    }
    let mut f = File::create(&out_path)?;
    let json = serde_json::to_string_pretty(&candidates)?;
    f.write_all(json.as_bytes())?;
    println!("wrote candidates golden: {:?}", out_path);
    Ok(())
}
