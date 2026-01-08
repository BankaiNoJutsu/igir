use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use crate::config::Config;
use crate::types::FixExtensionMode;

/// Return (extension, confidence) if a signature is recognized.
fn detect_extension_from_bytes(buf: &[u8]) -> Option<(&'static str, f32)> {
    // Check common exact headers first (high confidence)
    if buf.len() >= 4 {
        // NES: "NES\x1A"
        if buf[0..4] == [0x4E, 0x45, 0x53, 0x1A] {
            return Some(("nes", 0.99));
        }
        // Game Boy ROM header: at 0x104 the Nintendo logo begins; detect by 0x00 at 0x104..0x104+6 heuristic
    }

    if buf.len() >= 64 {
        // Lynx 'LYNX' at start
        if buf[0..4] == [0x4C, 0x59, 0x4E, 0x58] {
            return Some(("lnx", 0.95));
        }
    }

    if buf.len() >= 512 {
        // SMC/SFC heuristic: presence of 0x00 at offset 3 is common in SNES headers
        if buf[3] == 0x00 {
            return Some(("smc", 0.7));
        }
    }

    // Heuristic patterns: look for 'SEGA' at offset 0x100 for Mega Drive / Genesis
    if buf.len() >= 0x200 {
        if buf[0x100..0x104] == [0x53, 0x45, 0x47, 0x41] {
            return Some(("bin", 0.9));
        }
    }

    // Not recognized
    None
}

/// Post-process candidates to correct file extensions based on headers.
/// Behavior modes:
/// - `Never`: do nothing.
/// - `Always`: always replace extension when a signature is found.
/// - `Auto`: replace only when the detection confidence exceeds the heuristic threshold.
pub fn postprocess_candidates(
    mut candidates: Vec<crate::candidates::Candidate>,
    config: &Config,
) -> Vec<crate::candidates::Candidate> {
    match config.fix_extension {
        FixExtensionMode::Never => return candidates,
        _ => (),
    }

    let auto_threshold: f32 = 0.9; // confidence threshold for Auto mode

    for cand in candidates.iter_mut() {
        for rec in cand.matches.iter_mut() {
            let src = rec.source.clone();
            if let Ok(mut f) = File::open(&src) {
                let to_read = 1024.min(rec.size as usize).max(16);
                let mut buf = vec![0u8; to_read];
                if let Ok(n) = f.read(&mut buf) {
                    buf.truncate(n);
                    if let Some((ext, conf)) = detect_extension_from_bytes(&buf) {
                        let should_apply = match config.fix_extension {
                            FixExtensionMode::Always => true,
                            FixExtensionMode::Auto => conf >= auto_threshold,
                            FixExtensionMode::Never => false,
                        };
                        if should_apply {
                            if let Some(stem) = rec.relative.file_stem().and_then(|s| s.to_str()) {
                                let new_rel = PathBuf::from(format!("{}.{}", stem, ext));
                                rec.relative = new_rel;
                            }
                        }
                    }
                }
            }
        }
    }

    candidates
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ChecksumSet, FileRecord};
    use std::io::Write;
    use std::path::PathBuf;
    use tempfile::NamedTempFile;

    #[test]
    fn detects_nes_header_and_changes_relative_when_always() {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(&[0x4E, 0x45, 0x53, 0x1A]).unwrap();
        f.flush().unwrap();

        let rec = FileRecord {
            source: f.path().to_path_buf(),
            relative: PathBuf::from("game.bin"),
            size: 4,
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
        let cand = crate::candidates::Candidate {
            name: "g".to_string(),
            matches: vec![rec],
        };

        let cfg = Config {
            fix_extension: FixExtensionMode::Always,
            ..Config::default()
        };
        let out = postprocess_candidates(vec![cand], &cfg);
        assert_eq!(out[0].matches[0].relative.to_string_lossy(), "game.nes");
    }

    #[test]
    fn auto_mode_respects_threshold() {
        let mut f = NamedTempFile::new().unwrap();
        // write a lower-confidence SMC-like header (we treat as 0.7)
        let mut buf = vec![0u8; 512];
        buf[3] = 0x00;
        f.write_all(&buf).unwrap();
        f.flush().unwrap();

        let rec = FileRecord {
            source: f.path().to_path_buf(),
            relative: PathBuf::from("game.bin"),
            size: 512,
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
        let cand = crate::candidates::Candidate {
            name: "g".to_string(),
            matches: vec![rec],
        };

        let cfg_auto = Config {
            fix_extension: FixExtensionMode::Auto,
            ..Config::default()
        };
        let out_auto = postprocess_candidates(vec![cand.clone()], &cfg_auto);
        // confidence 0.7 < 0.9 threshold -> no change
        assert_eq!(
            out_auto[0].matches[0].relative.to_string_lossy(),
            "game.bin"
        );

        let cfg_always = Config {
            fix_extension: FixExtensionMode::Always,
            ..Config::default()
        };
        let out_always = postprocess_candidates(vec![cand], &cfg_always);
        assert_eq!(
            out_always[0].matches[0].relative.to_string_lossy(),
            "game.smc"
        );
    }
}
