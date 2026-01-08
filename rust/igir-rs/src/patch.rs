use glob::glob;
use serde::Serialize;
use std::path::PathBuf;

use crate::config::Config;

/// Minimal representation of a discovered patch file.
#[derive(Debug, Clone, Serialize)]
pub struct PatchEntry {
    pub path: PathBuf,
    /// extension in lowercase (eg "ips", "bps")
    pub ext: String,
}

/// Discover patch files according to `config.patch` and `config.patch_exclude`.
pub fn load_patches(config: &Config) -> anyhow::Result<Vec<PatchEntry>> {
    let mut resolved: Vec<PathBuf> = Vec::new();

    for pat in &config.patch {
        let s = pat.to_string_lossy();
        if s.contains('*') || s.contains('?') || s.contains('[') {
            for entry in glob(s.as_ref())? {
                if let Ok(p) = entry {
                    if p.is_file() {
                        resolved.push(p);
                    }
                }
            }
        } else {
            if pat.exists() {
                resolved.push(pat.clone());
            }
        }
    }

    // dedupe & sort
    resolved.sort();
    resolved.dedup();

    // apply excludes
    let mut excluded: Vec<PathBuf> = Vec::new();
    for ex in &config.patch_exclude {
        let s = ex.to_string_lossy();
        if s.contains('*') || s.contains('?') || s.contains('[') {
            for entry in glob(s.as_ref())? {
                if let Ok(p) = entry {
                    excluded.push(p);
                }
            }
        } else {
            excluded.push(ex.clone());
        }
    }

    resolved.retain(|p| !excluded.iter().any(|e| e == p));

    let out = resolved
        .into_iter()
        .map(|p| {
            let ext = p
                .extension()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_ascii_lowercase();
            PatchEntry { path: p, ext }
        })
        .collect();

    Ok(out)
}

/// Guess whether the patch file is supported based on extension.
pub fn guess_patch_type(entry: &PatchEntry) -> Option<&'static str> {
    match entry.ext.as_str() {
        "ips" | "ips32" => Some("ips"),
        "bps" => Some("bps"),
        "ups" => Some("ups"),
        _ => None,
    }
}
