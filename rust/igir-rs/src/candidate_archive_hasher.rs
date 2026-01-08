use crate::config::Config;
use once_cell::sync::OnceCell;
use std::collections::HashMap;
use std::sync::Mutex;

/// Representation of an inner-entry checksum inside an archive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InnerEntryChecksum {
    pub entry_path: String,
    pub crc32: Option<String>,
    pub md5: Option<String>,
    pub sha1: Option<String>,
}

static LAST_ARCHIVE_SCAN: OnceCell<Mutex<HashMap<std::path::PathBuf, Vec<InnerEntryChecksum>>>> =
    OnceCell::new();

/// Scan archives referenced by candidates and populate an in-memory map of
/// archive_path -> inner-entry checksum list. Returns the incoming candidates
/// unchanged for now. This function preferentially uses the existing
/// `archives::scan_zip_entries` / `archives::scan_7z_entries` helpers which may
/// use native or external tools as available.
pub fn process_archive_hashes(
    candidates: Vec<crate::candidates::Candidate>,
    config: &Config,
) -> Vec<crate::candidates::Candidate> {
    let mut map: HashMap<std::path::PathBuf, Vec<InnerEntryChecksum>> = HashMap::new();

    // collect unique archive paths from candidates along with a detected extension
    let mut archives: std::collections::HashMap<std::path::PathBuf, String> =
        std::collections::HashMap::new();
    for cand in candidates.iter() {
        for rec in cand.matches.iter() {
            // consider either the physical source extension or the in-archive relative
            // extension (some temp files may not have a .zip suffix but still be zip data)
            let mut detected: Option<String> = None;
            if let Some(ext) = rec.relative.extension().and_then(|s| s.to_str()) {
                let el = ext.to_ascii_lowercase();
                if el == "zip" || el == "7z" {
                    detected = Some(el);
                }
            }
            if detected.is_none() {
                if let Some(ext) = rec.source.extension().and_then(|s| s.to_str()) {
                    let el = ext.to_ascii_lowercase();
                    if el == "zip" || el == "7z" {
                        detected = Some(el);
                    }
                }
            }
            if let Some(ext) = detected {
                archives.entry(rec.source.clone()).or_insert(ext);
            }
        }
    }

    for (a, detected_ext) in archives.into_iter() {
        let mut entries: Vec<InnerEntryChecksum> = Vec::new();
        // prefer scan based on detected extension (from relative or source)
        if detected_ext == "zip" {
            if let Ok(recs) = crate::archives::scan_zip_entries(&a, config, None) {
                for r in recs.into_iter() {
                    entries.push(InnerEntryChecksum {
                        entry_path: r.relative.to_string_lossy().to_string(),
                        crc32: r.checksums.crc32,
                        md5: r.checksums.md5,
                        sha1: r.checksums.sha1,
                    });
                }
            }
        } else if detected_ext == "7z" {
            if let Ok(recs) = crate::archives::scan_7z_entries(&a, config, None) {
                for r in recs.into_iter() {
                    entries.push(InnerEntryChecksum {
                        entry_path: r.relative.to_string_lossy().to_string(),
                        crc32: r.checksums.crc32,
                        md5: r.checksums.md5,
                        sha1: r.checksums.sha1,
                    });
                }
            }
        }
        if !entries.is_empty() {
            map.insert(a.clone(), entries);
        }
    }

    // store into OnceCell for test inspection or future retrieval
    let cell = LAST_ARCHIVE_SCAN.get_or_init(|| Mutex::new(HashMap::new()));
    if let Ok(mut guard) = cell.lock() {
        *guard = map.clone();
    }

    candidates
}

/// Return a cloned copy of the last archive scan map, if any.
pub fn get_last_archive_scan() -> Option<HashMap<std::path::PathBuf, Vec<InnerEntryChecksum>>> {
    if let Some(cell) = LAST_ARCHIVE_SCAN.get() {
        if let Ok(guard) = cell.lock() {
            return Some(guard.clone());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::candidates::Candidate;
    use crate::types::{ChecksumSet, FileRecord};
    use std::io::Write;
    use std::path::PathBuf;
    use tempfile::NamedTempFile;
    use zip::write::FileOptions;

    #[test]
    fn zip_scan_populates_map() {
        let f = NamedTempFile::new().unwrap();
        {
            let mut zipw = zip::ZipWriter::new(f.reopen().unwrap());
            zipw.start_file::<_, ()>("a.txt", FileOptions::default())
                .unwrap();
            zipw.write_all(b"hello").unwrap();
            zipw.finish().unwrap();
        }

        let rec = FileRecord {
            source: f.path().to_path_buf(),
            relative: PathBuf::from("a.zip"),
            size: 0,
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
        let cand = Candidate {
            name: "a".to_string(),
            matches: vec![rec],
        };
        let cfg = Config::default();
        let out = process_archive_hashes(vec![cand], &cfg);
        assert_eq!(out.len(), 1);

        let map = get_last_archive_scan().expect("expected scan map");
        // Accept a match in any scanned archive: ensure some entry named a.txt was discovered
        let found = map
            .values()
            .any(|entries| entries.iter().any(|e| e.entry_path == "a.txt"));
        assert!(found, "expected at least one archive to contain a.txt");
    }
}
