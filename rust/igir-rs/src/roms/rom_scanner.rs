use crate::roms::chd;
use std::fs;
use std::io::Read;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct RomInfo {
    pub path: String,
    /// If a header was detected, header size in bytes (e.g., 128 or 512)
    pub header_size: Option<u64>,
    pub is_chd: bool,
    pub is_nkit: bool,
    pub is_iso: bool,
    pub is_pbp: bool,
    pub is_psx_exe: bool,
    pub is_cue: bool,
    pub trimmed_size: u64,
}

/// Heuristic rom scanner to detect common header sizes and archive-like types.
/// It tries a few common header sizes (128, 256, 512) using a size-based heuristic.
pub fn scan(path: &Path) -> anyhow::Result<RomInfo> {
    let meta = fs::metadata(path)?;
    let size = meta.len();

    // Recognize CHD and NKit by extension first
    let lc = path
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    let mut is_chd = lc == "chd";
    let mut is_nkit = lc.contains("nkit");
    let mut is_iso = lc == "iso";
    let mut is_pbp = lc == "pbp";
    let mut is_psx_exe = false;
    let mut is_cue = lc == "cue";

    // First, try signature-based header detection by reading initial bytes
    let mut file = fs::File::open(path)?;
    // Read up to 1024 bytes which is more than the largest header we check
    let mut buf = vec![0u8; 1024.min(size as usize)];
    let _read = file.read(&mut buf)?;

    // Known headers mapping: (name, header_offset_bytes, hex string pattern, data_offset)
    // We'll check a small subset used by the TypeScript implementation
    let known_headers: &[(&str, usize, &str, u64)] = &[
        // NES: 0x4E45531A at offset 0, data offset 16
        ("NES", 0, "4E45531A", 16),
        // SMC: large pattern is handled by checking 0x00 repeated at offset 3
        ("SMC", 3, &"00".repeat(509), 512),
        // LNX (Lynx): '4C594E58' at offset 0
        ("LNX", 0, "4C594E58", 64),
    ];

    let mut header_size: Option<u64> = None;
    for (_name, offset, hexpat, data_offset) in known_headers {
        let needed = offset + hexpat.len() / 2;
        if buf.len() >= needed {
            let slice = &buf[*offset..(*offset + hexpat.len() / 2)];
            let hex = slice
                .iter()
                .map(|b| format!("{:02X}", b))
                .collect::<String>();
            if hex == hexpat.to_uppercase() {
                header_size = Some(*data_offset);
                break;
            }
        }
    }

    // Detect CHD via magic header ("MCompr"). CHD files often start with "MCompr" or "MCHD" depending on version
    if !is_chd {
        if buf.len() >= 4 {
            let magic = &buf[0..4];
            let magic_s = magic
                .iter()
                .map(|b| format!("{:02X}", b))
                .collect::<String>();
            // 'M' 'C' 'H' 'D' -> 4D 43 48 44 or ASCII "MCHD" / "MCompr" prefix
            if magic_s.starts_with("4D43") {
                is_chd = true;
            }
        }
    }

    // NKit detection heuristic: some NKit files are zipped or have 'NKIT' in filename; try to detect 'NKIT' magic in initial bytes
    if !is_nkit {
        let name_lc = path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        if name_lc.contains("nkit") {
            is_nkit = true;
        } else {
            // check for ASCII 'NKIT' or 'nkit' in the buffer
            if buf.windows(4).any(|w| w == b"NKIT" || w == b"nkit") {
                is_nkit = true;
            }
        }
    }

    // Detect PBP (PSP container) by header
    if !is_pbp {
        if buf.len() >= 3 {
            if &buf[0..3] == b"PBP" {
                is_pbp = true;
            }
        }
    }

    // Detect PS-X EXE by ASCII signature anywhere in header ("PS-X EXE")
    if !is_psx_exe {
        if buf.windows(8).any(|w| w == b"PS-X EXE") {
            is_psx_exe = true;
        }
    }

    // Detect CUE by extension or content tokens
    if !is_cue {
        if buf.windows(4).any(|w| w.eq_ignore_ascii_case(b"FILE"))
            || buf.windows(5).any(|w| w.eq_ignore_ascii_case(b"TRACK"))
        {
            is_cue = true;
        }
    }

    // Detect ISO9660 primary volume descriptor signature "CD001" at sector 16 (offset 0x8000)
    if !is_iso {
        if size > 0x8000 {
            use std::io::Seek;
            use std::io::SeekFrom;
            if let Ok(_) = file.seek(SeekFrom::Start(0x8000)) {
                let mut v = [0u8; 6];
                if let Ok(n) = file.read(&mut v) {
                    if n >= 5 {
                        if &v[1..6] == b"CD001" {
                            is_iso = true;
                        }
                    }
                }
            }
        }
    }

    // Fallback heuristics: test common header sizes
    if header_size.is_none() {
        let candidates: [u64; 3] = [128, 256, 512];
        for &candidate in &candidates {
            if size > candidate {
                if size % 512 == 0 || size % 512 == candidate || (size - candidate) % 512 == 0 {
                    header_size = Some(candidate);
                    break;
                }
            }
        }
    }

    // If this is a CHD, try to parse header to obtain uncompressed size
    let mut computed_trimmed = if let Some(h) = header_size {
        size.saturating_sub(h)
    } else {
        size
    };
    if is_chd {
        if let Ok(Some(info)) = chd::parse_chd_header(path) {
            if let Some(uncomp) = info.uncompressed_size {
                computed_trimmed = uncomp;
            }
        }
    }

    let trimmed_size = computed_trimmed;

    Ok(RomInfo {
        path: path.to_string_lossy().to_string(),
        header_size,
        is_chd,
        is_nkit,
        is_iso,
        is_pbp,
        is_psx_exe,
        is_cue,
        trimmed_size,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn detects_header_by_size_modulo() {
        let mut f = NamedTempFile::new().unwrap();
        // create file of size 1024 (multiple of 512)
        f.write_all(&vec![0u8; 1024]).unwrap();
        let info = scan(f.path()).unwrap();
        assert!(info.header_size.is_some());
        assert_eq!(info.trimmed_size, 1024 - info.header_size.unwrap());
    }

    #[test]
    fn no_header_small_file() {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(&vec![0u8; 300]).unwrap();
        let info = scan(f.path()).unwrap();
        assert!(info.header_size.is_none());
        assert_eq!(info.trimmed_size, 300);
    }

    #[test]
    fn detects_chd_by_extension_and_magic() {
        let mut f = NamedTempFile::new().unwrap();
        // write CHD-like magic 'MCHD' then some data
        f.write_all(&[0x4D, 0x43, 0x48, 0x44]).unwrap();
        f.write_all(&vec![0u8; 100]).unwrap();
        let info = scan(f.path()).unwrap();
        // extension won't indicate CHD here, but magic should
        assert!(info.is_chd);
    }

    #[test]
    fn detects_nkit_by_buffer() {
        let mut f = NamedTempFile::new().unwrap();
        // embed 'NKIT' in the start
        f.write_all(b"NKIT").unwrap();
        f.write_all(&vec![0u8; 100]).unwrap();
        let info = scan(f.path()).unwrap();
        assert!(info.is_nkit);
    }
}
