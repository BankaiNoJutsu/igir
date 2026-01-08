use anyhow::Context;
use std::fs::File;
use std::io::Read;
use std::path::Path;

// If the "libchd" feature is enabled we will attempt to route parsing
// through an external CHD parsing crate or FFI binding. For now that
// integration is not provided; keep a lightweight fallback implementation
// so the rest of the crate can function without the feature.
#[cfg(feature = "libchd")]
mod libchd_integration {
    use anyhow::Context;
    use std::fs::File;
    use std::io::BufReader;
    use std::path::Path;

    pub fn parse_chd_header(path: &Path) -> anyhow::Result<Option<super::ChdInfo>> {
        // Use the chd crate to read header and metadata
        let f = File::open(path).with_context(|| format!("opening chd: {:?}", path))?;
        let mut reader = BufReader::new(f);
        let mut chd =
            chd::Chd::open(&mut reader, None).with_context(|| "opening CHD via chd crate")?;
        let header = chd.header();
        // Use header logical_bytes to obtain logical/uncompressed size
        let uncompressed = Some(header.logical_bytes());
        // Extract available checksums from header (if present)
        let sha1 = header.sha1().map(|arr| hex::encode(arr));
        let md5 = header.md5().map(|arr| hex::encode(arr));
        let raw_sha1 = header.raw_sha1().map(|arr| hex::encode(arr));
        Ok(Some(super::ChdInfo {
            tag: "chd-crate".to_string(),
            uncompressed_size: uncompressed,
            sha1,
            md5,
            raw_sha1,
        }))
    }
}

#[derive(Debug, Clone)]
pub struct ChdInfo {
    /// detected version or tag
    pub tag: String,
    /// uncompressed size if known
    pub uncompressed_size: Option<u64>,
    /// optional SHA-1 checksum (hex)
    pub sha1: Option<String>,
    /// optional MD5 checksum (hex)
    pub md5: Option<String>,
    /// optional raw SHA-1 (hex) as provided by header
    pub raw_sha1: Option<String>,
}

/// Best-effort CHD header inspection. This is not a full CHD parser.
/// It looks for common magic and returns limited metadata. For full parsing
/// a dedicated CHD crate or libchd binding is recommended.
pub fn parse_chd_header(path: &Path) -> anyhow::Result<Option<ChdInfo>> {
    // If the libchd feature is enabled call into the integration module.
    #[cfg(feature = "libchd")]
    {
        if let Ok(Some(info)) = libchd_integration::parse_chd_header(path) {
            return Ok(Some(info));
        }
    }

    // Lightweight fallback: read the first bytes and look for common CHD markers.
    let mut f = File::open(path).with_context(|| format!("opening CHD file: {:?}", path))?;
    let mut buf = [0u8; 512];
    let n = f.read(&mut buf)?;
    let s = String::from_utf8_lossy(&buf[..n]).to_string();

    // Look for common CHD header markers
    if s.contains("MCompr") || s.contains("MCHD") {
        // Try to find an ASCII decimal uncompressed size token like "LENGTH=" or "len="
        let mut uncompressed: Option<u64> = None;
        if let Some(pos) = s.find("LENGTH=") {
            let tail = &s[pos + 7..];
            let digits: String = tail.chars().take_while(|c| c.is_ascii_digit()).collect();
            if let Ok(val) = digits.parse::<u64>() {
                uncompressed = Some(val);
            }
        }

        // best-effort extract ascii hex checksums from the first 512 bytes
        let mut sha1_token: Option<String> = None;
        let mut md5_token: Option<String> = None;
        if let Ok(re) = regex::Regex::new(r"(?i)\b([0-9a-f]{40})\b") {
            if let Some(m) = re.find(&s) {
                sha1_token = Some(m.as_str().to_string());
            }
        }
        if let Ok(re) = regex::Regex::new(r"(?i)\b([0-9a-f]{32})\b") {
            if let Some(m) = re.find(&s) {
                md5_token = Some(m.as_str().to_string());
            }
        }

        return Ok(Some(ChdInfo {
            tag: "chd-detected".to_string(),
            uncompressed_size: uncompressed,
            sha1: sha1_token.clone(),
            md5: md5_token,
            raw_sha1: sha1_token.clone(),
        }));
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn parse_chd_magic() {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(b"MCHD").unwrap();
        let info = parse_chd_header(f.path()).unwrap();
        assert!(info.is_some());
    }
}
