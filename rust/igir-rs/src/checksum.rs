use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::mpsc::Sender;

use anyhow::Context;
use crc32fast::Hasher as Crc32;
use md5::{Digest as Md5Digest, Md5};
use sha1_smol::Sha1;
use sha2::Sha256;

use crate::config::Config;
use crate::progress::ProgressEvent;
use crate::types::{Checksum, ChecksumSet};

const STREAM_CHUNK_SIZE: usize = 512 * 1024; // 512 KiB chunks to better utilize network I/O
pub fn compute_checksums_stream<R: Read>(
    mut reader: R,
    config: &Config,
) -> anyhow::Result<(ChecksumSet, u64)> {
    let mut crc32 = None;
    let mut md5 = None;
    let mut sha1 = None;
    let mut sha256 = None;

    let targets = checksum_range(config.input_checksum_min, config.input_checksum_max);
    let mut crc32h = targets
        .iter()
        .any(|t| *t == Checksum::Crc32)
        .then(Crc32::new);
    let mut md5h = targets.iter().any(|t| *t == Checksum::Md5).then(Md5::new);
    let mut sha1h = targets.iter().any(|t| *t == Checksum::Sha1).then(Sha1::new);
    let mut sha256h = targets
        .iter()
        .any(|t| *t == Checksum::Sha256)
        .then(Sha256::new);

    let mut processed: u64 = 0;
    let mut buf = vec![0u8; STREAM_CHUNK_SIZE.min(64 * 1024)];
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        processed = processed.saturating_add(n as u64);
        let slice = &buf[..n];
        if let Some(h) = crc32h.as_mut() {
            h.update(slice);
        }
        if let Some(h) = md5h.as_mut() {
            h.update(slice);
        }
        if let Some(h) = sha1h.as_mut() {
            h.update(slice);
        }
        if let Some(h) = sha256h.as_mut() {
            h.update(slice);
        }
    }

    for target in targets {
        match target {
            Checksum::Crc32 => {
                if let Some(h) = crc32h.take() {
                    crc32 = Some(format!("{:08x}", h.finalize()));
                }
            }
            Checksum::Md5 => {
                if let Some(h) = md5h.take() {
                    let digest = h.finalize();
                    md5 = Some(format!("{:032x}", digest));
                }
            }
            Checksum::Sha1 => {
                if let Some(h) = sha1h.take() {
                    let digest = h.digest();
                    sha1 = Some(digest.to_string());
                }
            }
            Checksum::Sha256 => {
                if let Some(h) = sha256h.take() {
                    sha256 = Some(format!("{:064x}", h.finalize()));
                }
            }
        }
    }

    Ok((
        ChecksumSet {
            crc32,
            md5,
            sha1,
            sha256,
        },
        processed,
    ))
}
const MIN_PROGRESS_UPDATE: u64 = 64 * 1024;

pub fn checksum_range(min: Checksum, max: Option<Checksum>) -> Vec<Checksum> {
    let min_rank = min.rank();
    let max_rank = max.map(|c| c.rank()).unwrap_or(min_rank);

    let mut checksums = Vec::new();
    for value in [
        Checksum::Crc32,
        Checksum::Md5,
        Checksum::Sha1,
        Checksum::Sha256,
    ] {
        if value.rank() >= min_rank && value.rank() <= max_rank {
            checksums.push(value);
        }
    }

    checksums
}

pub fn compute_checksums(path: &Path, config: &Config) -> anyhow::Result<ChecksumSet> {
    compute_checksums_with_header(path, config, None, None)
}

pub fn compute_checksums_with_header(
    path: &Path,
    config: &Config,
    header_size: Option<u64>,
    progress_sender: Option<Sender<ProgressEvent>>,
) -> anyhow::Result<ChecksumSet> {
    let mut crc32 = None;
    let mut md5 = None;
    let mut sha1 = None;
    let mut sha256 = None;

    let targets = checksum_range(config.input_checksum_min, config.input_checksum_max);
    // Stream the file in chunks and update hashers incrementally.
    let mut file =
        File::open(path).with_context(|| format!("opening file for checksum: {path:?}"))?;
    let file_len = file.metadata().map(|m| m.len()).unwrap_or(0);

    // Determine whether trimming should occur: only when header_size <= file_len
    let trim = match header_size {
        Some(s) if s > 0 && s <= file_len => Some(s),
        _ => None,
    };

    // If trimming, consume header bytes first using the streaming buffer to avoid extra allocations.
    let mut buf = vec![0u8; STREAM_CHUNK_SIZE];
    if let Some(mut remaining) = trim {
        while remaining > 0 {
            let to_read = std::cmp::min(remaining, buf.len() as u64) as usize;
            let n = file.read(&mut buf[..to_read])?;
            if n == 0 {
                break;
            }
            remaining -= n as u64;
        }
    }

    // Prepare hashers only for requested targets
    let mut crc32h = if targets.iter().any(|t| *t == Checksum::Crc32) {
        Some(Crc32::new())
    } else {
        None
    };
    let mut md5h = if targets.iter().any(|t| *t == Checksum::Md5) {
        Some(Md5::new())
    } else {
        None
    };
    let mut sha1h = if targets.iter().any(|t| *t == Checksum::Sha1) {
        Some(Sha1::new())
    } else {
        None
    };
    let mut sha256h = if targets.iter().any(|t| *t == Checksum::Sha256) {
        Some(Sha256::new())
    } else {
        None
    };

    let mut bytes_read: u64 = 0;
    let mut last_reported: u64 = 0;
    let report_threshold: u64 = std::cmp::max(MIN_PROGRESS_UPDATE, (buf.len() as u64) / 2);
    let total_size = file_len.saturating_sub(trim.unwrap_or(0));
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            // final update for this file
            if let Some(tx) = &progress_sender {
                let _ = tx.send(ProgressEvent::hashing(
                    path.to_path_buf(),
                    bytes_read,
                    Some(total_size),
                ));
            }
            break;
        }
        let slice = &buf[..n];
        if let Some(h) = crc32h.as_mut() {
            h.update(slice);
        }
        if let Some(h) = md5h.as_mut() {
            h.update(slice);
        }
        if let Some(h) = sha1h.as_mut() {
            h.update(slice);
        }
        if let Some(h) = sha256h.as_mut() {
            h.update(slice);
        }
        bytes_read = bytes_read.saturating_add(n as u64);
        if let Some(tx) = &progress_sender {
            if bytes_read - last_reported >= report_threshold {
                let _ = tx.send(ProgressEvent::hashing(
                    path.to_path_buf(),
                    bytes_read,
                    Some(total_size),
                ));
                last_reported = bytes_read;
            }
        }
    }

    for target in targets {
        match target {
            Checksum::Crc32 => {
                if let Some(h) = crc32h.take() {
                    crc32 = Some(format!("{:08x}", h.finalize()));
                }
            }
            Checksum::Md5 => {
                if let Some(h) = md5h.take() {
                    let digest = h.finalize();
                    md5 = Some(format!("{:032x}", digest));
                }
            }
            Checksum::Sha1 => {
                if let Some(h) = sha1h.take() {
                    let digest = h.digest();
                    sha1 = Some(digest.to_string());
                }
            }
            Checksum::Sha256 => {
                if let Some(h) = sha256h.take() {
                    sha256 = Some(format!("{:064x}", h.finalize()));
                }
            }
        }
    }

    Ok(ChecksumSet {
        crc32,
        md5,
        sha1,
        sha256,
    })
}

/// Compute checksums from an in-memory buffer. Mirrors behavior of compute_checksums_with_header
pub fn compute_checksums_from_bytes(buf: &[u8], config: &Config) -> anyhow::Result<ChecksumSet> {
    let cursor = std::io::Cursor::new(buf);
    let (checksums, _) = compute_checksums_stream(cursor, config)?;
    Ok(checksums)
}

/// Compute all supported checksums for a file path (ignores `Config` settings).
pub fn compute_all_checksums(path: &Path) -> anyhow::Result<ChecksumSet> {
    let mut file =
        File::open(path).with_context(|| format!("opening file for checksum: {path:?}"))?;

    // prepare hashers
    let mut crc32h = Crc32::new();
    let mut md5h = Md5::new();
    let mut sha1h = Sha1::new();
    let mut sha256h = Sha256::new();

    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        let slice = &buf[..n];
        crc32h.update(slice);
        md5h.update(slice);
        sha1h.update(slice);
        sha256h.update(slice);
    }

    let crc32 = Some(format!("{:08x}", crc32h.finalize()));
    let md5 = Some(format!("{:032x}", md5h.finalize()));
    let sha1 = Some(sha1h.digest().to_string());
    let sha256 = Some(format!("{:064x}", sha256h.finalize()));

    Ok(ChecksumSet {
        crc32,
        md5,
        sha1,
        sha256,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    // Helper to construct a config that computes all checksum types
    fn all_checksums_config() -> crate::config::Config {
        let mut c = crate::config::Config::default();
        c.input_checksum_min = Checksum::Crc32;
        c.input_checksum_max = Some(Checksum::Sha256);
        c
    }

    #[test]
    fn trimmed_checksums_match_body_bytes() {
        let mut f = NamedTempFile::new().unwrap();
        let header = vec![0xAAu8; 128];
        let body = b"hello trimmed world".to_vec();
        f.write_all(&header).unwrap();
        f.write_all(&body).unwrap();
        f.flush().unwrap();

        let cfg = all_checksums_config();

        let trimmed = compute_checksums_with_header(f.path(), &cfg, Some(128), None).unwrap();
        let from_bytes = compute_checksums_from_bytes(&body, &cfg).unwrap();

        assert_eq!(trimmed.crc32, from_bytes.crc32);
        assert_eq!(trimmed.md5, from_bytes.md5);
        assert_eq!(trimmed.sha1, from_bytes.sha1);
        assert_eq!(trimmed.sha256, from_bytes.sha256);
    }

    #[test]
    fn header_size_equal_file_results_in_empty_body_checksums() {
        let mut f = NamedTempFile::new().unwrap();
        let header = vec![0xFFu8; 64];
        f.write_all(&header).unwrap();
        f.flush().unwrap();

        let cfg = all_checksums_config();

        // header_size equals file length -> trimmed body is empty
        let trimmed = compute_checksums_with_header(f.path(), &cfg, Some(64), None).unwrap();
        let empty = compute_checksums_from_bytes(&[], &cfg).unwrap();

        assert_eq!(trimmed.crc32, empty.crc32);
        assert_eq!(trimmed.md5, empty.md5);
        assert_eq!(trimmed.sha1, empty.sha1);
        assert_eq!(trimmed.sha256, empty.sha256);
    }

    #[test]
    fn header_size_larger_than_file_uses_full_buffer() {
        let mut f = NamedTempFile::new().unwrap();
        let data = b"actual data";
        f.write_all(data).unwrap();
        f.flush().unwrap();

        let cfg = all_checksums_config();

        // header_size larger than file length -> no trimming should occur
        let computed = compute_checksums_with_header(f.path(), &cfg, Some(1024), None).unwrap();
        let full = compute_checksums_from_bytes(data, &cfg).unwrap();

        assert_eq!(computed.crc32, full.crc32);
        assert_eq!(computed.md5, full.md5);
        assert_eq!(computed.sha1, full.sha1);
        assert_eq!(computed.sha256, full.sha256);
    }
}
