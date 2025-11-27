use std::fs;
use std::path::Path;

use anyhow::Context;
use crc32fast::Hasher as Crc32;
use md5::{Digest as Md5Digest, Md5};
use sha1_smol::{Digest, Sha1};
use sha2::Sha256;

use crate::config::Config;
use crate::types::{Checksum, ChecksumSet};

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
    let mut crc32 = None;
    let mut md5 = None;
    let mut sha1 = None;
    let mut sha256 = None;

    let targets = checksum_range(config.input_checksum_min, config.input_checksum_max);
    let buffer = fs::read(path).with_context(|| format!("reading file for checksum: {path:?}"))?;

    for target in targets {
        match target {
            Checksum::Crc32 => {
                let mut hasher = Crc32::new();
                hasher.update(&buffer);
                crc32 = Some(format!("{:08x}", hasher.finalize()));
            }
            Checksum::Md5 => {
                let digest = Md5::digest(&buffer);
                md5 = Some(format!("{:032x}", digest));
            }
            Checksum::Sha1 => {
                let digest = Sha1::digest(&buffer);
                sha1 = Some(format!("{:040x}", digest));
            }
            Checksum::Sha256 => {
                let mut hasher = Sha256::new();
                hasher.update(&buffer);
                sha256 = Some(format!("{:064x}", hasher.finalize()));
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
