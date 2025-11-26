use clap::ValueEnum;
use serde::Serialize;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, ValueEnum, PartialEq, Eq, Hash)]
pub enum Action {
    Copy,
    Move,
    Link,
    Extract,
    Zip,
    Playlist,
    Test,
    Dir2dat,
    Fixdat,
    Clean,
    Report,
}

#[derive(Debug, Clone, Serialize, ValueEnum, PartialEq, Eq, Hash, Copy)]
pub enum Checksum {
    #[serde(rename = "CRC32")]
    Crc32,
    #[serde(rename = "MD5")]
    Md5,
    #[serde(rename = "SHA1")]
    Sha1,
    #[serde(rename = "SHA256")]
    Sha256,
}

impl Checksum {
    pub fn rank(&self) -> u8 {
        match self {
            Checksum::Crc32 => 0,
            Checksum::Md5 => 1,
            Checksum::Sha1 => 2,
            Checksum::Sha256 => 3,
        }
    }
}

#[derive(Debug, Clone, Serialize, ValueEnum)]
pub enum ArchiveChecksumMode {
    Never,
    Auto,
    Always,
}

#[derive(Debug, Clone, Serialize, ValueEnum)]
pub enum DirGameSubdirMode {
    Never,
    Multiple,
    Always,
}

#[derive(Debug, Clone, Serialize, ValueEnum)]
pub enum FixExtensionMode {
    Never,
    Auto,
    Always,
}

#[derive(Debug, Clone, Serialize, ValueEnum)]
pub enum MoveDeleteDirsMode {
    Never,
    Auto,
    Always,
}

#[derive(Debug, Clone, Serialize, ValueEnum)]
pub enum ZipFormat {
    Torrentzip,
    Rvzstd,
}

#[derive(Debug, Clone, Serialize, ValueEnum)]
pub enum LinkMode {
    Hardlink,
    Symlink,
    Reflink,
}

#[derive(Debug, Clone, Serialize, ValueEnum)]
pub enum MergeMode {
    Fullnonmerged,
    Nonmerged,
    Split,
    Merged,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChecksumSet {
    pub crc32: Option<String>,
    pub md5: Option<String>,
    pub sha1: Option<String>,
    pub sha256: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FileRecord {
    pub source: PathBuf,
    pub relative: PathBuf,
    pub size: u64,
    pub checksums: ChecksumSet,
    pub letter_dir: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ActionOutcome {
    pub action: Action,
    pub status: String,
    pub note: String,
}

#[derive(Debug, Serialize)]
pub struct ExecutionPlan {
    pub config: crate::config::Config,
    pub steps: Vec<ActionOutcome>,
    pub files_processed: usize,
}
