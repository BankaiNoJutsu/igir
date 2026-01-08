use clap::ValueEnum;
use serde::Serialize;
use std::fmt;
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

#[derive(Debug, Serialize, ValueEnum, PartialEq, Eq, Copy, Clone)]
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
    Deflate,
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

#[derive(Debug, Clone, Serialize, ValueEnum, PartialEq, Eq)]
pub enum IgdbLookupMode {
    BestEffort,
    Always,
    Off,
}

impl Default for IgdbLookupMode {
    fn default() -> Self {
        IgdbLookupMode::BestEffort
    }
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
    // Optional platform token derived from online hints / cache (e.g. "snes").
    // When present this should be preferred for token expansion instead of
    // guessing from extension or DATs.
    pub derived_platform: Option<String>,
    // Optional genre tags populated from IGDB or other online hints.
    pub derived_genres: Vec<String>,
    // Optional region token (e.g. "USA", "EUR") derived from tag parsing or metadata.
    pub derived_region: Option<String>,
    // Optional language codes derived from ROM tags or metadata (e.g. "EN").
    pub derived_languages: Vec<String>,
    #[serde(skip)]
    pub scan_info: Option<crate::roms::rom_scanner::RomInfo>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum SkipReason {
    #[serde(rename = "regex_include")]
    RegexInclude,
    #[serde(rename = "regex_exclude")]
    RegexExclude,
    #[serde(rename = "region_language")]
    RegionLanguage,
}

impl fmt::Display for SkipReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SkipReason::RegexInclude => write!(f, "failed include regex"),
            SkipReason::RegexExclude => write!(f, "matched exclude regex"),
            SkipReason::RegionLanguage => write!(f, "filtered by region/language"),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SkippedFile {
    pub path: PathBuf,
    pub reason: SkipReason,
    pub detail: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SkipSummary {
    pub reason: SkipReason,
    pub count: usize,
}

#[derive(Debug, Serialize)]
pub struct FilterSummary {
    pub region: Option<String>,
    pub language: Option<String>,
    pub include_regex: Option<String>,
    pub exclude_regex: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct RunSummary {
    pub total_inputs: usize,
    pub input_roots: Vec<PathBuf>,
    pub files_processed: usize,
    pub files_skipped: usize,
    pub files_copied: Option<usize>,
    pub dat_unmatched: usize,
    pub skip_breakdown: Vec<SkipSummary>,
    pub actions_run: Vec<Action>,
    pub filters: FilterSummary,
}

#[derive(Debug, Serialize)]
pub struct FileCollection {
    pub records: Vec<FileRecord>,
    pub skipped: Vec<SkippedFile>,
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
    pub dat_matched: Vec<crate::dat::DatRom>,
    pub dat_unmatched: Vec<crate::dat::DatRom>,
    pub online_matches: Vec<crate::dat::OnlineMatch>,
    pub skipped: Vec<SkippedFile>,
    pub summary: RunSummary,
}
