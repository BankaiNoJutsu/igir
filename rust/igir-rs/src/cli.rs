use clap::{ArgAction, Parser};
use std::path::PathBuf;

use crate::types::{
    Action, ArchiveChecksumMode, Checksum, DirGameSubdirMode, FixExtensionMode, IgdbLookupMode,
    LinkMode, MergeMode, MoveDeleteDirsMode, ZipFormat,
};

#[derive(Parser, Debug, serde::Serialize)]
#[command(
    name = "igir",
    version,
    about = "Rust rewrite of Igir ROM collection manager",
    long_about = include_str!("help_examples.md")
)]
pub struct Cli {
    /// Commands to run (can specify multiple)
    #[arg(value_enum, value_name = "COMMAND", action = ArgAction::Append)]
    pub commands: Vec<Action>,

    // ROM input options
    /// Path(s) to ROM files or archives (supports globbing)
    #[arg(short = 'i', long = "input", value_name = "PATH", action = ArgAction::Append)]
    pub input: Vec<PathBuf>,

    /// Path(s) to ROM files or archives to exclude from processing (supports globbing)
    #[arg(short = 'I', long = "input-exclude", value_name = "PATH", action = ArgAction::Append)]
    pub input_exclude: Vec<PathBuf>,

    /// Only read checksums from archive headers, don't decompress to calculate
    #[arg(long = "input-checksum-quick")]
    pub input_checksum_quick: bool,

    /// The minimum checksum level to calculate and use for matching
    #[arg(
        long = "input-checksum-min",
        value_enum,
        default_value_t = Checksum::Crc32,
    )]
    pub input_checksum_min: Checksum,

    /// The maximum checksum level to calculate and use for matching
    #[arg(long = "input-checksum-max", value_enum)]
    pub input_checksum_max: Option<Checksum>,

    /// Calculate checksums of archive files themselves, allowing them to match files in DATs
    #[arg(
        long = "input-checksum-archives",
        value_enum,
        default_value_t = ArchiveChecksumMode::Auto,
    )]
    pub input_checksum_archives: ArchiveChecksumMode,

    // DAT input options (parsed but not yet used for matching)
    #[arg(short = 'd', long = "dat", value_name = "PATH", action = ArgAction::Append)]
    pub dat: Vec<PathBuf>,
    #[arg(long = "dat-exclude", value_name = "PATH", action = ArgAction::Append)]
    pub dat_exclude: Vec<PathBuf>,
    #[arg(long = "dat-name-regex", value_name = "REGEX")]
    pub dat_name_regex: Option<String>,
    #[arg(long = "dat-name-regex-exclude", value_name = "REGEX")]
    pub dat_name_regex_exclude: Option<String>,
    #[arg(long = "dat-description-regex", value_name = "REGEX")]
    pub dat_description_regex: Option<String>,
    #[arg(long = "dat-description-regex-exclude", value_name = "REGEX")]
    pub dat_description_regex_exclude: Option<String>,
    #[arg(long = "dat-combine")]
    pub dat_combine: bool,
    #[arg(long = "dat-ignore-parent-clone")]
    pub dat_ignore_parent_clone: bool,
    /// Include unmatched DAT entries in the printed execution plan JSON
    #[arg(long = "list-unmatched-dats")]
    pub list_unmatched_dats: bool,
    /// Enable Hasheous lookups for unmatched ROMs
    #[arg(long = "enable-hasheous")]
    pub enable_hasheous: bool,
    /// IGDB client id for online matching of unmatched ROMs
    #[arg(long = "igdb-client-id", value_name = "ID")]
    pub igdb_client_id: Option<String>,
    /// IGDB client secret used to automatically obtain OAuth tokens when needed
    #[arg(long = "igdb-client-secret", value_name = "SECRET")]
    pub igdb_client_secret: Option<String>,
    /// IGDB token for online matching of unmatched ROMs
    #[arg(long = "igdb-token", value_name = "TOKEN")]
    pub igdb_token: Option<String>,
    /// Strategy for IGDB lookups (best-effort, always, or off)
    #[arg(
        long = "igdb-mode",
        value_enum,
        value_name = "MODE",
        default_value_t = IgdbLookupMode::BestEffort
    )]
    pub igdb_mode: IgdbLookupMode,
    /// Path to sqlite cache DB file. If omitted a default is used inside the output or next to the binary.
    #[arg(long = "cache-db", value_name = "PATH")]
    pub cache_db: Option<PathBuf>,
    /// Number of threads to use for hashing (overrides default of logical CPU count)
    #[arg(long = "hash-threads", value_name = "N")]
    pub hash_threads: Option<usize>,
    /// Number of threads to use for scanning (overrides default of logical CPU count)
    #[arg(long = "scan-threads", value_name = "N")]
    pub scan_threads: Option<usize>,
    /// Show per-DAT match reasons in the IGIR summary output
    #[arg(long = "show-match-reasons")]
    pub show_match_reasons: bool,
    /// Only use cached Hasheous/IGDB results; never perform network lookups
    #[arg(long = "cache-only")]
    pub cache_only: bool,
    /// If set, save the provided or discovered IGDB client id/token to the persistent config file
    #[arg(long = "save-igdb-creds")]
    pub save_igdb_creds: bool,

    // Patch input options
    #[arg(short = 'p', long = "patch", value_name = "PATH", action = ArgAction::Append)]
    pub patch: Vec<PathBuf>,
    #[arg(short = 'P', long = "patch-exclude", value_name = "PATH", action = ArgAction::Append)]
    pub patch_exclude: Vec<PathBuf>,

    // ROM output path options
    #[arg(short = 'o', long = "output", value_name = "PATH")]
    pub output: Option<PathBuf>,
    #[arg(long = "dir-mirror")]
    pub dir_mirror: bool,
    #[arg(long = "dir-dat-mirror")]
    pub dir_dat_mirror: bool,
    #[arg(short = 'D', long = "dir-dat-name")]
    pub dir_dat_name: bool,
    #[arg(long = "dir-dat-description")]
    pub dir_dat_description: bool,
    #[arg(long = "dir-letter")]
    pub dir_letter: bool,
    #[arg(long = "dir-letter-count", value_name = "NUM")]
    pub dir_letter_count: Option<usize>,
    #[arg(long = "dir-letter-limit", value_name = "NUM")]
    pub dir_letter_limit: Option<usize>,
    #[arg(long = "dir-letter-group")]
    pub dir_letter_group: bool,
    #[arg(
        long = "dir-game-subdir",
        value_enum,
        default_value_t = DirGameSubdirMode::Multiple,
    )]
    pub dir_game_subdir: DirGameSubdirMode,

    // ROM writing options
    #[arg(
        long = "fix-extension",
        value_enum,
        default_value_t = FixExtensionMode::Auto,
    )]
    pub fix_extension: FixExtensionMode,
    #[arg(short = 'O', long = "overwrite")]
    pub overwrite: bool,
    #[arg(long = "overwrite-invalid")]
    pub overwrite_invalid: bool,

    // move command options
    #[arg(
        long = "move-delete-dirs",
        value_enum,
        default_value_t = MoveDeleteDirsMode::Auto,
    )]
    pub move_delete_dirs: MoveDeleteDirsMode,

    // clean command options
    #[arg(short = 'C', long = "clean-exclude", value_name = "PATH", action = ArgAction::Append)]
    pub clean_exclude: Vec<PathBuf>,
    #[arg(long = "clean-backup", value_name = "PATH")]
    pub clean_backup: Option<PathBuf>,
    #[arg(long = "clean-dry-run")]
    pub clean_dry_run: bool,

    // zip command options
    #[arg(
        long = "zip-format",
        value_enum,
        default_value_t = ZipFormat::Torrentzip,
    )]
    pub zip_format: ZipFormat,
    #[arg(short = 'Z', long = "zip-exclude", value_name = "GLOB")]
    pub zip_exclude: Option<String>,
    #[arg(long = "zip-dat-name")]
    pub zip_dat_name: bool,

    // link command options
    #[arg(
        long = "link-mode",
        value_enum,
        default_value_t = LinkMode::Hardlink,
    )]
    pub link_mode: LinkMode,
    #[arg(long = "symlink-relative")]
    pub symlink_relative: bool,

    // header options
    #[arg(long = "header", value_name = "GLOB")]
    pub header: Option<String>,
    #[arg(short = 'H', long = "remove-headers", value_name = "EXTENSIONS")]
    pub remove_headers: Option<String>,

    // trimmed ROM options
    #[arg(long = "trimmed-glob", value_name = "GLOB")]
    pub trimmed_glob: Option<String>,
    #[arg(long = "trim-scan-archives")]
    pub trim_scan_archives: bool,

    // ROM set options
    #[arg(
        long = "merge-roms",
        value_enum,
        default_value_t = MergeMode::Fullnonmerged,
    )]
    pub merge_roms: MergeMode,
    #[arg(long = "merge-discs")]
    pub merge_discs: bool,
    #[arg(long = "exclude-disks")]
    pub exclude_disks: bool,
    #[arg(long = "allow-excess-sets")]
    pub allow_excess_sets: bool,
    #[arg(long = "allow-incomplete-sets")]
    pub allow_incomplete_sets: bool,

    // ROM filtering options
    #[arg(short = 'x', long = "filter-regex", value_name = "REGEX")]
    pub filter_regex: Option<String>,
    #[arg(short = 'X', long = "filter-regex-exclude", value_name = "REGEX")]
    pub filter_regex_exclude: Option<String>,
    #[arg(short = 'L', long = "filter-language", value_name = "LANGS")]
    pub filter_language: Option<String>,
    #[arg(short = 'R', long = "filter-region", value_name = "REGIONS")]
    pub filter_region: Option<String>,
    #[arg(long = "filter-category-regex", value_name = "REGEX")]
    pub filter_category_regex: Option<String>,
    #[arg(long = "no-bios")]
    pub no_bios: bool,
    #[arg(long = "no-device")]
    pub no_device: bool,
    #[arg(long = "no-unlicensed")]
    pub no_unlicensed: bool,
    #[arg(long = "only-retail")]
    pub only_retail: bool,
    #[arg(long = "no-debug")]
    pub no_debug: bool,
    #[arg(long = "no-demo")]
    pub no_demo: bool,
    #[arg(long = "no-beta")]
    pub no_beta: bool,
    #[arg(long = "no-sample")]
    pub no_sample: bool,
    #[arg(long = "no-prototype")]
    pub no_prototype: bool,
    #[arg(long = "no-program")]
    pub no_program: bool,

    // logging options (limited parity)
    #[arg(short = 'v', long = "verbose", action = ArgAction::Count)]
    pub verbose: u8,
    #[arg(short = 'q', long = "quiet", action = ArgAction::Count)]
    pub quiet: u8,
    /// Print the execution plan JSON to stdout (opt-in)
    #[arg(long = "print-plan")]
    pub print_plan: bool,
    /// Enable diagnostic progress logging on the DIAG bar
    #[arg(long = "diag")]
    pub diag: bool,
}
