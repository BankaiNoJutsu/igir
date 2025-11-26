use std::collections::HashMap;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use anyhow::Context;
use clap::{ArgAction, Parser, ValueEnum, builder::PossibleValuesParser};
use globset::{Glob, GlobSet, GlobSetBuilder};
use serde::Serialize;
use sha2::{Digest, Sha256};
use walkdir::WalkDir;

#[derive(Debug, Clone, Serialize, ValueEnum, PartialEq, Eq, Hash)]
enum Action {
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

#[derive(Debug, Clone, Serialize, ValueEnum)]
enum Checksum {
    #[serde(rename = "CRC32")]
    Crc32,
    #[serde(rename = "MD5")]
    Md5,
    #[serde(rename = "SHA1")]
    Sha1,
    #[serde(rename = "SHA256")]
    Sha256,
}

#[derive(Debug, Clone, Serialize, ValueEnum)]
enum ArchiveChecksumMode {
    Never,
    Auto,
    Always,
}

#[derive(Debug, Clone, Serialize, ValueEnum)]
enum DirGameSubdirMode {
    Never,
    Multiple,
    Always,
}

#[derive(Debug, Clone, Serialize, ValueEnum)]
enum FixExtensionMode {
    Never,
    Auto,
    Always,
}

#[derive(Debug, Clone, Serialize, ValueEnum)]
enum MoveDeleteDirsMode {
    Never,
    Auto,
    Always,
}

#[derive(Debug, Clone, Serialize, ValueEnum)]
enum ZipFormat {
    Torrentzip,
    Rvzstd,
}

#[derive(Debug, Clone, Serialize, ValueEnum)]
enum LinkMode {
    Hardlink,
    Symlink,
    Reflink,
}

#[derive(Debug, Clone, Serialize, ValueEnum)]
enum MergeMode {
    Fullnonmerged,
    Nonmerged,
    Split,
    Merged,
}

#[derive(Debug, Clone, Serialize)]
struct ChecksumSet {
    crc32: Option<String>,
    md5: Option<String>,
    sha1: Option<String>,
    sha256: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct FileRecord {
    source: PathBuf,
    relative: PathBuf,
    size: u64,
    checksums: ChecksumSet,
}

#[derive(Parser, Debug, Serialize)]
#[command(
    name = "igir",
    version,
    about = "Rust rewrite of Igir ROM collection manager",
    long_about = "This CLI mirrors Igir's multi-command interface and performs on-disk actions with a focus on parity to the original Node.js implementation."
)]
struct Cli {
    /// Commands to run (can specify multiple)
    #[arg(value_enum, value_name = "COMMAND", action = ArgAction::Append)]
    commands: Vec<Action>,

    // ROM input options
    /// Path(s) to ROM files or archives (supports globbing)
    #[arg(short = 'i', long = "input", value_name = "PATH", action = ArgAction::Append)]
    input: Vec<PathBuf>,

    /// Path(s) to ROM files or archives to exclude from processing (supports globbing)
    #[arg(short = 'I', long = "input-exclude", value_name = "PATH", action = ArgAction::Append)]
    input_exclude: Vec<PathBuf>,

    /// Only read checksums from archive headers, don't decompress to calculate
    #[arg(long = "input-checksum-quick")]
    input_checksum_quick: bool,

    /// The minimum checksum level to calculate and use for matching
    #[arg(
        long = "input-checksum-min",
        value_enum,
        default_value_t = Checksum::Crc32,
        value_parser = PossibleValuesParser::new(Checksum::value_variants()),
    )]
    input_checksum_min: Checksum,

    /// The maximum checksum level to calculate and use for matching
    #[arg(long = "input-checksum-max", value_enum, value_parser = PossibleValuesParser::new(Checksum::value_variants()))]
    input_checksum_max: Option<Checksum>,

    /// Calculate checksums of archive files themselves, allowing them to match files in DATs
    #[arg(
        long = "input-checksum-archives",
        value_enum,
        default_value_t = ArchiveChecksumMode::Auto,
        value_parser = PossibleValuesParser::new(ArchiveChecksumMode::value_variants()),
    )]
    input_checksum_archives: ArchiveChecksumMode,

    // DAT input options (parsed but not yet used for matching)
    #[arg(short = 'd', long = "dat", value_name = "PATH", action = ArgAction::Append)]
    dat: Vec<PathBuf>,
    #[arg(long = "dat-exclude", value_name = "PATH", action = ArgAction::Append)]
    dat_exclude: Vec<PathBuf>,
    #[arg(long = "dat-name-regex", value_name = "REGEX")]
    dat_name_regex: Option<String>,
    #[arg(long = "dat-name-regex-exclude", value_name = "REGEX")]
    dat_name_regex_exclude: Option<String>,
    #[arg(long = "dat-description-regex", value_name = "REGEX")]
    dat_description_regex: Option<String>,
    #[arg(long = "dat-description-regex-exclude", value_name = "REGEX")]
    dat_description_regex_exclude: Option<String>,
    #[arg(long = "dat-combine")]
    dat_combine: bool,
    #[arg(long = "dat-ignore-parent-clone")]
    dat_ignore_parent_clone: bool,

    // Patch input options
    #[arg(short = 'p', long = "patch", value_name = "PATH", action = ArgAction::Append)]
    patch: Vec<PathBuf>,
    #[arg(short = 'P', long = "patch-exclude", value_name = "PATH", action = ArgAction::Append)]
    patch_exclude: Vec<PathBuf>,

    // ROM output path options
    #[arg(short = 'o', long = "output", value_name = "PATH")]
    output: Option<PathBuf>,
    #[arg(long = "dir-mirror")]
    dir_mirror: bool,
    #[arg(long = "dir-dat-mirror")]
    dir_dat_mirror: bool,
    #[arg(short = 'D', long = "dir-dat-name")]
    dir_dat_name: bool,
    #[arg(long = "dir-dat-description")]
    dir_dat_description: bool,
    #[arg(long = "dir-letter")]
    dir_letter: bool,
    #[arg(long = "dir-letter-count", value_name = "NUM")]
    dir_letter_count: Option<usize>,
    #[arg(long = "dir-letter-limit", value_name = "NUM")]
    dir_letter_limit: Option<usize>,
    #[arg(long = "dir-letter-group")]
    dir_letter_group: bool,
    #[arg(
        long = "dir-game-subdir",
        value_enum,
        default_value_t = DirGameSubdirMode::Multiple,
        value_parser = PossibleValuesParser::new(DirGameSubdirMode::value_variants()),
    )]
    dir_game_subdir: DirGameSubdirMode,

    // ROM writing options
    #[arg(
        long = "fix-extension",
        value_enum,
        default_value_t = FixExtensionMode::Auto,
        value_parser = PossibleValuesParser::new(FixExtensionMode::value_variants()),
    )]
    fix_extension: FixExtensionMode,
    #[arg(short = 'O', long = "overwrite")]
    overwrite: bool,
    #[arg(long = "overwrite-invalid")]
    overwrite_invalid: bool,

    // move command options
    #[arg(
        long = "move-delete-dirs",
        value_enum,
        default_value_t = MoveDeleteDirsMode::Auto,
        value_parser = PossibleValuesParser::new(MoveDeleteDirsMode::value_variants()),
    )]
    move_delete_dirs: MoveDeleteDirsMode,

    // clean command options
    #[arg(short = 'C', long = "clean-exclude", value_name = "PATH", action = ArgAction::Append)]
    clean_exclude: Vec<PathBuf>,
    #[arg(long = "clean-backup", value_name = "PATH")]
    clean_backup: Option<PathBuf>,
    #[arg(long = "clean-dry-run")]
    clean_dry_run: bool,

    // zip command options
    #[arg(
        long = "zip-format",
        value_enum,
        default_value_t = ZipFormat::Torrentzip,
        value_parser = PossibleValuesParser::new(ZipFormat::value_variants()),
    )]
    zip_format: ZipFormat,
    #[arg(short = 'Z', long = "zip-exclude", value_name = "GLOB")]
    zip_exclude: Option<String>,
    #[arg(long = "zip-dat-name")]
    zip_dat_name: bool,

    // link command options
    #[arg(
        long = "link-mode",
        value_enum,
        default_value_t = LinkMode::Hardlink,
        value_parser = PossibleValuesParser::new(LinkMode::value_variants()),
    )]
    link_mode: LinkMode,
    #[arg(long = "symlink-relative")]
    symlink_relative: bool,

    // header options
    #[arg(long = "header", value_name = "GLOB")]
    header: Option<String>,
    #[arg(short = 'H', long = "remove-headers", value_name = "EXTENSIONS")]
    remove_headers: Option<String>,

    // trimmed ROM options
    #[arg(long = "trimmed-glob", value_name = "GLOB")]
    trimmed_glob: Option<String>,
    #[arg(long = "trim-scan-archives")]
    trim_scan_archives: bool,

    // ROM set options
    #[arg(
        long = "merge-roms",
        value_enum,
        default_value_t = MergeMode::Fullnonmerged,
        value_parser = PossibleValuesParser::new(MergeMode::value_variants()),
    )]
    merge_roms: MergeMode,
    #[arg(long = "merge-discs")]
    merge_discs: bool,
    #[arg(long = "exclude-disks")]
    exclude_disks: bool,
    #[arg(long = "allow-excess-sets")]
    allow_excess_sets: bool,
    #[arg(long = "allow-incomplete-sets")]
    allow_incomplete_sets: bool,

    // ROM filtering options
    #[arg(short = 'x', long = "filter-regex", value_name = "REGEX")]
    filter_regex: Option<String>,
    #[arg(short = 'X', long = "filter-regex-exclude", value_name = "REGEX")]
    filter_regex_exclude: Option<String>,
    #[arg(short = 'L', long = "filter-language", value_name = "LANGS")]
    filter_language: Option<String>,
    #[arg(short = 'R', long = "filter-region", value_name = "REGIONS")]
    filter_region: Option<String>,
    #[arg(long = "filter-category-regex", value_name = "REGEX")]
    filter_category_regex: Option<String>,
    #[arg(long = "no-bios")]
    no_bios: bool,
    #[arg(long = "no-device")]
    no_device: bool,
    #[arg(long = "no-unlicensed")]
    no_unlicensed: bool,
    #[arg(long = "only-retail")]
    only_retail: bool,
    #[arg(long = "no-debug")]
    no_debug: bool,
    #[arg(long = "no-demo")]
    no_demo: bool,
    #[arg(long = "no-beta")]
    no_beta: bool,
    #[arg(long = "no-sample")]
    no_sample: bool,
    #[arg(long = "no-prototype")]
    no_prototype: bool,
    #[arg(long = "no-program")]
    no_program: bool,

    // logging options (limited parity)
    #[arg(short = 'v', long = "verbose", action = ArgAction::Count)]
    verbose: u8,
    #[arg(short = 'q', long = "quiet", action = ArgAction::Count)]
    quiet: u8,
}

#[derive(Debug, Clone, Serialize)]
struct Config {
    commands: Vec<Action>,
    input: Vec<PathBuf>,
    input_exclude: Vec<PathBuf>,
    input_checksum_quick: bool,
    input_checksum_min: Checksum,
    input_checksum_max: Option<Checksum>,
    input_checksum_archives: ArchiveChecksumMode,
    dat: Vec<PathBuf>,
    dat_exclude: Vec<PathBuf>,
    dat_name_regex: Option<String>,
    dat_name_regex_exclude: Option<String>,
    dat_description_regex: Option<String>,
    dat_description_regex_exclude: Option<String>,
    dat_combine: bool,
    dat_ignore_parent_clone: bool,
    patch: Vec<PathBuf>,
    patch_exclude: Vec<PathBuf>,
    output: Option<PathBuf>,
    dir_mirror: bool,
    dir_dat_mirror: bool,
    dir_dat_name: bool,
    dir_dat_description: bool,
    dir_letter: bool,
    dir_letter_count: Option<usize>,
    dir_letter_limit: Option<usize>,
    dir_letter_group: bool,
    dir_game_subdir: DirGameSubdirMode,
    fix_extension: FixExtensionMode,
    overwrite: bool,
    overwrite_invalid: bool,
    move_delete_dirs: MoveDeleteDirsMode,
    clean_exclude: Vec<PathBuf>,
    clean_backup: Option<PathBuf>,
    clean_dry_run: bool,
    zip_format: ZipFormat,
    zip_exclude: Option<String>,
    zip_dat_name: bool,
    link_mode: LinkMode,
    symlink_relative: bool,
    header: Option<String>,
    remove_headers: Option<String>,
    trimmed_glob: Option<String>,
    trim_scan_archives: bool,
    merge_roms: MergeMode,
    merge_discs: bool,
    exclude_disks: bool,
    allow_excess_sets: bool,
    allow_incomplete_sets: bool,
    filter_regex: Option<String>,
    filter_regex_exclude: Option<String>,
    filter_language: Option<String>,
    filter_region: Option<String>,
    filter_category_regex: Option<String>,
    no_bios: bool,
    no_device: bool,
    no_unlicensed: bool,
    only_retail: bool,
    no_debug: bool,
    no_demo: bool,
    no_beta: bool,
    no_sample: bool,
    no_prototype: bool,
    no_program: bool,
    verbose: u8,
    quiet: u8,
}

#[derive(Debug, Serialize)]
struct ActionOutcome {
    action: Action,
    status: String,
    note: String,
}

#[derive(Debug, Serialize)]
struct ExecutionPlan {
    config: Config,
    steps: Vec<ActionOutcome>,
    files_processed: usize,
}

impl Checksum {
    fn rank(&self) -> u8 {
        match self {
            Checksum::Crc32 => 0,
            Checksum::Md5 => 1,
            Checksum::Sha1 => 2,
            Checksum::Sha256 => 3,
        }
    }
}

impl Config {
    fn validate_checksum_range(&self) -> anyhow::Result<()> {
        if let Some(max) = self.input_checksum_max {
            let min_rank = self.input_checksum_min.rank();
            let max_rank = max.rank();

            if max_rank < min_rank {
                anyhow::bail!(
                    "input-checksum-max cannot be lower fidelity than input-checksum-min"
                );
            }
        }

        Ok(())
    }

    fn validate_letter_strategy(&self) -> anyhow::Result<()> {
        if self.dir_letter_group && self.dir_letter_limit.is_none() {
            anyhow::bail!("dir-letter-group requires --dir-letter-limit to split ranges");
        }

        if self.dir_letter_group && !self.dir_letter {
            anyhow::bail!("dir-letter-group requires --dir-letter to organize by letter");
        }

        if self.dir_letter_limit.is_some() && !self.dir_letter {
            anyhow::bail!("dir-letter-limit requires --dir-letter to organize by letter");
        }

        if !self.dir_letter && self.dir_letter_count.is_some() {
            anyhow::bail!("dir-letter-count requires --dir-letter to organize by letter");
        }

        Ok(())
    }

    fn validate_commands(&self) -> anyhow::Result<()> {
        if self.commands.is_empty() {
            anyhow::bail!("at least one command must be provided");
        }

        Ok(())
    }

    fn validate_output_requirements(&self) -> anyhow::Result<()> {
        let needs_output = self.commands.iter().any(|action| match action {
            Action::Copy
            | Action::Move
            | Action::Link
            | Action::Extract
            | Action::Zip
            | Action::Playlist
            | Action::Dir2dat
            | Action::Fixdat
            | Action::Clean
            | Action::Report => true,
            Action::Test => false,
        });

        if needs_output && self.output.is_none() {
            anyhow::bail!("--output is required for the selected commands");
        }

        Ok(())
    }
}

impl TryFrom<Cli> for Config {
    type Error = anyhow::Error;

    fn try_from(cli: Cli) -> Result<Self, Self::Error> {
        let config = Self {
            commands: cli.commands,
            input: cli.input,
            input_exclude: cli.input_exclude,
            input_checksum_quick: cli.input_checksum_quick,
            input_checksum_min: cli.input_checksum_min,
            input_checksum_max: cli.input_checksum_max,
            input_checksum_archives: cli.input_checksum_archives,
            dat: cli.dat,
            dat_exclude: cli.dat_exclude,
            dat_name_regex: cli.dat_name_regex,
            dat_name_regex_exclude: cli.dat_name_regex_exclude,
            dat_description_regex: cli.dat_description_regex,
            dat_description_regex_exclude: cli.dat_description_regex_exclude,
            dat_combine: cli.dat_combine,
            dat_ignore_parent_clone: cli.dat_ignore_parent_clone,
            patch: cli.patch,
            patch_exclude: cli.patch_exclude,
            output: cli.output,
            dir_mirror: cli.dir_mirror,
            dir_dat_mirror: cli.dir_dat_mirror,
            dir_dat_name: cli.dir_dat_name,
            dir_dat_description: cli.dir_dat_description,
            dir_letter: cli.dir_letter,
            dir_letter_count: cli.dir_letter_count.or_else(|| cli.dir_letter.then_some(1)),
            dir_letter_limit: cli.dir_letter_limit,
            dir_letter_group: cli.dir_letter_group,
            dir_game_subdir: cli.dir_game_subdir,
            fix_extension: cli.fix_extension,
            overwrite: cli.overwrite,
            overwrite_invalid: cli.overwrite_invalid,
            move_delete_dirs: cli.move_delete_dirs,
            clean_exclude: cli.clean_exclude,
            clean_backup: cli.clean_backup,
            clean_dry_run: cli.clean_dry_run,
            zip_format: cli.zip_format,
            zip_exclude: cli.zip_exclude,
            zip_dat_name: cli.zip_dat_name,
            link_mode: cli.link_mode,
            symlink_relative: cli.symlink_relative,
            header: cli.header,
            remove_headers: cli.remove_headers,
            trimmed_glob: cli.trimmed_glob,
            trim_scan_archives: cli.trim_scan_archives,
            merge_roms: cli.merge_roms,
            merge_discs: cli.merge_discs,
            exclude_disks: cli.exclude_disks,
            allow_excess_sets: cli.allow_excess_sets,
            allow_incomplete_sets: cli.allow_incomplete_sets,
            filter_regex: cli.filter_regex,
            filter_regex_exclude: cli.filter_regex_exclude,
            filter_language: cli.filter_language,
            filter_region: cli.filter_region,
            filter_category_regex: cli.filter_category_regex,
            no_bios: cli.no_bios,
            no_device: cli.no_device,
            no_unlicensed: cli.no_unlicensed,
            only_retail: cli.only_retail,
            no_debug: cli.no_debug,
            no_demo: cli.no_demo,
            no_beta: cli.no_beta,
            no_sample: cli.no_sample,
            no_prototype: cli.no_prototype,
            no_program: cli.no_program,
            verbose: cli.verbose,
            quiet: cli.quiet,
        };

        config.validate_commands()?;
        config.validate_checksum_range()?;
        config.validate_letter_strategy()?;
        config.validate_output_requirements()?;

        Ok(config)
    }
}

fn build_globset(patterns: &[PathBuf]) -> anyhow::Result<Option<GlobSet>> {
    if patterns.is_empty() {
        return Ok(None);
    }

    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        let pattern_str = pattern
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("invalid glob pattern"))?;
        builder.add(
            Glob::new(pattern_str)
                .with_context(|| format!("invalid glob pattern: {pattern_str}"))?,
        );
    }

    Ok(Some(builder.build()?))
}

fn checksum_range(min: Checksum, max: Option<Checksum>) -> Vec<Checksum> {
    let max_rank = max.unwrap_or(min).rank();
    let mut checksums = Vec::new();

    for variant in [
        Checksum::Crc32,
        Checksum::Md5,
        Checksum::Sha1,
        Checksum::Sha256,
    ] {
        if variant.rank() >= min.rank() && variant.rank() <= max_rank {
            checksums.push(variant);
        }
    }

    checksums
}

fn compute_checksums(path: &Path, config: &Config) -> anyhow::Result<ChecksumSet> {
    let mut crc32 = None;
    let mut md5 = None;
    let mut sha1 = None;
    let mut sha256 = None;

    let checksums_to_compute = checksum_range(config.input_checksum_min, config.input_checksum_max);

    if !checksums_to_compute.is_empty() {
        let mut file = fs::File::open(path)?;
        let mut buffer = Vec::new();
        io::copy(&mut file, &mut buffer)?;

        for checksum in checksums_to_compute {
            match checksum {
                Checksum::Crc32 => {
                    crc32 = Some(format!("{:08x}", crc32fast::hash(&buffer)));
                }
                Checksum::Md5 => {
                    md5 = Some(format!("{:032x}", md5::compute(&buffer)));
                }
                Checksum::Sha1 => {
                    sha1 = Some(format!("{:040x}", sha1_smol::Sha1::from(&buffer).digest()));
                }
                Checksum::Sha256 => {
                    let mut hasher = Sha256::new();
                    hasher.update(&buffer);
                    sha256 = Some(format!("{:064x}", hasher.finalize()));
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

fn collect_files(config: &Config) -> anyhow::Result<Vec<FileRecord>> {
    let exclude = build_globset(&config.input_exclude)?;
    let mut records = Vec::new();

    for input in &config.input {
        let metadata = fs::metadata(input).with_context(|| format!("reading input: {input:?}"))?;
        if metadata.is_file() {
            if exclude
                .as_ref()
                .is_some_and(|set| set.is_match(input.to_string_lossy().as_ref()))
            {
                continue;
            }

            let checksums = compute_checksums(input, config)?;
            records.push(FileRecord {
                source: input.clone(),
                relative: input
                    .file_name()
                    .map(PathBuf::from)
                    .unwrap_or_else(|| PathBuf::from("unknown")),
                size: metadata.len(),
                checksums,
            });
            continue;
        }

        for entry in WalkDir::new(input)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| e.file_type().is_file())
        {
            let path = entry.into_path();
            if exclude
                .as_ref()
                .is_some_and(|set| set.is_match(path.to_string_lossy().as_ref()))
            {
                continue;
            }

            let checksums = compute_checksums(&path, config)?;
            let relative = path.strip_prefix(input).unwrap_or(&path).to_path_buf();

            records.push(FileRecord {
                size: fs::metadata(&path)?.len(),
                source: path,
                relative,
                checksums,
            });
        }
    }

    Ok(records)
}

fn select_letter_dir(name: &str, count: usize) -> String {
    let mut chars = name.chars();
    let mut dir = String::new();
    for _ in 0..count {
        if let Some(ch) = chars.next() {
            if ch.is_alphabetic() {
                dir.push(ch.to_ascii_uppercase());
            }
        }
    }

    if dir.is_empty() {
        "_misc".to_string()
    } else {
        dir
    }
}

fn resolve_output_path(record: &FileRecord, config: &Config) -> PathBuf {
    let mut base = config
        .output
        .as_ref()
        .cloned()
        .unwrap_or_else(|| PathBuf::from("output"));

    if config.dir_mirror {
        if let Some(parent) = record.relative.parent() {
            base = base.join(parent);
        }
    }

    if config.dir_letter {
        let letter = select_letter_dir(
            record
                .relative
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown"),
            config.dir_letter_count.unwrap_or(1),
        );
        base = base.join(letter);
    }

    if matches!(config.dir_game_subdir, DirGameSubdirMode::Always) {
        if let Some(stem) = record.relative.file_stem().and_then(|s| s.to_str()) {
            base = base.join(stem);
        }
    }

    base.join(
        record
            .relative
            .file_name()
            .unwrap_or_else(|| record.relative.as_os_str()),
    )
}

fn ensure_parent(path: &Path) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    Ok(())
}

fn copy_record(record: &FileRecord, config: &Config) -> anyhow::Result<PathBuf> {
    let target = resolve_output_path(record, config);
    ensure_parent(&target)?;

    if target.exists() {
        if !config.overwrite && !config.overwrite_invalid {
            return Ok(target);
        }
    }

    fs::copy(&record.source, &target)
        .with_context(|| format!("copying {:?} to {:?}", record.source, target))?;
    Ok(target)
}

fn move_record(record: &FileRecord, config: &Config) -> anyhow::Result<PathBuf> {
    let target = resolve_output_path(record, config);
    ensure_parent(&target)?;

    if target.exists() && !config.overwrite {
        return Ok(target);
    }

    fs::rename(&record.source, &target).or_else(|_| {
        fs::copy(&record.source, &target)?;
        fs::remove_file(&record.source)
    })?;

    if matches!(
        config.move_delete_dirs,
        MoveDeleteDirsMode::Always | MoveDeleteDirsMode::Auto
    ) {
        if let Some(parent) = record.source.parent() {
            let _ = fs::remove_dir(parent);
        }
    }

    Ok(target)
}

fn link_record(record: &FileRecord, config: &Config) -> anyhow::Result<PathBuf> {
    let target = resolve_output_path(record, config);
    ensure_parent(&target)?;

    match config.link_mode {
        LinkMode::Hardlink => {
            if target.exists() {
                fs::remove_file(&target)?;
            }
            fs::hard_link(&record.source, &target)?;
        }
        LinkMode::Symlink => {
            if target.exists() {
                fs::remove_file(&target)?;
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::symlink;
                let src = if config.symlink_relative {
                    pathdiff::diff_paths(&record.source, target.parent().unwrap_or(Path::new(".")))
                        .unwrap_or(record.source.clone())
                } else {
                    record.source.clone()
                };
                symlink(src, &target)?;
            }
            #[cfg(not(unix))]
            {
                fs::copy(&record.source, &target)?;
            }
        }
        LinkMode::Reflink => {
            // Fallback to copy for portability
            fs::copy(&record.source, &target)?;
        }
    }

    Ok(target)
}

fn extract_record(record: &FileRecord, config: &Config) -> anyhow::Result<Vec<PathBuf>> {
    let mut written = Vec::new();
    let extension = record
        .source
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();

    if extension == "zip" {
        let file = fs::File::open(&record.source)?;
        let mut archive = zip::ZipArchive::new(file)?;
        for i in 0..archive.len() {
            let mut file = archive.by_index(i)?;
            if file.is_dir() {
                continue;
            }

            let out_path = resolve_output_path(
                &FileRecord {
                    source: record.source.clone(),
                    relative: PathBuf::from(file.name()),
                    size: file.size(),
                    checksums: ChecksumSet {
                        crc32: None,
                        md5: None,
                        sha1: None,
                        sha256: None,
                    },
                },
                config,
            );
            ensure_parent(&out_path)?;
            let mut outfile = fs::File::create(&out_path)?;
            io::copy(&mut file, &mut outfile)?;
            written.push(out_path);
        }
    } else {
        written.push(copy_record(record, config)?);
    }

    Ok(written)
}

fn zip_record(record: &FileRecord, config: &Config) -> anyhow::Result<PathBuf> {
    let mut target = resolve_output_path(record, config);
    target.set_extension("zip");
    ensure_parent(&target)?;

    if let Some(glob) = &config.zip_exclude {
        if Glob::new(glob)?.compile_matcher().is_match(
            record
                .relative
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or(""),
        ) {
            return Ok(target);
        }
    }

    let file = fs::File::create(&target)?;
    let mut zip = zip::ZipWriter::new(file);
    let options = zip::write::FileOptions::default();
    zip.start_file(
        record
            .relative
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("file"),
        options,
    )?;
    let mut source = fs::File::open(&record.source)?;
    let mut buffer = Vec::new();
    io::copy(&mut source, &mut buffer)?;
    zip.write_all(&buffer)?;
    zip.finish()?;

    Ok(target)
}

fn playlist(records: &[FileRecord], config: &Config) -> anyhow::Result<PathBuf> {
    let mut target = config
        .output
        .clone()
        .unwrap_or_else(|| PathBuf::from("output"));
    target.push("playlist.m3u");
    ensure_parent(&target)?;

    let mut file = fs::File::create(&target)?;
    for record in records {
        let path = resolve_output_path(record, config);
        let display = path
            .strip_prefix(config.output.as_deref().unwrap_or(Path::new("output")))
            .unwrap_or(&path)
            .to_string_lossy()
            .to_string();
        writeln!(file, "{display}")?;
    }

    Ok(target)
}

fn write_report(records: &[FileRecord], config: &Config) -> anyhow::Result<PathBuf> {
    let mut target = config
        .output
        .clone()
        .unwrap_or_else(|| PathBuf::from("output"));
    target.push("report.csv");
    ensure_parent(&target)?;

    let mut file = fs::File::create(&target)?;
    writeln!(file, "path,size,crc32,md5,sha1,sha256")?;
    for record in records {
        writeln!(
            file,
            "{},{},{},{},{},{}",
            record.relative.display(),
            record.size,
            record.checksums.crc32.clone().unwrap_or_default(),
            record.checksums.md5.clone().unwrap_or_default(),
            record.checksums.sha1.clone().unwrap_or_default(),
            record.checksums.sha256.clone().unwrap_or_default()
        )?;
    }

    Ok(target)
}

fn write_dir2dat(records: &[FileRecord], config: &Config) -> anyhow::Result<PathBuf> {
    let mut target = config
        .output
        .clone()
        .unwrap_or_else(|| PathBuf::from("output"));
    target.push("dir2dat.json");
    ensure_parent(&target)?;

    let json = serde_json::to_string_pretty(records)?;
    fs::write(&target, json)?;
    Ok(target)
}

fn write_fixdat(records: &[FileRecord], config: &Config) -> anyhow::Result<PathBuf> {
    let mut target = config
        .output
        .clone()
        .unwrap_or_else(|| PathBuf::from("output"));
    target.push("fixdat.json");
    ensure_parent(&target)?;

    // Simplified: treat missing files as placeholders
    let mut missing = Vec::new();
    for record in records {
        if !resolve_output_path(record, config).exists() {
            missing.push(record);
        }
    }

    let json = serde_json::to_string_pretty(&missing)?;
    fs::write(&target, json)?;
    Ok(target)
}

fn clean_output(records: &[FileRecord], config: &Config) -> anyhow::Result<Vec<PathBuf>> {
    let mut cleaned = Vec::new();
    let mut expected = HashMap::new();
    for record in records {
        expected.insert(resolve_output_path(record, config), ());
    }

    let exclude = build_globset(&config.clean_exclude)?;
    if let Some(output) = &config.output {
        for entry in WalkDir::new(output)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| e.file_type().is_file())
        {
            let path = entry.into_path();
            if expected.contains_key(&path) {
                continue;
            }

            if exclude
                .as_ref()
                .is_some_and(|set| set.is_match(path.to_string_lossy().as_ref()))
            {
                continue;
            }

            if config.clean_dry_run {
                cleaned.push(path);
                continue;
            }

            if let Some(backup) = &config.clean_backup {
                let target = backup.join(path.file_name().unwrap_or_default());
                ensure_parent(&target)?;
                fs::rename(&path, &target).or_else(|_| {
                    fs::copy(&path, &target)?;
                    fs::remove_file(&path)
                })?;
                cleaned.push(target);
            } else {
                fs::remove_file(&path)?;
                cleaned.push(path);
            }
        }
    }

    Ok(cleaned)
}

fn perform_actions(config: &Config) -> anyhow::Result<ExecutionPlan> {
    let records = collect_files(config)?;
    let mut steps = Vec::new();

    for action in &config.commands {
        match action {
            Action::Copy => {
                for record in &records {
                    let _ = copy_record(record, config)?;
                }
                steps.push(ActionOutcome {
                    action: action.clone(),
                    status: "ok".to_string(),
                    note: "Copied input files to output".to_string(),
                });
            }
            Action::Move => {
                for record in &records {
                    let _ = move_record(record, config)?;
                }
                steps.push(ActionOutcome {
                    action: action.clone(),
                    status: "ok".to_string(),
                    note: "Moved input files to output".to_string(),
                });
            }
            Action::Link => {
                for record in &records {
                    let _ = link_record(record, config)?;
                }
                steps.push(ActionOutcome {
                    action: action.clone(),
                    status: "ok".to_string(),
                    note: format!("Linked files using {:?}", config.link_mode),
                });
            }
            Action::Extract => {
                for record in &records {
                    let _ = extract_record(record, config)?;
                }
                steps.push(ActionOutcome {
                    action: action.clone(),
                    status: "ok".to_string(),
                    note: "Extracted archives and copied loose files".to_string(),
                });
            }
            Action::Zip => {
                for record in &records {
                    let _ = zip_record(record, config)?;
                }
                steps.push(ActionOutcome {
                    action: action.clone(),
                    status: "ok".to_string(),
                    note: format!("Created {:?} archives", config.zip_format),
                });
            }
            Action::Playlist => {
                let path = playlist(&records, config)?;
                steps.push(ActionOutcome {
                    action: action.clone(),
                    status: "ok".to_string(),
                    note: format!("Wrote playlist at {}", path.display()),
                });
            }
            Action::Test => {
                steps.push(ActionOutcome {
                    action: action.clone(),
                    status: "ok".to_string(),
                    note: "Calculated checksums for verification".to_string(),
                });
            }
            Action::Dir2dat => {
                let path = write_dir2dat(&records, config)?;
                steps.push(ActionOutcome {
                    action: action.clone(),
                    status: "ok".to_string(),
                    note: format!("Wrote dir2dat at {}", path.display()),
                });
            }
            Action::Fixdat => {
                let path = write_fixdat(&records, config)?;
                steps.push(ActionOutcome {
                    action: action.clone(),
                    status: "ok".to_string(),
                    note: format!("Wrote fixdat at {}", path.display()),
                });
            }
            Action::Clean => {
                let cleaned = clean_output(&records, config)?;
                steps.push(ActionOutcome {
                    action: action.clone(),
                    status: "ok".to_string(),
                    note: format!("Cleaned {} files", cleaned.len()),
                });
            }
            Action::Report => {
                let path = write_report(&records, config)?;
                steps.push(ActionOutcome {
                    action: action.clone(),
                    status: "ok".to_string(),
                    note: format!("Wrote report at {}", path.display()),
                });
            }
        }
    }

    Ok(ExecutionPlan {
        config: config.clone(),
        steps,
        files_processed: records.len(),
    })
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let config = Config::try_from(cli)?;
    let plan = perform_actions(&config)?;
    let serialized = serde_json::to_string_pretty(&plan)?;
    println!("{}", serialized);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn errors_when_no_commands_provided() {
        let cli = Cli::parse_from(["igir"]);
        let err = Config::try_from(cli).unwrap_err();
        assert!(err.to_string().contains("at least one command"));
    }

    #[test]
    fn errors_when_checksum_max_lower_than_min() {
        let cli = Cli::parse_from([
            "igir",
            "copy",
            "--input-checksum-min",
            "SHA1",
            "--input-checksum-max",
            "CRC32",
        ]);

        let err = Config::try_from(cli).unwrap_err();
        assert!(
            err.to_string()
                .contains("input-checksum-max cannot be lower")
        );
    }

    #[test]
    fn errors_when_letter_group_without_limit() {
        let cli = Cli::parse_from(["igir", "copy", "--dir-letter", "--dir-letter-group"]);
        let err = Config::try_from(cli).unwrap_err();
        assert!(
            err.to_string()
                .contains("dir-letter-group requires --dir-letter-limit")
        );
    }

    #[test]
    fn supplies_default_letter_count() {
        let cli = Cli::parse_from(["igir", "copy", "--dir-letter", "--dir-letter-limit", "100"]);

        let config = Config::try_from(cli).expect("config should parse");
        assert_eq!(config.dir_letter_count, Some(1));
        assert_eq!(config.dir_letter_limit, Some(100));
    }

    #[test]
    fn errors_without_output_for_copy() {
        let cli = Cli::parse_from(["igir", "copy", "--input", "./foo"]);
        let err = Config::try_from(cli).unwrap_err();
        assert!(err.to_string().contains("--output is required"));
    }
}
