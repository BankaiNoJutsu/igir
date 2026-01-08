use std::env;
use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::anyhow;
use reqwest::blocking::Client;
use serde::Deserialize;

use crate::{
    cli::Cli,
    types::{
        Action, ArchiveChecksumMode, Checksum, DirGameSubdirMode, FileRecord, FixExtensionMode,
        IgdbLookupMode, LinkMode, MergeMode, MoveDeleteDirsMode, ZipFormat,
    },
};

#[derive(Debug, Clone, serde::Serialize)]
pub struct Config {
    pub commands: Vec<Action>,
    pub input: Vec<PathBuf>,
    pub input_exclude: Vec<PathBuf>,
    pub input_checksum_quick: bool,
    pub input_checksum_min: Checksum,
    pub input_checksum_max: Option<Checksum>,
    pub input_checksum_archives: ArchiveChecksumMode,
    pub dat: Vec<PathBuf>,
    pub dat_exclude: Vec<PathBuf>,
    pub dat_name_regex: Option<String>,
    pub dat_name_regex_exclude: Option<String>,
    pub dat_description_regex: Option<String>,
    pub dat_description_regex_exclude: Option<String>,
    pub dat_combine: bool,
    pub dat_ignore_parent_clone: bool,
    pub list_unmatched_dats: bool,
    pub print_plan: bool,
    pub enable_hasheous: bool,
    pub igdb_client_id: Option<String>,
    #[serde(skip_serializing)]
    pub igdb_client_secret: Option<String>,
    pub igdb_token: Option<String>,
    pub igdb_token_expires_at: Option<i64>,
    pub igdb_mode: IgdbLookupMode,
    pub patch: Vec<PathBuf>,
    pub patch_exclude: Vec<PathBuf>,
    pub output: Option<PathBuf>,
    pub dir_mirror: bool,
    pub dir_dat_mirror: bool,
    pub dir_dat_name: bool,
    pub dir_dat_description: bool,
    pub dir_letter: bool,
    pub dir_letter_count: Option<usize>,
    pub dir_letter_limit: Option<usize>,
    pub dir_letter_group: bool,
    pub dir_game_subdir: DirGameSubdirMode,
    pub fix_extension: FixExtensionMode,
    pub overwrite: bool,
    pub overwrite_invalid: bool,
    pub move_delete_dirs: MoveDeleteDirsMode,
    pub clean_exclude: Vec<PathBuf>,
    pub clean_backup: Option<PathBuf>,
    pub clean_dry_run: bool,
    pub zip_format: ZipFormat,
    pub zip_exclude: Option<String>,
    pub zip_dat_name: bool,
    pub link_mode: LinkMode,
    pub symlink_relative: bool,
    pub header: Option<String>,
    pub remove_headers: Option<String>,
    pub trimmed_glob: Option<String>,
    pub trim_scan_archives: bool,
    pub merge_roms: MergeMode,
    pub merge_discs: bool,
    pub exclude_disks: bool,
    pub allow_excess_sets: bool,
    pub allow_incomplete_sets: bool,
    pub filter_regex: Option<String>,
    pub filter_regex_exclude: Option<String>,
    pub filter_language: Option<String>,
    pub filter_region: Option<String>,
    pub filter_category_regex: Option<String>,
    pub no_bios: bool,
    pub no_device: bool,
    pub no_unlicensed: bool,
    pub only_retail: bool,
    pub no_debug: bool,
    pub no_demo: bool,
    pub no_beta: bool,
    pub no_sample: bool,
    pub no_prototype: bool,
    pub no_program: bool,
    pub verbose: u8,
    pub quiet: u8,
    pub diag: bool,
    pub show_match_reasons: bool,
    pub scan_threads: Option<usize>,
    // Online lookup tuning
    pub online_timeout_secs: Option<u64>,
    pub online_max_retries: Option<usize>,
    pub online_throttle_ms: Option<u64>,
    // Cache options
    pub cache_only: bool,
    // Optional explicit cache DB path
    pub cache_db: Option<PathBuf>,
    pub hash_threads: Option<usize>,
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

        if let Some(limit) = self.dir_letter_limit {
            if limit == 0 {
                anyhow::bail!("dir-letter-limit must be greater than zero");
            }
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

    pub fn validate(&self) -> anyhow::Result<()> {
        self.validate_commands()?;
        self.validate_checksum_range()?;
        self.validate_letter_strategy()?;
        self.validate_output_requirements()?;
        // Validate CLI-provided hash thread count (if any)
        if let Some(n) = self.hash_threads {
            if n == 0 {
                anyhow::bail!("--hash-threads must be >= 1");
            }
        }
        if let Some(n) = self.scan_threads {
            if n == 0 {
                anyhow::bail!("--scan-threads must be >= 1");
            }
        }
        Ok(())
    }

    pub fn igdb_lookup_enabled(&self) -> bool {
        !matches!(self.igdb_mode, IgdbLookupMode::Off)
    }

    pub fn igdb_client_configured(&self) -> bool {
        self.igdb_lookup_enabled() && self.igdb_client_id.is_some()
    }

    pub fn igdb_network_enabled(&self) -> bool {
        self.igdb_lookup_enabled() && self.igdb_client_id.is_some() && self.igdb_token.is_some()
    }

    pub fn should_attempt_igdb_lookup(&self, record: &FileRecord) -> bool {
        match self.igdb_mode {
            IgdbLookupMode::Off => false,
            IgdbLookupMode::BestEffort => record.derived_platform.is_none(),
            IgdbLookupMode::Always => record.derived_genres.is_empty(),
        }
    }

    fn igdb_token_is_valid(&self, token_from_cli: bool) -> bool {
        if self.igdb_client_id.is_none() {
            return true;
        }
        let Some(token) = self.igdb_token.as_ref() else {
            return false;
        };
        let _ = token; // suppress unused warning when no expiry metadata
        match self.igdb_token_expires_at {
            Some(expires_at) => {
                let now = chrono::Utc::now().timestamp();
                expires_at.saturating_sub(now) > 60
            }
            None => self.igdb_client_secret.is_none() || token_from_cli,
        }
    }

    fn refresh_igdb_token_if_needed(&mut self, token_from_cli: bool) -> anyhow::Result<()> {
        if self.cache_only || !self.igdb_lookup_enabled() {
            return Ok(());
        }
        if self.igdb_client_id.is_none() {
            return Ok(());
        }
        if self.igdb_token_is_valid(token_from_cli) {
            return Ok(());
        }
        let client_id = self
            .igdb_client_id
            .as_ref()
            .expect("client_id checked above");
        let client_secret = self
            .igdb_client_secret
            .as_ref()
            .ok_or_else(|| anyhow!(
                "IGDB token is missing or expired. Provide --igdb-client-secret or refresh the token manually with --igdb-token."
            ))?;

        let (token, expires_at) = request_new_igdb_token(client_id, client_secret)?;
        self.igdb_token = Some(token);
        self.igdb_token_expires_at = Some(expires_at);
        Ok(())
    }

    fn persist_igdb_creds(&self) -> anyhow::Result<()> {
        if self.igdb_client_id.is_none()
            && self.igdb_client_secret.is_none()
            && self.igdb_token.is_none()
        {
            return Ok(());
        }
        let path = persisted_config_path()?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let obj = serde_json::json!({
            "igdb_client_id": self.igdb_client_id.clone(),
            "igdb_client_secret": self.igdb_client_secret.clone(),
            "igdb_token": self.igdb_token.clone(),
            "igdb_token_expires_at": self.igdb_token_expires_at,
        });
        fs::write(path, serde_json::to_string_pretty(&obj)?)?;
        Ok(())
    }
}

impl TryFrom<Cli> for Config {
    type Error = anyhow::Error;

    fn try_from(cli: Cli) -> Result<Self, Self::Error> {
        // Attempt to load persisted credentials from disk. CLI flags override persisted values.
        let mut persisted_client_id: Option<String> = None;
        let mut persisted_client_secret: Option<String> = None;
        let mut persisted_token: Option<String> = None;
        let mut persisted_token_expires_at: Option<i64> = None;
        let mut loaded_persisted = false;
        if let Ok(cfg_path) = persisted_config_path() {
            if cfg_path.exists() {
                if let Ok(s) = fs::read_to_string(&cfg_path) {
                    if let Ok(j) = serde_json::from_str::<serde_json::Value>(&s) {
                        loaded_persisted = true;
                        persisted_client_id = j
                            .get("igdb_client_id")
                            .and_then(|v| v.as_str().map(|s| s.to_string()));
                        persisted_client_secret = j
                            .get("igdb_client_secret")
                            .and_then(|v| v.as_str().map(|s| s.to_string()));
                        persisted_token = j
                            .get("igdb_token")
                            .and_then(|v| v.as_str().map(|s| s.to_string()));
                        persisted_token_expires_at =
                            j.get("igdb_token_expires_at").and_then(|v| v.as_i64());
                    }
                }
            }
        }

        // Effective credentials: CLI flags take precedence over persisted values
        let effective_client_id = cli.igdb_client_id.clone().or(persisted_client_id);
        let effective_client_secret = cli.igdb_client_secret.clone().or(persisted_client_secret);
        let token_from_cli = cli.igdb_token.is_some();
        let effective_token = cli.igdb_token.clone().or(persisted_token);
        let effective_token_expires_at = if token_from_cli {
            None
        } else {
            persisted_token_expires_at
        };

        let mut config = Self {
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
            list_unmatched_dats: cli.list_unmatched_dats,
            print_plan: cli.print_plan,
            enable_hasheous: cli.enable_hasheous,
            igdb_client_id: effective_client_id.clone(),
            igdb_client_secret: effective_client_secret.clone(),
            igdb_token: effective_token.clone(),
            igdb_token_expires_at: effective_token_expires_at,
            igdb_mode: cli.igdb_mode,
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
            diag: cli.diag,
            online_timeout_secs: Some(5),
            online_max_retries: Some(3),
            online_throttle_ms: None,
            cache_only: cli.cache_only,
            cache_db: cli.cache_db,
            hash_threads: cli.hash_threads,
            scan_threads: cli.scan_threads,
            show_match_reasons: cli.show_match_reasons,
        };

        config.refresh_igdb_token_if_needed(token_from_cli)?;

        if cli.save_igdb_creds || loaded_persisted {
            if let Err(err) = config.persist_igdb_creds() {
                if config.verbose > 0 {
                    eprintln!("warning: unable to persist IGDB credentials: {}", err);
                }
            }
        }

        config.validate()?;

        Ok(config)
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            commands: vec![],
            input: vec![],
            input_exclude: vec![],
            input_checksum_quick: false,
            input_checksum_min: crate::types::Checksum::Crc32,
            input_checksum_max: None,
            input_checksum_archives: crate::types::ArchiveChecksumMode::Auto,
            dat: vec![],
            dat_exclude: vec![],
            dat_name_regex: None,
            dat_name_regex_exclude: None,
            dat_description_regex: None,
            dat_description_regex_exclude: None,
            dat_combine: false,
            dat_ignore_parent_clone: false,
            list_unmatched_dats: false,
            print_plan: true,
            enable_hasheous: false,
            igdb_client_id: None,
            igdb_client_secret: None,
            igdb_token: None,
            igdb_token_expires_at: None,
            igdb_mode: crate::types::IgdbLookupMode::BestEffort,
            patch: vec![],
            patch_exclude: vec![],
            output: None,
            dir_mirror: false,
            dir_dat_mirror: false,
            dir_dat_name: false,
            dir_dat_description: false,
            dir_letter: false,
            dir_letter_count: None,
            dir_letter_limit: None,
            dir_letter_group: false,
            dir_game_subdir: crate::types::DirGameSubdirMode::Multiple,
            fix_extension: crate::types::FixExtensionMode::Auto,
            overwrite: false,
            overwrite_invalid: false,
            move_delete_dirs: crate::types::MoveDeleteDirsMode::Auto,
            clean_exclude: vec![],
            clean_backup: None,
            clean_dry_run: false,
            zip_format: crate::types::ZipFormat::Torrentzip,
            zip_exclude: None,
            zip_dat_name: false,
            link_mode: crate::types::LinkMode::Hardlink,
            symlink_relative: false,
            header: None,
            remove_headers: None,
            trimmed_glob: None,
            trim_scan_archives: false,
            merge_roms: crate::types::MergeMode::Fullnonmerged,
            merge_discs: false,
            exclude_disks: false,
            allow_excess_sets: false,
            allow_incomplete_sets: false,
            filter_regex: None,
            filter_regex_exclude: None,
            filter_language: None,
            filter_region: None,
            filter_category_regex: None,
            no_bios: false,
            no_device: false,
            no_unlicensed: false,
            only_retail: false,
            no_debug: false,
            no_demo: false,
            no_beta: false,
            no_sample: false,
            no_prototype: false,
            no_program: false,
            verbose: 0,
            quiet: 0,
            diag: false,
            show_match_reasons: false,
            online_timeout_secs: Some(5),
            online_max_retries: Some(3),
            online_throttle_ms: None,
            cache_only: false,
            cache_db: None,
            hash_threads: None,
            scan_threads: None,
        }
    }
}

// Determine a platform-appropriate path to store persistent config (e.g., IGDB creds)
fn persisted_config_path() -> anyhow::Result<PathBuf> {
    // Allow an explicit override for tests or user preference
    if let Ok(dir) = env::var("IGIR_CONFIG_DIR") {
        let mut p = PathBuf::from(dir);
        p.push("config.json");
        return Ok(p);
    }

    if let Ok(path) = env::var("IGIR_CONFIG_PATH") {
        return Ok(PathBuf::from(path));
    }

    // Default: save next to the running binary (in the binary's folder)
    let exe = env::current_exe()?;
    if let Some(parent) = exe.parent() {
        let mut p = PathBuf::from(parent);
        p.push("config.json");
        return Ok(p);
    }

    anyhow::bail!("unable to determine a config path for persisted credentials")
}

#[derive(Deserialize)]
struct IgdbTokenResponse {
    access_token: String,
    expires_in: i64,
}

fn request_new_igdb_token(client_id: &str, client_secret: &str) -> anyhow::Result<(String, i64)> {
    let base =
        env::var("IGDB_TOKEN_BASE").unwrap_or_else(|_| "https://id.twitch.tv/oauth2".to_string());
    let url = format!("{}/token", base.trim_end_matches('/'));
    let client = Client::builder().timeout(Duration::from_secs(10)).build()?;
    let resp = client
        .post(url)
        .form(&[
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("grant_type", "client_credentials"),
        ])
        .send()?;
    let resp = resp.error_for_status()?;
    let parsed: IgdbTokenResponse = resp.json()?;
    let now = chrono::Utc::now().timestamp();
    let expires_at = now + parsed.expires_in.saturating_sub(60).max(0);
    Ok((parsed.access_token, expires_at))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::Cli;
    use crate::types::{ChecksumSet, FileRecord};

    fn make_cli(print_plan: bool) -> Cli {
        Cli {
            commands: vec![Action::Test],
            input: vec![PathBuf::from("/tmp/file.bin")],
            input_exclude: vec![],
            input_checksum_quick: false,
            input_checksum_min: Checksum::Crc32,
            input_checksum_max: None,
            input_checksum_archives: ArchiveChecksumMode::Auto,
            dat: vec![],
            dat_exclude: vec![],
            dat_name_regex: None,
            dat_name_regex_exclude: None,
            dat_description_regex: None,
            dat_description_regex_exclude: None,
            dat_combine: false,
            dat_ignore_parent_clone: false,
            list_unmatched_dats: false,
            enable_hasheous: false,
            igdb_client_id: None,
            igdb_client_secret: None,
            igdb_token: None,
            igdb_mode: IgdbLookupMode::BestEffort,
            patch: vec![],
            patch_exclude: vec![],
            output: None,
            dir_mirror: false,
            dir_dat_mirror: false,
            dir_dat_name: false,
            dir_dat_description: false,
            dir_letter: false,
            dir_letter_count: None,
            dir_letter_limit: None,
            dir_letter_group: false,
            dir_game_subdir: DirGameSubdirMode::Multiple,
            fix_extension: FixExtensionMode::Auto,
            overwrite: false,
            overwrite_invalid: false,
            move_delete_dirs: MoveDeleteDirsMode::Auto,
            clean_exclude: vec![],
            clean_backup: None,
            clean_dry_run: false,
            zip_format: ZipFormat::Torrentzip,
            zip_exclude: None,
            zip_dat_name: false,
            link_mode: LinkMode::Hardlink,
            symlink_relative: false,
            header: None,
            remove_headers: None,
            trimmed_glob: None,
            trim_scan_archives: false,
            merge_roms: MergeMode::Fullnonmerged,
            merge_discs: false,
            exclude_disks: false,
            allow_excess_sets: false,
            allow_incomplete_sets: false,
            filter_regex: None,
            filter_regex_exclude: None,
            filter_language: None,
            filter_region: None,
            filter_category_regex: None,
            no_bios: false,
            no_device: false,
            no_unlicensed: false,
            only_retail: false,
            no_debug: false,
            no_demo: false,
            no_beta: false,
            no_sample: false,
            no_prototype: false,
            no_program: false,
            verbose: 0,
            quiet: 0,
            diag: false,
            cache_only: false,
            cache_db: None,
            hash_threads: None,
            scan_threads: None,
            show_match_reasons: false,
            save_igdb_creds: false,
            print_plan,
        }
    }

    fn sample_record() -> FileRecord {
        FileRecord {
            source: PathBuf::from("test.bin"),
            relative: PathBuf::from("test.bin"),
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
        }
    }

    #[test]
    fn igdb_lookup_mode_controls_behavior() {
        let cli = make_cli(false);
        let mut config = Config::try_from(cli).expect("valid config");
        let mut record = sample_record();

        config.igdb_mode = IgdbLookupMode::Off;
        assert!(!config.should_attempt_igdb_lookup(&record));

        config.igdb_mode = IgdbLookupMode::BestEffort;
        assert!(config.should_attempt_igdb_lookup(&record));
        record.derived_platform = Some("snes".to_string());
        assert!(!config.should_attempt_igdb_lookup(&record));

        config.igdb_mode = IgdbLookupMode::Always;
        record.derived_genres.clear();
        record.derived_platform = Some("gba".to_string());
        assert!(config.should_attempt_igdb_lookup(&record));
        record.derived_genres.push("RPG".to_string());
        assert!(!config.should_attempt_igdb_lookup(&record));
    }

    #[test]
    fn errors_when_no_commands_provided() {
        let cli = Cli {
            commands: vec![],
            input: vec![],
            input_exclude: vec![],
            input_checksum_quick: false,
            input_checksum_min: Checksum::Crc32,
            input_checksum_max: None,
            input_checksum_archives: ArchiveChecksumMode::Auto,
            dat: vec![],
            dat_exclude: vec![],
            dat_name_regex: None,
            dat_name_regex_exclude: None,
            dat_description_regex: None,
            dat_description_regex_exclude: None,
            dat_combine: false,
            dat_ignore_parent_clone: false,
            list_unmatched_dats: false,
            enable_hasheous: false,
            igdb_client_id: None,
            igdb_client_secret: None,
            igdb_token: None,
            igdb_mode: IgdbLookupMode::BestEffort,
            patch: vec![],
            patch_exclude: vec![],
            output: None,
            dir_mirror: false,
            dir_dat_mirror: false,
            dir_dat_name: false,
            dir_dat_description: false,
            dir_letter: false,
            dir_letter_count: None,
            dir_letter_limit: None,
            dir_letter_group: false,
            dir_game_subdir: DirGameSubdirMode::Multiple,
            fix_extension: FixExtensionMode::Auto,
            overwrite: false,
            overwrite_invalid: false,
            move_delete_dirs: MoveDeleteDirsMode::Auto,
            clean_exclude: vec![],
            clean_backup: None,
            clean_dry_run: false,
            zip_format: ZipFormat::Torrentzip,
            zip_exclude: None,
            zip_dat_name: false,
            link_mode: LinkMode::Hardlink,
            symlink_relative: false,
            header: None,
            remove_headers: None,
            trimmed_glob: None,
            trim_scan_archives: false,
            merge_roms: MergeMode::Fullnonmerged,
            merge_discs: false,
            exclude_disks: false,
            allow_excess_sets: false,
            allow_incomplete_sets: false,
            filter_regex: None,
            filter_regex_exclude: None,
            filter_language: None,
            filter_region: None,
            filter_category_regex: None,
            no_bios: false,
            no_device: false,
            no_unlicensed: false,
            only_retail: false,
            no_debug: false,
            no_demo: false,
            no_beta: false,
            no_sample: false,
            no_prototype: false,
            no_program: false,
            verbose: 0,
            quiet: 0,
            diag: false,
            print_plan: false,
            cache_only: false,
            cache_db: None,
            hash_threads: None,
            scan_threads: None,
            show_match_reasons: false,
            save_igdb_creds: false,
        };

        let result = Config::try_from(cli);
        assert!(result.is_err());
    }

    #[test]
    fn errors_when_checksum_max_lower_than_min() {
        let cli = Cli {
            commands: vec![Action::Test],
            input: vec![PathBuf::from("/tmp/file.bin")],
            input_exclude: vec![],
            input_checksum_quick: false,
            input_checksum_min: Checksum::Sha1,
            input_checksum_max: Some(Checksum::Md5),
            input_checksum_archives: ArchiveChecksumMode::Auto,
            dat: vec![],
            dat_exclude: vec![],
            dat_name_regex: None,
            dat_name_regex_exclude: None,
            dat_description_regex: None,
            dat_description_regex_exclude: None,
            dat_combine: false,
            dat_ignore_parent_clone: false,
            list_unmatched_dats: false,
            enable_hasheous: false,
            igdb_client_id: None,
            igdb_client_secret: None,
            igdb_token: None,
            igdb_mode: IgdbLookupMode::BestEffort,
            patch: vec![],
            patch_exclude: vec![],
            output: Some(PathBuf::from("out")),
            dir_mirror: false,
            dir_dat_mirror: false,
            dir_dat_name: false,
            dir_dat_description: false,
            dir_letter: false,
            dir_letter_count: None,
            dir_letter_limit: None,
            dir_letter_group: false,
            dir_game_subdir: DirGameSubdirMode::Multiple,
            fix_extension: FixExtensionMode::Auto,
            overwrite: false,
            overwrite_invalid: false,
            move_delete_dirs: MoveDeleteDirsMode::Auto,
            clean_exclude: vec![],
            clean_backup: None,
            clean_dry_run: false,
            zip_format: ZipFormat::Torrentzip,
            zip_exclude: None,
            zip_dat_name: false,
            link_mode: LinkMode::Hardlink,
            symlink_relative: false,
            header: None,
            remove_headers: None,
            trimmed_glob: None,
            trim_scan_archives: false,
            merge_roms: MergeMode::Fullnonmerged,
            merge_discs: false,
            exclude_disks: false,
            allow_excess_sets: false,
            allow_incomplete_sets: false,
            filter_regex: None,
            filter_regex_exclude: None,
            filter_language: None,
            filter_region: None,
            filter_category_regex: None,
            no_bios: false,
            no_device: false,
            no_unlicensed: false,
            only_retail: false,
            no_debug: false,
            no_demo: false,
            no_beta: false,
            no_sample: false,
            no_prototype: false,
            no_program: false,
            verbose: 0,
            quiet: 0,
            diag: false,
            print_plan: false,
            cache_only: false,
            cache_db: None,
            hash_threads: None,
            scan_threads: None,
            show_match_reasons: false,
            save_igdb_creds: false,
        };

        let result = Config::try_from(cli);
        assert!(result.is_err());
    }

    #[test]
    fn errors_when_letter_group_without_limit() {
        let cli = Cli {
            commands: vec![Action::Test],
            input: vec![PathBuf::from("/tmp/file.bin")],
            input_exclude: vec![],
            input_checksum_quick: false,
            input_checksum_min: Checksum::Crc32,
            input_checksum_max: None,
            input_checksum_archives: ArchiveChecksumMode::Auto,
            dat: vec![],
            dat_exclude: vec![],
            dat_name_regex: None,
            dat_name_regex_exclude: None,
            dat_description_regex: None,
            dat_description_regex_exclude: None,
            dat_combine: false,
            dat_ignore_parent_clone: false,
            list_unmatched_dats: false,
            enable_hasheous: false,
            igdb_client_id: None,
            igdb_client_secret: None,
            igdb_token: None,
            igdb_mode: IgdbLookupMode::BestEffort,
            patch: vec![],
            patch_exclude: vec![],
            output: Some(PathBuf::from("out")),
            dir_mirror: false,
            dir_dat_mirror: false,
            dir_dat_name: false,
            dir_dat_description: false,
            dir_letter: false,
            dir_letter_count: None,
            dir_letter_limit: None,
            dir_letter_group: true,
            dir_game_subdir: DirGameSubdirMode::Multiple,
            fix_extension: FixExtensionMode::Auto,
            overwrite: false,
            overwrite_invalid: false,
            move_delete_dirs: MoveDeleteDirsMode::Auto,
            clean_exclude: vec![],
            clean_backup: None,
            clean_dry_run: false,
            zip_format: ZipFormat::Torrentzip,
            zip_exclude: None,
            zip_dat_name: false,
            link_mode: LinkMode::Hardlink,
            symlink_relative: false,
            header: None,
            remove_headers: None,
            trimmed_glob: None,
            trim_scan_archives: false,
            merge_roms: MergeMode::Fullnonmerged,
            merge_discs: false,
            exclude_disks: false,
            allow_excess_sets: false,
            allow_incomplete_sets: false,
            filter_regex: None,
            filter_regex_exclude: None,
            filter_language: None,
            filter_region: None,
            filter_category_regex: None,
            no_bios: false,
            no_device: false,
            no_unlicensed: false,
            only_retail: false,
            no_debug: false,
            no_demo: false,
            no_beta: false,
            no_sample: false,
            no_prototype: false,
            no_program: false,
            verbose: 0,
            quiet: 0,
            diag: false,
            print_plan: false,
            cache_only: false,
            cache_db: None,
            hash_threads: None,
            scan_threads: None,
            show_match_reasons: false,
            save_igdb_creds: false,
        };

        let result = Config::try_from(cli);
        assert!(result.is_err());
    }

    #[test]
    fn errors_when_letter_limit_zero() {
        let cli = Cli {
            commands: vec![Action::Test],
            input: vec![PathBuf::from("/tmp/file.bin")],
            input_exclude: vec![],
            input_checksum_quick: false,
            input_checksum_min: Checksum::Crc32,
            input_checksum_max: None,
            input_checksum_archives: ArchiveChecksumMode::Auto,
            dat: vec![],
            dat_exclude: vec![],
            dat_name_regex: None,
            dat_name_regex_exclude: None,
            dat_description_regex: None,
            dat_description_regex_exclude: None,
            dat_combine: false,
            dat_ignore_parent_clone: false,
            list_unmatched_dats: false,
            enable_hasheous: false,
            igdb_client_id: None,
            igdb_client_secret: None,
            igdb_token: None,
            igdb_mode: IgdbLookupMode::BestEffort,
            patch: vec![],
            patch_exclude: vec![],
            output: Some(PathBuf::from("out")),
            dir_mirror: false,
            dir_dat_mirror: false,
            dir_dat_name: false,
            dir_dat_description: false,
            dir_letter: true,
            dir_letter_count: None,
            dir_letter_limit: Some(0),
            dir_letter_group: false,
            dir_game_subdir: DirGameSubdirMode::Multiple,
            fix_extension: FixExtensionMode::Auto,
            overwrite: false,
            overwrite_invalid: false,
            move_delete_dirs: MoveDeleteDirsMode::Auto,
            clean_exclude: vec![],
            clean_backup: None,
            clean_dry_run: false,
            zip_format: ZipFormat::Torrentzip,
            zip_exclude: None,
            zip_dat_name: false,
            link_mode: LinkMode::Hardlink,
            symlink_relative: false,
            header: None,
            remove_headers: None,
            trimmed_glob: None,
            trim_scan_archives: false,
            merge_roms: MergeMode::Fullnonmerged,
            merge_discs: false,
            exclude_disks: false,
            allow_excess_sets: false,
            allow_incomplete_sets: false,
            filter_regex: None,
            filter_regex_exclude: None,
            filter_language: None,
            filter_region: None,
            filter_category_regex: None,
            no_bios: false,
            no_device: false,
            no_unlicensed: false,
            only_retail: false,
            no_debug: false,
            no_demo: false,
            no_beta: false,
            no_sample: false,
            no_prototype: false,
            no_program: false,
            verbose: 0,
            quiet: 0,
            diag: false,
            print_plan: false,
            cache_only: false,
            cache_db: None,
            hash_threads: None,
            scan_threads: None,
            show_match_reasons: false,
            save_igdb_creds: false,
        };

        let result = Config::try_from(cli);
        assert!(result.is_err());
    }

    #[test]
    fn supplies_default_letter_count() {
        let cli = Cli {
            commands: vec![Action::Test],
            input: vec![PathBuf::from("/tmp/file.bin")],
            input_exclude: vec![],
            input_checksum_quick: false,
            input_checksum_min: Checksum::Crc32,
            input_checksum_max: None,
            input_checksum_archives: ArchiveChecksumMode::Auto,
            dat: vec![],
            dat_exclude: vec![],
            dat_name_regex: None,
            dat_name_regex_exclude: None,
            dat_description_regex: None,
            dat_description_regex_exclude: None,
            dat_combine: false,
            dat_ignore_parent_clone: false,
            list_unmatched_dats: false,
            enable_hasheous: false,
            igdb_client_id: None,
            igdb_client_secret: None,
            igdb_token: None,
            igdb_mode: IgdbLookupMode::BestEffort,
            patch: vec![],
            patch_exclude: vec![],
            output: Some(PathBuf::from("out")),
            dir_mirror: false,
            dir_dat_mirror: false,
            dir_dat_name: false,
            dir_dat_description: false,
            dir_letter: true,
            dir_letter_count: None,
            dir_letter_limit: None,
            dir_letter_group: false,
            dir_game_subdir: DirGameSubdirMode::Multiple,
            fix_extension: FixExtensionMode::Auto,
            overwrite: false,
            overwrite_invalid: false,
            move_delete_dirs: MoveDeleteDirsMode::Auto,
            clean_exclude: vec![],
            clean_backup: None,
            clean_dry_run: false,
            zip_format: ZipFormat::Torrentzip,
            zip_exclude: None,
            zip_dat_name: false,
            link_mode: LinkMode::Hardlink,
            symlink_relative: false,
            header: None,
            remove_headers: None,
            trimmed_glob: None,
            trim_scan_archives: false,
            merge_roms: MergeMode::Fullnonmerged,
            merge_discs: false,
            exclude_disks: false,
            allow_excess_sets: false,
            allow_incomplete_sets: false,
            filter_regex: None,
            filter_regex_exclude: None,
            filter_language: None,
            filter_region: None,
            filter_category_regex: None,
            no_bios: false,
            no_device: false,
            no_unlicensed: false,
            only_retail: false,
            no_debug: false,
            no_demo: false,
            no_beta: false,
            no_sample: false,
            no_prototype: false,
            no_program: false,
            verbose: 0,
            quiet: 0,
            diag: false,
            print_plan: false,
            cache_only: false,
            cache_db: None,
            hash_threads: None,
            scan_threads: None,
            show_match_reasons: false,
            save_igdb_creds: false,
        };

        let config = Config::try_from(cli).unwrap();
        assert_eq!(config.dir_letter_count, Some(1));
    }

    #[test]
    fn errors_without_output_for_copy() {
        let cli = Cli {
            commands: vec![Action::Copy],
            input: vec![PathBuf::from("/tmp/file.bin")],
            input_exclude: vec![],
            input_checksum_quick: false,
            input_checksum_min: Checksum::Crc32,
            input_checksum_max: None,
            input_checksum_archives: ArchiveChecksumMode::Auto,
            dat: vec![],
            dat_exclude: vec![],
            dat_name_regex: None,
            dat_name_regex_exclude: None,
            dat_description_regex: None,
            dat_description_regex_exclude: None,
            dat_combine: false,
            dat_ignore_parent_clone: false,
            list_unmatched_dats: false,
            enable_hasheous: false,
            igdb_client_id: None,
            igdb_client_secret: None,
            igdb_token: None,
            igdb_mode: IgdbLookupMode::BestEffort,
            patch: vec![],
            patch_exclude: vec![],
            output: None,
            dir_mirror: false,
            dir_dat_mirror: false,
            dir_dat_name: false,
            dir_dat_description: false,
            dir_letter: false,
            dir_letter_count: None,
            dir_letter_limit: None,
            dir_letter_group: false,
            dir_game_subdir: DirGameSubdirMode::Multiple,
            fix_extension: FixExtensionMode::Auto,
            overwrite: false,
            overwrite_invalid: false,
            move_delete_dirs: MoveDeleteDirsMode::Auto,
            clean_exclude: vec![],
            clean_backup: None,
            clean_dry_run: false,
            zip_format: ZipFormat::Torrentzip,
            zip_exclude: None,
            zip_dat_name: false,
            link_mode: LinkMode::Hardlink,
            symlink_relative: false,
            header: None,
            remove_headers: None,
            trimmed_glob: None,
            trim_scan_archives: false,
            merge_roms: MergeMode::Fullnonmerged,
            merge_discs: false,
            exclude_disks: false,
            allow_excess_sets: false,
            allow_incomplete_sets: false,
            filter_regex: None,
            filter_regex_exclude: None,
            filter_language: None,
            filter_region: None,
            filter_category_regex: None,
            no_bios: false,
            no_device: false,
            no_unlicensed: false,
            only_retail: false,
            no_debug: false,
            no_demo: false,
            no_beta: false,
            no_sample: false,
            no_prototype: false,
            no_program: false,
            verbose: 0,
            quiet: 0,
            diag: false,
            print_plan: false,
            cache_only: false,
            cache_db: None,
            hash_threads: None,
            scan_threads: None,
            show_match_reasons: false,
            save_igdb_creds: false,
        };

        let result = Config::try_from(cli);
        assert!(result.is_err());
    }

    #[test]
    fn honors_print_plan_flag() {
        fn make_cli(print_plan: bool) -> Cli {
            Cli {
                commands: vec![Action::Test],
                input: vec![PathBuf::from("/tmp/file.bin")],
                input_exclude: vec![],
                input_checksum_quick: false,
                input_checksum_min: Checksum::Crc32,
                input_checksum_max: None,
                input_checksum_archives: ArchiveChecksumMode::Auto,
                dat: vec![],
                dat_exclude: vec![],
                dat_name_regex: None,
                dat_name_regex_exclude: None,
                dat_description_regex: None,
                dat_description_regex_exclude: None,
                dat_combine: false,
                dat_ignore_parent_clone: false,
                list_unmatched_dats: false,
                enable_hasheous: false,
                igdb_client_id: None,
                igdb_client_secret: None,
                igdb_token: None,
                igdb_mode: IgdbLookupMode::BestEffort,
                patch: vec![],
                patch_exclude: vec![],
                output: None,
                dir_mirror: false,
                dir_dat_mirror: false,
                dir_dat_name: false,
                dir_dat_description: false,
                dir_letter: false,
                dir_letter_count: None,
                dir_letter_limit: None,
                dir_letter_group: false,
                dir_game_subdir: DirGameSubdirMode::Multiple,
                fix_extension: FixExtensionMode::Auto,
                overwrite: false,
                overwrite_invalid: false,
                move_delete_dirs: MoveDeleteDirsMode::Auto,
                clean_exclude: vec![],
                clean_backup: None,
                clean_dry_run: false,
                zip_format: ZipFormat::Torrentzip,
                zip_exclude: None,
                zip_dat_name: false,
                link_mode: LinkMode::Hardlink,
                symlink_relative: false,
                header: None,
                remove_headers: None,
                trimmed_glob: None,
                trim_scan_archives: false,
                merge_roms: MergeMode::Fullnonmerged,
                merge_discs: false,
                exclude_disks: false,
                allow_excess_sets: false,
                allow_incomplete_sets: false,
                filter_regex: None,
                filter_regex_exclude: None,
                filter_language: None,
                filter_region: None,
                filter_category_regex: None,
                no_bios: false,
                no_device: false,
                no_unlicensed: false,
                only_retail: false,
                no_debug: false,
                no_demo: false,
                no_beta: false,
                no_sample: false,
                no_prototype: false,
                no_program: false,
                verbose: 0,
                quiet: 0,
                diag: false,
                cache_only: false,
                cache_db: None,
                hash_threads: None,
                scan_threads: None,
                show_match_reasons: false,
                save_igdb_creds: false,
                print_plan,
            }
        }

        let default_cfg = Config::try_from(make_cli(false)).expect("valid config");
        assert!(!default_cfg.print_plan);

        let opt_in_cfg = Config::try_from(make_cli(true)).expect("valid config");
        assert!(opt_in_cfg.print_plan);
    }

    #[test]
    fn errors_when_hash_threads_zero() {
        let mut cli = make_cli(false);
        cli.hash_threads = Some(0);
        let result = Config::try_from(cli);
        assert!(result.is_err());
    }
}
