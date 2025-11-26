use std::path::PathBuf;

use crate::{
    cli::Cli,
    types::{
        Action, ArchiveChecksumMode, Checksum, DirGameSubdirMode, FixExtensionMode, LinkMode,
        MergeMode, MoveDeleteDirsMode, ZipFormat,
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

    pub fn validate(&self) -> anyhow::Result<()> {
        self.validate_commands()?;
        self.validate_checksum_range()?;
        self.validate_letter_strategy()?;
        self.validate_output_requirements()?;
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

        config.validate()?;

        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::Cli;

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
        };

        let result = Config::try_from(cli);
        assert!(result.is_err());
    }
}
