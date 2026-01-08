// Lightweight verbosity-gated logging helper used throughout the crate.
macro_rules! vprintln {
	($verbose:expr, $level:expr, $($arg:tt)*) => {
		if $verbose >= $level {
			eprintln!($($arg)*);
		}
	};
}

// Public library re-exports for integration tests and external use.
pub mod actions;
pub mod archives;
pub mod cache;
pub mod candidate_archive_hasher;
pub mod candidate_extension;
pub mod candidates;
pub mod checksum;
pub mod cli;
pub mod config;
pub mod dat;
pub mod game_console;
pub mod igdb_platform_map;
pub mod patch;
pub mod patch_apply;
pub mod progress;
pub mod records;
pub mod roms;
pub mod torrentzip;
pub mod torrentzip_zip64;
pub mod types;
pub mod utils;
pub mod write_candidate;

// Keep main.rs thin and have it call into the library functions.
