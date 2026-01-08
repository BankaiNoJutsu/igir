use clap::Parser;

use igir::actions::perform_actions;
use igir::cli::Cli;
use igir::config::Config;
use num_cpus;
use rayon::ThreadPoolBuilder;

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let config = Config::try_from(cli)?;

    // Initialize Rayon global thread pool using the configured thread counts.
    // Use the larger of the configured hash and scan thread counts (or CPU count by default)
    let default_threads = num_cpus::get();
    let hash_threads = config.hash_threads.unwrap_or(default_threads);
    let scan_threads = config.scan_threads.unwrap_or(default_threads);
    let threads = std::cmp::max(hash_threads, scan_threads);
    let _ = ThreadPoolBuilder::new().num_threads(threads).build_global();

    let plan = perform_actions(&config)?;
    if config.print_plan {
        println!("{}", serde_json::to_string_pretty(&plan)?);
    }

    Ok(())
}
