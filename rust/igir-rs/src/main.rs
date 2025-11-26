mod actions;
mod checksum;
mod cli;
mod config;
mod records;
mod types;
mod utils;

use clap::Parser;

use crate::actions::perform_actions;
use crate::cli::Cli;
use crate::config::Config;

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let config = Config::try_from(cli)?;

    let plan = perform_actions(&config)?;
    println!("{}", serde_json::to_string_pretty(&plan)?);

    Ok(())
}
