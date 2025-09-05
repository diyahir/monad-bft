use std::path::PathBuf;

use clap::Parser;

#[derive(Debug, Parser)]
pub struct BlockWriterArgs {
    /// S3 bucket name for storing checker state
    #[arg(long)]
    pub bucket: String,

    /// AWS region
    #[arg(long)]
    pub region: Option<String>,

    #[arg(long)]
    pub start_block: u64,

    /// Override block number to stop at
    #[arg(long)]
    pub stop_block: u64,

    #[arg(long)]
    pub dest_path: PathBuf,
}
