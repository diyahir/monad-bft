use std::sync::Arc;

use alloy_rlp::Encodable;
use clap::Parser;
use futures::future::join_all;
use monad_archive::prelude::*;
use tokio::{fs, sync::Semaphore};
use tracing::Level;

pub mod cli;

pub async fn main(args: cli::BlockWriterArgs) -> Result<()> {
    // set concurrency level to 25
    let concurrent_block_semaphore = Arc::new(Semaphore::new(50));

    let url = "https://abc.com"; // dummy proxy url
    let api_key = "";
    let aws_reader =
        ArchiveReader::init_aws_reader(args.bucket.clone(), args.region.clone(), url, api_key, 1)
            .await?;

    let aws_reader = Arc::new(aws_reader);

    let dest_path = args.dest_path.clone();

    let join_handles = (args.start_block..=args.stop_block).map(|current_block: u64| {
        let aws_reader = Arc::clone(&aws_reader);
        let dest_path = dest_path.clone();

        let semaphore = concurrent_block_semaphore.clone();
        tokio::spawn(async move {
            let _permit = semaphore
                .acquire()
                .await
                .expect("Got permit to execute a new block");
            let block_result = aws_reader
                .get_block_by_number(current_block)
                .await
                .wrap_err("Failed to get blocks from archiver");
            match block_result {
                Ok(block) => {
                    // info!("Block {current_block} is: {:?}", block);
                    let mut block_rlp = Vec::new();
                    block.encode(&mut block_rlp);

                    let output_path = dest_path.join(current_block.to_string());
                    let write_result = fs::write(&output_path, &block_rlp)
                        .await
                        .wrap_err("Failed to write to file");

                    match write_result {
                        Ok(()) => {
                            info!("Wrote block {} to {:?}", current_block, output_path);
                        }
                        Err(e) => {
                            error!("Error in writing file: {:?}", e);
                        }
                    }
                }
                Err(e) => {
                    error!("Error in aws reader {:?}", e)
                }
            }
        })
    });

    // TODO: Assume no join error
    let _results = join_all(join_handles).await;

    Ok(())
}
