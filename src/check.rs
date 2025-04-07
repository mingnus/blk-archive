use anyhow::{anyhow, Context, Result};
use clap::ArgMatches;
use std::env;
use std::path::Path;
use std::sync::Arc;

use crate::hash::hash_256_iov;
use crate::hash_index::ByIndex;
use crate::output::Output;
use crate::paths::*;
use crate::slab::SlabFileBuilder;

fn check(archive_dir: &Path) -> Result<()> {
    env::set_current_dir(&archive_dir)?;

    let mut data_file = SlabFileBuilder::open(data_path())
        .build()
        .context("couldn't open data slab file")?;

    let mut hashes_file = SlabFileBuilder::open(hashes_path())
        .build()
        .context("couldn't open hashes slab file")?;

    let nr_data_slabs = data_file.get_nr_slabs();

    if nr_data_slabs != hashes_file.get_nr_slabs() {
        return Err(anyhow!("Number of slabs mismatch"));
    }

    if nr_data_slabs > u32::MAX as usize {
        return Err(anyhow!("Too many slabs"));
    }

    for s in 0..nr_data_slabs as u32 {
        let data = data_file.read(s)?;
        let hashes = ByIndex::new(hashes_file.read(s)?)?;

        // TODO: multithreading
        for i in 0..hashes.len() {
            let (data_begin, data_end, expected_hash) = hashes
                .get(i)
                .ok_or_else(|| anyhow!("Failed to get hash entry {} at slab {}", i, s))?;
            let iov = vec![&data[*data_begin as usize..*data_end as usize]];
            if hash_256_iov(&iov) != *expected_hash {
                return Err(anyhow!("Unexpected hash at slab {} index {}", s, i));
            }
        }
    }

    Ok(())
}

pub fn run(matches: &ArgMatches, _output: Arc<Output>) -> Result<()> {
    let archive_dir = Path::new(matches.get_one::<String>("ARCHIVE").unwrap()).canonicalize()?;
    check(&archive_dir)
}
