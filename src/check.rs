use anyhow::{anyhow, Context, Result};
use clap::ArgMatches;
use std::env;
use std::path::Path;
use std::sync::Arc;

use crate::hash::hash_256_iov;
use crate::hash_index::ByIndex;
use crate::output::Output;
use crate::paths::*;
use crate::slab::{SlabFile, SlabFileBuilder};

fn check_data_and_hashes(
    data_file: Arc<SlabFile>,
    hashes_file: Arc<SlabFile>,
    slab_range: std::ops::Range<u32>,
) -> Result<()> {
    for s in slab_range {
        let data = data_file.read_uncached(s)?;
        let hashes = ByIndex::new(Arc::new(hashes_file.read_uncached(s)?))?;

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

fn check(archive_dir: &Path) -> Result<()> {
    env::set_current_dir(&archive_dir)?;

    let data_file = Arc::new(
        SlabFileBuilder::open(data_path())
            .build()
            .context("couldn't open data slab file")?,
    );

    let hashes_file = Arc::new(
        SlabFileBuilder::open(hashes_path())
            .build()
            .context("couldn't open hashes slab file")?,
    );

    let nr_data_slabs = data_file.get_nr_slabs();

    if nr_data_slabs != hashes_file.get_nr_slabs() {
        return Err(anyhow!("Number of slabs mismatch"));
    }

    if nr_data_slabs > u32::MAX as usize {
        return Err(anyhow!("Too many slabs"));
    }

    let nr_threads = 2;
    let slabs_per_thread = nr_data_slabs / nr_threads;
    let mut slab_begin = 0;
    for i in 0..nr_threads - 1 {
        std::thread::spawn(|| {
            check_data_and_hashes(
                data_file.clone(),
                hashes_file.clone(),
                slab_begin..slab_begin + slabs_per_thread as u32,
            )
        });
    }

    Ok(())
}

pub fn run(matches: &ArgMatches, _output: Arc<Output>) -> Result<()> {
    let archive_dir = Path::new(matches.get_one::<String>("ARCHIVE").unwrap()).canonicalize()?;
    check(&archive_dir)
}
