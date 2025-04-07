use anyhow::Result;
use clap::ArgMatches;
use std::env;
use std::path::Path;
use std::sync::Arc;

use crate::output::Output;
use crate::paths::*;
use crate::slab::repair::rebuild_offsets;
use crate::slab::*;

pub fn repair(archive_dir: &Path) -> Result<()> {
    env::set_current_dir(&archive_dir)?;

    let data_offsets = rebuild_offsets(data_path())?;
    data_offsets.write_offset_file(offsets_path(data_path()))?;

    let hashes_offsets = rebuild_offsets(hashes_path())?;
    hashes_offsets.write_offset_file(offsets_path(hashes_path()))?;

    let indexes_offsets = rebuild_offsets(indexes_path())?;
    indexes_offsets.write_offset_file(offsets_path(indexes_path()))?;

    // TODO: rebuild all the stream's offsets

    Ok(())
}

pub fn run(matches: &ArgMatches, _output: Arc<Output>) -> Result<()> {
    let archive_dir = Path::new(matches.get_one::<String>("ARCHIVE").unwrap()).canonicalize()?;
    repair(&archive_dir)
}
