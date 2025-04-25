use anyhow::{anyhow, Result};
use byteorder::{LittleEndian, ReadBytesExt};
use clap::ArgMatches;
use std::env;
use std::path::Path;
use std::sync::Arc;

use crate::archive::{DATA_HEADER_LEN, MAX_NR_ENTRIES};
use crate::hash::hash_256_iov;
use crate::hash_index::IndexBuilder;
use crate::output::Output;
use crate::paths::*;
use crate::slab::repair::rebuild_offsets;
use crate::slab::*;

fn build_hash_index(data: &[u8]) -> Result<Arc<Vec<u8>>> {
    let mut builder = IndexBuilder::with_capacity(MAX_NR_ENTRIES);
    let entries_len = {
        let mut c = std::io::Cursor::new(data);
        let nr_entries = c.read_u64::<LittleEndian>()?;
        (0..nr_entries)
            .map(|_| c.read_u64::<LittleEndian>())
            .collect::<std::io::Result<Vec<_>>>()
    }?;

    let mut offset = DATA_HEADER_LEN;
    for len in &entries_len {
        let l = *len as usize;
        let iov = vec![&data[offset..offset + l]];
        let h = hash_256_iov(&iov);
        builder.insert(h, l);
        offset += l;
    }
    builder.build()
}

fn rebuild_hashes_index(archive_dir: &Path) -> Result<()> {
    env::set_current_dir(archive_dir)?;

    let mut data_file = SlabFileBuilder::open(data_path()).build()?;
    let nr_slabs = data_file.get_nr_slabs();

    if nr_slabs > u32::MAX as usize {
        return Err(anyhow!("Too many slabs"));
    }

    // FIXME: swap the filename, instead of overwrite
    let mut hashes_file = SlabFileBuilder::create(hashes_path()).build()?;

    for s in 0..nr_slabs as u32 {
        let data = data_file.read(s)?;
        let buf = build_hash_index(&data)?;
        hashes_file.write_slab(&buf)?;
    }

    hashes_file.close()?;
    data_file.close()?;

    Ok(())
}

fn repair(archive_dir: &Path) -> Result<()> {
    env::set_current_dir(archive_dir)?;

    let data_offsets = rebuild_offsets(data_path())?;
    data_offsets.write_offset_file(offsets_path(data_path()))?;

    rebuild_hashes_index(archive_dir)?;

    let indexes_offsets = rebuild_offsets(index_path())?;
    indexes_offsets.write_offset_file(offsets_path(index_path()))?;

    // TODO: rebuild all the stream's offsets

    Ok(())
}

pub fn run(matches: &ArgMatches, _output: Arc<Output>) -> Result<()> {
    let archive_dir = Path::new(matches.get_one::<String>("ARCHIVE").unwrap()).canonicalize()?;
    repair(&archive_dir)
}
