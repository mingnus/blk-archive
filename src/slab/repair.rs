use anyhow::{anyhow, Result};
use byteorder::{LittleEndian, ReadBytesExt};
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom};
use std::path::Path;

use crate::archive::SLAB_SIZE_TARGET;
use crate::slab::offsets::SlabOffsets;
use crate::slab::{read_slab_header, SLAB_MAGIC};

//------------------------------------------------

pub fn repair<P: AsRef<Path>>(_p: P) -> Result<()> {
    todo!();
}

pub fn rebuild_offsets<P: AsRef<Path>>(data_path: P) -> Result<SlabOffsets> {
    let mut data = OpenOptions::new()
        .read(true)
        .write(false)
        .create(false)
        .open(data_path)?;
    let data_size = data.metadata()?.len();
    let nr_slabs = data_size / SLAB_SIZE_TARGET as u64;
    let mut offsets = Vec::<u64>::with_capacity(nr_slabs as usize); // FIXME: estimate properly

    read_slab_header(&mut data)?;

    let mut pos = data.stream_position()?;
    while pos < data_size {
        if data.read_u64::<LittleEndian>()? != SLAB_MAGIC {
            return Err(anyhow!("unexpected slab magic at offset {}", pos));
        }

        offsets.push(pos);

        let len = data.read_u64::<LittleEndian>()?;
        pos += len + 8 + 8 + 8; // incl. magic, len and csum
        data.seek(SeekFrom::Start(pos))?;
    }

    Ok(SlabOffsets { offsets })
}

//------------------------------------------------
