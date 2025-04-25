use anyhow::{anyhow, Context, Result};
use clap::ArgMatches;
use std::env;
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::archive::{self, *};
use crate::config;
use crate::create::create_archive;
use crate::hash::hash_256_iov;
use crate::output::Output;
use crate::paths::*;
use crate::slab::builder::*;
use crate::stream::{self, *};
use crate::stream_builders::{Builder, MappingBuilder};

fn migrate(archive_dir: &Path, output_dir: &Path, config: &config::Config) -> Result<()> {
    env::set_current_dir(archive_dir)?;

    let paths = fs::read_dir(Path::new("./streams"))?;
    let stream_ids = paths
        .filter_map(|entry| entry.ok().and_then(|e| e.file_name().into_string().ok()))
        .collect::<Vec<String>>();

    // for each stream:
    // decode the stream entries
    // if it's a MapEntry::Data, read the data chunk (one or multiple)
    // add the chunk to the destination archive, while keep the stream name unchanged
    // - read the hash from hashes file
    // - add the hash and chunk iov to the dest archive
    // - data_add returns the chunk's new location in the stream
    // - add the MapEntry to the StreamBuilder
    let cache_nr_entries = (1024 * 1024 * config.data_cache_size_meg) / SLAB_SIZE_TARGET;

    let src_data_file = SlabFileBuilder::open(archive_dir.join(data_path()))
        .cache_nr_entries(cache_nr_entries)
        .build()?;
    let compressed = src_data_file.is_compressed();
    let src_hashes_file = Arc::new(Mutex::new(
        SlabFileBuilder::open(archive_dir.join(hashes_path())).build()?,
    ));
    // TODO: estimate the number of cache entries for hashes
    let mut src_archive = archive::Data::new(src_data_file, src_hashes_file, 128)?;

    create_archive(
        output_dir,
        config.block_size,
        compressed,
        config.hash_cache_size_meg,
        config.data_cache_size_meg,
    )?;

    let dest_data_file = SlabFileBuilder::open(output_dir.join(data_path()))
        .write(true)
        .queue_depth(128)
        .build()?;
    let dest_hashes_file = Arc::new(Mutex::new(
        SlabFileBuilder::open(output_dir.join(hashes_path()))
            .write(true)
            .queue_depth(128)
            .build()?,
    ));
    let mut dest_archive = archive::Data::new(dest_data_file, dest_hashes_file, 128)?;

    for stream in stream_ids {
        let mut src_stream =
            SlabFileBuilder::open(archive_dir.join(stream_path(&stream))).build()?;
        let nr_slabs = src_stream.get_nr_slabs();

        let mut unpacker = stream::MappingUnpacker::default();

        let mut mapping_builder = MappingBuilder::default();
        let mut stream_buf = Vec::new();

        let dest_stream_path = output_dir.join(stream_path(&stream));
        std::fs::create_dir(
            dest_stream_path
                .parent()
                .ok_or_else(|| anyhow!("invalid stream path"))?,
        )?;
        let mut dest_stream = SlabFileBuilder::create(dest_stream_path)
            .queue_depth(16)
            .compressed(true)
            .build()
            .context("couldn't open stream slab file")?;

        for s in 0..nr_slabs {
            let stream_data = src_stream.read(s as u32)?;
            let (entries, _position) = unpacker.unpack(&stream_data[..])?;
            let mut stream_buf = Vec::new();

            // FIXME: display the progress according to the entry index
            for e in entries.iter() {
                use MapEntry::*;

                // handle a single MapEntry in the stream
                match e {
                    Fill { byte, len } => {
                        mapping_builder.next(
                            &MapEntry::Fill {
                                byte: *byte,
                                len: *len,
                            },
                            *len,
                            &mut stream_buf,
                        )?;
                    }
                    Data {
                        slab,
                        offset,
                        nr_entries,
                    } => {
                        // read the chunk from the source archive
                        for i in *offset..*offset + *nr_entries {
                            let (data, start, end) = src_archive.data_get(*slab, i, 1, None)?;
                            let len = end - start;
                            let iov = vec![&data[start..end]];
                            let h = hash_256_iov(&iov);
                            let (entry_location, _) = dest_archive.data_add(h, &iov, len as u64)?;
                            let me = MapEntry::Data {
                                slab: entry_location.0,
                                offset: entry_location.1,
                                nr_entries: 1,
                            };

                            mapping_builder.next(&me, len as u64, &mut stream_buf)?;
                        }
                    }
                    Partial {
                        begin: _,
                        end: _,
                        slab: _,
                        offset: _,
                        nr_entries: _,
                    } => {
                        // TODO
                    }
                    Unmapped { len } => {
                        mapping_builder.next(
                            &MapEntry::Unmapped { len: *len },
                            *len,
                            &mut stream_buf,
                        )?;
                    }
                    Ref { .. } => {
                        return Err(anyhow!("unexpected MapEntry::Ref"));
                    }
                }

                complete_slab(&mut dest_stream, &mut stream_buf, SLAB_SIZE_TARGET)?;
            }
        }

        complete_slab(&mut dest_stream, &mut stream_buf, 0)?;
        dest_stream.close()?;

        // copy the stream config
        // TODO: update the packed_size
        let mut cfg = config::read_stream_config_from(archive_dir, &stream)?;
        cfg.pack_time = config::now();
        config::write_stream_config_to(output_dir, &stream, &cfg)?;
    }

    Ok(())
}

pub fn run(matches: &ArgMatches, _output: Arc<Output>) -> Result<()> {
    let archive_dir = Path::new(matches.get_one::<String>("ARCHIVE").unwrap()).canonicalize()?;
    let output_dir = Path::new(matches.get_one::<String>("OUTPUT_DIR").unwrap());
    let config = config::read_config(&archive_dir, matches)?;

    migrate(&archive_dir, &output_dir, &config)
}
