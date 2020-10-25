use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use bson::Document;
use structopt::clap;
use structopt::StructOpt;

static AUTO_FLUSH: i64 = 100_000;

#[derive(Debug, StructOpt)]
#[structopt(name = "bsonsplit", about = "Splits a BSON file")]
struct Cli {
    /// The maximum amount of resulting files. Must be at least 1.
    #[structopt(short, long)]
    split: u32,
    /// The path to the file to read
    #[structopt(parse(from_os_str))]
    path: std::path::PathBuf,
}

fn validate(opt: &Cli) {
    if opt.split < 1 {
        clap::Error::with_description("split must be at least 1", clap::ErrorKind::InvalidValue)
            .exit();
    }
}

fn process_doc(doc: bson::de::Result<Document>) -> anyhow::Result<Option<Document>> {
    match doc {
        Ok(doc) => Ok(Some(doc)),
        Err(e) => match e {
            bson::de::Error::IoError(e) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    Ok(None)
                } else {
                    Err(e).with_context(|| "IO error")
                }
            }
            _ => Err(e).with_context(|| "BSON error"),
        },
    }
}

fn create_files(prefix: &str, split: u32) -> anyhow::Result<(Vec<File>, Vec<String>)> {
    let runtime = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .with_context(|| "Failed to get time")?
        .as_millis();

    let mut files = Vec::new();
    let mut paths = Vec::new();
    for i in 1..=split {
        let filename = format!("{}-{}-{}.bson", prefix, runtime, i);
        paths.push(filename.clone());
        let f = File::create(filename)?;
        files.push(f);
    }
    Ok((files, paths))
}

fn flush_all(bufs: &mut Vec<BufWriter<&File>>) -> anyhow::Result<()> {
    for x in bufs {
        x.flush().with_context(|| "Failed to flush")?;
    }
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let opt = Cli::from_args();
    validate(&opt);

    let f = File::open(opt.path.clone()).with_context(|| "Failed to open file")?;
    let mut f = BufReader::new(f);

    let prefix = opt
        .path
        .file_stem()
        .ok_or_else(|| anyhow::Error::msg("Unable to extract prefix"))?
        .to_str()
        .ok_or_else(|| anyhow::Error::msg("Unable to read file path"))?;

    let (output, paths) =
        create_files(prefix, opt.split).with_context(|| "Failed to create output files")?;

    let mut output = output
        .iter()
        .map(|file| BufWriter::new(file))
        .collect::<Vec<BufWriter<&File>>>();

    let mut cycle = (0..output.len()).cycle();
    let mut writes: i64 = 0;

    loop {
        if let Some(doc) = process_doc(Document::from_reader(&mut f))? {
            let file_index = cycle.next().unwrap();
            let mut buf = &mut output[file_index];
            doc.to_writer(&mut buf)
                .with_context(|| "Failed to write document")?;
            writes += 1;

            if writes % AUTO_FLUSH == 0 {
                flush_all(&mut output)?;
            }
        } else {
            break;
        }
    }

    flush_all(&mut output)?;
    paths.iter().for_each(|p| println!("{}", p));

    Ok(())
}
