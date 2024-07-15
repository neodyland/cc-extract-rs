use std::{
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use async_compression::tokio::{bufread::ZstdDecoder, write::GzipEncoder};
use json::{object, parse, stringify};
use tokio::{
    fs::{read_dir, File},
    io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
    sync::mpsc,
};

mod clean_seq;

#[tokio::main]
async fn main() {
    if let Err(e) = main_inner().await {
        eprintln!("{e}")
    }
}

async fn stream_files(files: Vec<PathBuf>) -> mpsc::Receiver<String> {
    let (tx, rx) = async_channel::bounded(10000);
    tokio::spawn(async move {
        for file in files {
            let mut dec = ZstdDecoder::new(BufReader::new(File::open(file).await?));
            let mut v = vec![];
            dec.read_to_end(&mut v).await?;
            if let Ok(s) = String::from_utf8(v) {
                for l in s.lines() {
                    if let Ok(c) = parse(l.trim()) {
                        for (k, v) in c.entries() {
                            if k == "html" {
                                if let Some(s) = v.as_str() {
                                    tx.send(s.to_string()).await?;
                                }
                            }
                        }
                    };
                }
            }
        }
        anyhow::Ok(())
    });
    let (txf, rxf) = mpsc::channel(1000);
    for _ in 0..num_cpus::get_physical() {
        let rx = rx.clone();
        let txf = txf.clone();
        tokio::spawn(async move {
            while let Ok(s) = rx.recv().await {
                for s in crate::clean_seq::clean_seq(&s) {
                    if s.char_indices().collect::<Vec<_>>().len() > 50 {
                        txf.send(s).await?;
                    }
                }
            }
            anyhow::Ok(())
        });
    }
    rxf
}

async fn main_inner() -> anyhow::Result<()> {
    let mut files = vec![];
    let mut dir = read_dir("./input").await?;
    while let Ok(Some(entry)) = dir.next_entry().await {
        if entry.file_name().to_string_lossy().ends_with(".jsonl.zstd") {
            files.push(entry.path());
        }
    }
    let mut frx = stream_files(files).await;
    let f = File::create(format!(
        "output/{}.jsonl.gz",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros()
    ))
    .await?;
    let mut f = GzipEncoder::new(BufWriter::new(f));
    while let Some(s) = frx.recv().await {
        f.write_all(
            stringify(object! {
                html: s,
            })
            .as_bytes(),
        )
        .await?;
        f.write_all("\n".as_bytes()).await?;
        f.flush().await?;
    }
    f.shutdown().await?;
    Ok(())
}
