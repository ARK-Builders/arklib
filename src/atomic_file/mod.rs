mod atomic;
use atomic::AtomicFile;
use serde::{Serialize, de::DeserializeOwned};
use std::io::{Result, Read, Write};


pub fn modifiy(x: &AtomicFile, mut op: impl FnMut(&[u8]) -> Vec<u8>) -> Result<()> {
    let mut buf = vec![];
    loop {
        let latest = x.load()?;
        buf.clear();
        if let Some(mut file) = latest.open()? {
            file.read_to_end(&mut buf)?;
        }
        let data = op(&buf);
        let tmp = x.make_temp()?;
        (&tmp).write_all(&data)?;
        (&tmp).flush()?;
        match x.compare_and_swap(&latest, tmp) {
            Ok(()) => return Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(err) => return Err(err),
        }
    }
}

pub fn modify_json<T: Serialize + DeserializeOwned>(
    x: &AtomicFile,
    mut op: impl FnMut(&mut Option<T>),
) -> std::io::Result<()> {
    loop {
        let latest = x.load()?;
        let mut val = None;
        if let Some(file) = latest.open()? {
            val = Some(serde_json::from_reader(std::io::BufReader::new(file))?);
        }
        op(&mut val);
        let tmp = x.make_temp()?;
        let mut w = std::io::BufWriter::new(&tmp);
        serde_json::to_writer(&mut w, &val)?;
        w.flush()?;
        drop(w);
        match x.compare_and_swap(&latest, tmp) {
            Ok(()) => return Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(err) => return Err(err),
        }
    }
}