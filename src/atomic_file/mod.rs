mod atomic;
use serde::{de::DeserializeOwned, Serialize};
use std::io::{Read, Result, Write};

pub use atomic::AtomicFile;

pub fn modify(
    x: &AtomicFile,
    mut op: impl FnMut(&[u8]) -> Vec<u8>,
) -> Result<()> {
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
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                continue
            }
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
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                continue
            }
            Err(err) => return Err(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempdir::TempDir;

    #[test]
    fn failed_to_write_simultaneously() {
        let dir = TempDir::new("writing_test").unwrap();
        let root = dir.path();
        let shared_file = std::sync::Arc::new(AtomicFile::new(&root).unwrap());
        let mut handles = Vec::with_capacity(5);
        for i in 0..5 {
            let file = shared_file.clone();
            let handle = std::thread::spawn(move || {
                let temp = file.make_temp().unwrap();
                let current = file.load().unwrap();
                let content = format!("Content from thread {i}!");
                (&temp).write_all(&content.as_bytes()).unwrap();
                file.compare_and_swap(&current, temp)
            });
            handles.push(handle);
        }
        let results = handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>();
        // Ensure only one thread has succeed to write
        let success = results.iter().fold(0, |mut acc, r| {
            if r.is_ok() {
                acc += 1;
            }
            acc
        });
        assert!(success == 1);
    }

    #[test]
    fn multiple_writes_detected() {
        let dir = TempDir::new("simultaneous_writes").unwrap();
        let root = dir.path();
        let shared_file = std::sync::Arc::new(AtomicFile::new(&root).unwrap());
        let thread_number = 10;
        assert!(thread_number > 3);
        let mut handles = Vec::with_capacity(thread_number);
        for _ in 0..thread_number {
            let file = shared_file.clone();
            let handle = std::thread::spawn(move || {
                modify(&file, |data| {
                    if data.len() == 0 {
                        // Buffer has already been filled by another thread
                        "First content of file".as_bytes().to_vec()
                    } else {
                        let previous_content =
                            std::str::from_utf8(data).unwrap();
                        match previous_content {
                            "First content of file" => {
                                "Second content of file".as_bytes().to_vec()
                            }
                            "Second content of file" => {
                                "Third content of file".as_bytes().to_vec()
                            }
                            "Third content of file" => {
                                "4th content of file".as_bytes().to_vec()
                            }
                            _ => {
                                let mut iter = previous_content.split("th");
                                let number = iter.next().unwrap();
                                let number = number.parse::<usize>().unwrap();
                                format!(
                                    "{}th{}",
                                    number + 1,
                                    iter.next().unwrap()
                                )
                                .as_bytes()
                                .to_vec()
                            }
                        }
                    }
                })
            });
            handles.push(handle);
        }
        handles.into_iter().for_each(|handle| {
            handle.join().unwrap().unwrap();
        });
        // Last content
        let last_file = shared_file.load().unwrap();
        let last_content = last_file.read_to_string().unwrap();
        assert_eq!(
            last_content,
            format!("{}th content of file", thread_number)
        );
    }
}
