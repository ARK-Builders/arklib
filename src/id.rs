use anyhow::anyhow;
use blake3::Hasher as Blake3Hasher;
use log;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use std::fs;
use std::io::Read;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::str::FromStr;

use crate::{ArklibError, Result};

#[derive(
    Eq,
    Ord,
    PartialEq,
    PartialOrd,
    Hash,
    Clone,
    Copy,
    Debug,
    Deserialize,
    Serialize,
)]
pub struct ResourceId {
    pub data_size: u64,
    pub blake3: [u8; 32],
}

impl Display for ResourceId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{:?}", self.data_size, self.blake3)
    }
}

impl FromStr for ResourceId {
    type Err = ArklibError;

    fn from_str(s: &str) -> Result<Self> {
        let (l, r) = s.split_once('-').ok_or(ArklibError::Parse)?;
        let data_size: u64 = l.parse().map_err(|_| ArklibError::Parse)?;
        let s = r.trim_start_matches('[').trim_end_matches(']');
        let bytes: Vec<&str> = s.split(", ").collect();
        if bytes.len() != 32 {
            return Err(ArklibError::Parse);
        }
        let mut blake3 = [0u8; 32];
        for (i, byte_str) in bytes.iter().enumerate() {
            blake3[i] = byte_str
                .trim()
                .parse()
                .map_err(|_e| ArklibError::Parse)?;
        }
        Ok(ResourceId { data_size, blake3 })
    }
}

impl ResourceId {
    pub fn compute<P: AsRef<Path>>(
        data_size: u64,
        file_path: P,
    ) -> Result<Self> {
        log::trace!(
            "[compute] file {} with size {} mb",
            file_path.as_ref().display(),
            data_size / MEGABYTE
        );

        let source = fs::OpenOptions::new()
            .read(true)
            .open(file_path.as_ref())?;

        let mut reader = BufReader::with_capacity(BUFFER_CAPACITY, source);
        ResourceId::compute_reader(data_size, &mut reader)
    }

    pub fn compute_bytes(bytes: &[u8]) -> Result<Self> {
        let data_size = bytes.len().try_into().map_err(|_| {
            ArklibError::Other(anyhow!("Can't convert usize to u64"))
        })?;
        let mut reader = BufReader::with_capacity(BUFFER_CAPACITY, bytes);
        ResourceId::compute_reader(data_size, &mut reader)
    }

    pub fn compute_reader<R: Read>(
        data_size: u64,
        reader: &mut BufReader<R>,
    ) -> Result<Self> {
        assert!(reader.buffer().is_empty());

        log::trace!(
            "Calculating hash of raw bytes (given size is {} megabytes)",
            data_size / MEGABYTE
        );

        let mut hasher = Blake3Hasher::new();
        let mut bytes_read: u32 = 0;
        loop {
            let bytes_read_iteration: usize = reader.fill_buf()?.len();
            if bytes_read_iteration == 0 {
                break;
            }
            hasher.update(reader.buffer());
            reader.consume(bytes_read_iteration);
            bytes_read +=
                u32::try_from(bytes_read_iteration).map_err(|_| {
                    ArklibError::Other(anyhow!("Can't convert usize to u32"))
                })?;
        }

        let blake3 = hasher.finalize();
        log::trace!("[compute] {} bytes has been read", bytes_read);
        log::trace!("[compute] blake3 hash: {}", blake3);
        assert_eq!(std::convert::Into::<u64>::into(bytes_read), data_size);

        Ok(ResourceId {
            data_size,
            blake3: blake3.into(),
        })
    }
}

const KILOBYTE: u64 = 1024;
const MEGABYTE: u64 = 1024 * KILOBYTE;
const BUFFER_CAPACITY: usize = 512 * KILOBYTE as usize;

#[cfg(test)]
mod tests {
    use crate::initialize;

    use super::*;

    #[test]
    fn resource_id_to_and_from_string() {
        let plain_text = "Hello, world!";
        let mut hasher = Blake3Hasher::new();
        hasher.update(plain_text.as_bytes());
        let blake3 = hasher.finalize();

        let id = ResourceId {
            data_size: plain_text.len() as u64,
            blake3: blake3.into(),
        };

        let id_str = id.to_string();
        let id2 = id_str.parse::<ResourceId>().unwrap();

        assert_eq!(id, id2);
    }

    #[test]
    fn compute_id_test() {
        initialize();

        let file_path = Path::new("./tests/lena.jpg");
        let data_size = fs::metadata(file_path)
            .unwrap_or_else(|_| {
                panic!(
                    "Could not open image test file_path.{}",
                    file_path.display()
                )
            })
            .len();

        let id1 = ResourceId::compute(data_size, file_path).unwrap();
        assert_eq!(
            id1.blake3,
            [
                23, 43, 75, 241, 72, 232, 88, 177, 61, 222, 15, 198, 97, 52,
                19, 188, 183, 85, 46, 92, 78, 92, 69, 25, 90, 198, 200, 15, 32,
                235, 95, 245
            ]
        );
        assert_eq!(id1.data_size, 128760);

        let raw_bytes = fs::read(file_path).unwrap();
        let id2 = ResourceId::compute_bytes(raw_bytes.as_slice()).unwrap();
        assert_eq!(
            id2.blake3,
            [
                23, 43, 75, 241, 72, 232, 88, 177, 61, 222, 15, 198, 97, 52,
                19, 188, 183, 85, 46, 92, 78, 92, 69, 25, 90, 198, 200, 15, 32,
                235, 95, 245
            ]
        );
        assert_eq!(id2.data_size, 128760);
    }

    #[test]
    fn resource_id_order() {
        let id1 = ResourceId {
            data_size: 1,
            blake3: [
                23, 43, 75, 241, 72, 232, 88, 177, 61, 222, 15, 198, 97, 52,
                19, 188, 183, 85, 46, 92, 78, 92, 69, 25, 90, 198, 200, 15, 32,
                235, 95, 245,
            ],
        };
        let id2 = ResourceId {
            data_size: 2,
            blake3: [
                23, 43, 75, 241, 72, 232, 88, 177, 61, 222, 15, 198, 97, 52,
                19, 188, 183, 85, 46, 92, 78, 92, 69, 25, 90, 198, 200, 15, 32,
                235, 95, 245,
            ],
        };

        assert!(id1 < id2);
        assert!(id2 > id1);
        assert!(id1 != id2);
        assert!(id1 == id1);
        assert!(id2 == id2);
    }
}
