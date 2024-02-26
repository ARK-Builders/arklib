use anyhow::anyhow;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::engine::Engine as _;
use blake3::Hasher as Blake3Hasher;
use log;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use std::fs;
use std::io::Read;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::str::FromStr;

use crate::resource::ResourceIdTrait;
use crate::{ArklibError, Result};

const KILOBYTE: u64 = 1024;
const MEGABYTE: u64 = 1024 * KILOBYTE;
const BUFFER_CAPACITY: usize = 512 * KILOBYTE as usize;

/// Represents a resource identifier using the BLAKE3 algorithm.
///
/// Uses `blake3` crate to compute the hash value.
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
pub struct ResourceIdBlake3 {
    pub data_size: u64,
    pub hash: [u8; 32],
}

impl Display for ResourceIdBlake3 {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let hash_base64 = BASE64.encode(self.hash);
        write!(f, "{}-{}", self.data_size, hash_base64)
    }
}

impl FromStr for ResourceIdBlake3 {
    type Err = ArklibError;

    fn from_str(s: &str) -> Result<Self> {
        let (l, r) = s.split_once('-').ok_or(ArklibError::Parse)?;
        let data_size: u64 = l.parse().map_err(|_| ArklibError::Parse)?;
        let hash_vec = BASE64
            .decode(r.as_bytes())
            .map_err(|_| ArklibError::Parse)?;
        let mut hash = [0; 32];
        hash.copy_from_slice(&hash_vec);

        Ok(ResourceIdBlake3 { data_size, hash })
    }
}

impl ResourceIdTrait<'_> for ResourceIdBlake3 {
    type HashType = [u8; 32];

    fn get_hash(&self) -> Self::HashType {
        self.hash
    }

    fn compute<P: AsRef<Path>>(data_size: u64, file_path: P) -> Result<Self> {
        log::trace!(
            "[compute] file {} with size {} mb",
            file_path.as_ref().display(),
            data_size / MEGABYTE
        );

        let source = fs::OpenOptions::new()
            .read(true)
            .open(file_path.as_ref())?;

        let mut reader = BufReader::with_capacity(BUFFER_CAPACITY, source);
        ResourceIdBlake3::compute_reader(data_size, &mut reader)
    }

    fn compute_bytes(bytes: &[u8]) -> Result<Self> {
        let data_size = bytes.len().try_into().map_err(|_| {
            ArklibError::Other(anyhow!("Can't convert usize to u64"))
        })?;
        let mut reader = BufReader::with_capacity(BUFFER_CAPACITY, bytes);
        ResourceIdBlake3::compute_reader(data_size, &mut reader)
    }

    fn compute_reader<R: Read>(
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

        let hash = hasher.finalize();
        log::trace!("[compute] {} bytes has been read", bytes_read);
        log::trace!("[compute] blake3 hash: {}", hash);
        assert_eq!(std::convert::Into::<u64>::into(bytes_read), data_size);

        Ok(ResourceIdBlake3 {
            data_size,
            hash: hash.into(),
        })
    }
}

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

        let id = ResourceIdBlake3 {
            data_size: 13,
            hash: blake3.into(),
        };

        let id_str = id.to_string();
        let id2 = id_str.parse::<ResourceIdBlake3>().unwrap();

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

        let id1 = ResourceIdBlake3::compute(data_size, file_path).unwrap();
        assert_eq!(
            id1.get_hash(),
            [
                23, 43, 75, 241, 72, 232, 88, 177, 61, 222, 15, 198, 97, 52,
                19, 188, 183, 85, 46, 92, 78, 92, 69, 25, 90, 198, 200, 15, 32,
                235, 95, 245
            ]
        );
        assert_eq!(id1.data_size, 128760);

        let raw_bytes = fs::read(file_path).unwrap();
        let id2 =
            ResourceIdBlake3::compute_bytes(raw_bytes.as_slice()).unwrap();
        assert_eq!(
            id2.get_hash(),
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
        let id1 = ResourceIdBlake3 {
            data_size: 1,
            hash: [
                23, 43, 75, 241, 72, 232, 88, 177, 61, 222, 15, 198, 97, 52,
                19, 188, 183, 85, 46, 92, 78, 92, 69, 25, 90, 198, 200, 15, 32,
                235, 95, 245,
            ],
        };
        let id2 = ResourceIdBlake3 {
            data_size: 2,
            hash: [
                24, 43, 75, 241, 72, 232, 88, 177, 61, 222, 15, 198, 97, 52,
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
