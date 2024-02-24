use core::fmt::Display;
use core::str::FromStr;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::hash::Hash;
use std::io::BufReader;
use std::io::Read;
use std::path::Path;

use crate::Result;

mod crc32;

pub use crc32::ResourceIdCrc32 as ResourceId;

pub trait ResourceIdTrait<'de>:
    Display
    + FromStr
    + Clone
    + PartialEq
    + Eq
    + Ord
    + PartialOrd
    + Debug
    + Hash
    + Serialize
    + Deserialize<'de>
    + Copy
{
    type HashType: Display
        + FromStr
        + Clone
        + PartialEq
        + Eq
        + Ord
        + PartialOrd
        + Debug
        + Hash
        + Serialize
        + Deserialize<'de>
        + Copy;

    fn get_hash(&self) -> Self::HashType;

    fn compute<P: AsRef<Path>>(data_size: u64, file_path: P) -> Result<Self>;
    fn compute_bytes(bytes: &[u8]) -> Result<Self>;
    fn compute_reader<R: Read>(
        data_size: u64,
        reader: &mut BufReader<R>,
    ) -> Result<Self>;
}
