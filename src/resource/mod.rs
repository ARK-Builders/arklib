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

/// This trait defines a generic type representing a resource identifier.
///
/// Resources are identified by a hash value, which is computed from the resource's data.
/// The hash value is used to uniquely identify the resource.
///
/// Implementors of this trait must provide a way to compute the hash value from the resource's data.
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
where
    Self::HashType: Display
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
        + Copy,
{
    /// Associated type representing the hash used by this resource identifier.
    type HashType;

    /// Returns the hash value of the resource.
    fn get_hash(&self) -> Self::HashType;

    /// Creates a new resource identifier from the given path.
    ///
    /// # Arguments
    /// * `data_size` - Size of the data being identified.
    /// * `file_path` - Path to the file containing the data.
    fn compute<P: AsRef<Path>>(data_size: u64, file_path: P) -> Result<Self>;

    /// Creates a new resource identifier from raw bytes.
    ///
    /// # Arguments
    /// * `bytes` - Byte array containing the data to be identified.
    fn compute_bytes(bytes: &[u8]) -> Result<Self>;

    /// Creates a new resource identifier from a buffered reader.
    ///
    /// # Arguments
    /// * `data_size` - Size of the data being read.
    /// * `reader` - Buffered reader providing access to the data.
    fn compute_reader<R: Read>(
        data_size: u64,
        reader: &mut BufReader<R>,
    ) -> Result<Self>;
}
