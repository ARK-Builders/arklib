use crate::id::ResourceId;
use canonical_path::CanonicalPathBuf;
use std::str::FromStr;

#[derive(Debug)]
pub struct IndexCache {
    pub path: CanonicalPathBuf,
    pub id: ResourceId,
}

impl ToString for IndexCache {
    fn to_string(&self) -> String {
        todo!()
    }
}

// impl FromStr for IndexCache {
//     type Err = ;
//     fn from_str(s: &str) -> Result<Self, Self::Err> {
//         todo!()
//     }
// }

impl IndexCache {
    pub fn parse(input: &str) -> Result<&str, Self> {
        todo!()
    }
}
