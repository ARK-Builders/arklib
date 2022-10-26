pub mod cache;
use std::{
    fs,
    path::{Path, PathBuf},
};

use std::time::SystemTime;
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};

use canonical_path::CanonicalPathBuf;
use walkdir::{DirEntry, WalkDir};

use anyhow::Error;
use log;
use nom::{sequence::preceded, IResult};

use crate::id::ResourceId;
use crate::meta::ResourceMeta;

pub const INDEX_CACHE_DIR: &'static str = ".ark/index";

pub const INDEX_DIR: &'static str = ".ark/index";

#[derive(Debug)]
pub struct ResourceIndex {
    // the data
    pub path2meta: HashMap<CanonicalPathBuf, ResourceMeta>,
    // collisions record to avoid hash collisions
    pub collisions: HashMap<ResourceId, usize>,
    // id records for hash collisions check
    ids: HashSet<ResourceId>,
    // the root path
    root: PathBuf,
}
// Since we treat outdated index as deleted index and updated index as added index,
// it's no need to keep a updated index record.
#[derive(Debug)]
pub struct IndexUpdate {
    pub deleted: HashMap<CanonicalPathBuf, ResourceMeta>,
    pub added: HashMap<CanonicalPathBuf, ResourceMeta>,
}

// pub struct Difference {
//     pub updated: Vec<CanonicalPathBuf>,
//     pub deleted: Vec<CanonicalPathBuf>,
//     pub added: Vec<CanonicalPathBuf>,
// }

impl ResourceIndex {
    /// initial index from given path,
    pub fn from_resources<P: AsRef<Path>>(
        root_path: P,
        resources: HashMap<CanonicalPathBuf, ResourceMeta>,
    ) -> Self {
        // TODO Return Result
        log::info!("creating resource index from giving resources");
        let root = CanonicalPathBuf::canonicalize(root_path).unwrap();

        let mut index = Self {
            path2meta: HashMap::new(),
            collisions: HashMap::new(),
            ids: HashSet::new(),
            root: root.into_path_buf(),
        };
        // Avoid hash collision.
        for (path, meta) in resources {
            add_meta(
                path,
                meta,
                &mut index.path2meta,
                &mut index.collisions,
                &mut index.ids,
            );
        }
        log::info!("Index created from giving resources.");
        index
    }
    // pub fn calc_diff(&self) -> Difference {
    //     let (present, absend): (Vec<_>, Vec<_>) = self
    //         .path2meta
    //         .keys()
    //         .partition(|path| path.exists());

    //     let updated = present
    //         .iter()
    //         .map(|&it| (it, &self.path2meta[it]))
    //         .filter(|(path, meta)| {
    //             path.metadata().unwrap().modified().unwrap()
    //                 > Into::<SystemTime>::into(meta.modified)
    //         })
    //         .map(|(path, _)| path)
    //         .cloned()
    //         .collect::<Vec<_>>();
    //     let added: Vec<_> = discover_paths(&self.root)
    //         .iter()
    //         .filter(|(path, _)| !&self.path2meta.contains_key(*path))
    //         .map(|(path, _)| path)
    //         .cloned()
    //         .collect();
    //     log::debug!(
    //         "{} absent, {} updated, {} added",
    //         absend.len(),
    //         updated.len(),
    //         added.len()
    //     );
    //     let deleted = absend
    //         .iter()
    //         .cloned()
    //         .cloned()
    //         .collect::<Vec<_>>();
    //     Difference {
    //         updated,
    //         deleted,
    //         added,
    //     }
    // }
    /// Get the size of a index.
    ///
    /// NOTE: the actual size is lower in presence of collisions
    pub fn size(&self) -> usize {
        self.path2meta.len()
    }
    /// Build index from scratch.
    pub fn build<P: AsRef<Path>>(root_path: P) -> Result<Self, Error> {
        log::info!("Creating the index from scratch");

        let paths = discover_paths(root_path.as_ref().to_owned());
        let metadata = scan_metadata(&paths);

        let mut index = ResourceIndex {
            path2meta: HashMap::new(),
            collisions: HashMap::new(),
            ids: HashSet::new(),
            root: root_path.as_ref().to_owned(),
        };

        // avoid hash collision
        for (path, meta) in metadata {
            add_meta(
                path,
                meta,
                &mut index.path2meta,
                &mut index.collisions,
                &mut index.ids,
            );
        }

        log::info!("index built");
        return Ok(index);
    }
    /// re-discovery the root path and update index in memory
    pub fn update(&mut self) -> Result<IndexUpdate, Error> {
        log::info!("Updating the index");
        log::trace!("Known paths:\n{:?}", self.path2meta.keys());

        let curr_entries = discover_paths(self.root.clone());

        //assuming that collections manipulation is
        // quicker than asking `path.exists()` for every path
        let curr_paths: Paths = curr_entries.keys().cloned().collect();
        let prev_paths: Paths = self.path2meta.keys().cloned().collect();
        let preserved_paths: Paths = curr_paths
            .intersection(&prev_paths)
            .cloned()
            .collect();

        let created_paths: HashMap<CanonicalPathBuf, DirEntry> = curr_entries
            .clone()
            .into_iter()
            .filter_map(|(path, entry)| {
                if !preserved_paths.contains(path.as_canonical_path()) {
                    Some((path.clone(), entry.clone()))
                } else {
                    None
                }
            })
            .collect();

        log::info!("Checking updated paths");
        let updated_paths: HashMap<CanonicalPathBuf, DirEntry> = curr_entries
            .into_iter()
            .filter(|(path, entry)| {
                if !preserved_paths.contains(path.as_canonical_path()) {
                    false
                } else {
                    let prev_modified = self.path2meta[path].modified;

                    let result = entry.metadata();
                    match result {
                        Err(msg) => {
                            log::error!(
                                "Couldn't retrieve metadata for {}: {}",
                                &path.display(),
                                msg
                            );
                            false
                        }
                        Ok(metadata) => match metadata.modified() {
                            Err(msg) => {
                                log::error!(
                                    "Couldn't retrieve timestamp for {}: {}",
                                    &path.display(),
                                    msg
                                );
                                false
                            }
                            Ok(curr_modified) => {
                                curr_modified > SystemTime::from(prev_modified)
                            }
                        },
                    }
                }
            })
            .collect();

        let mut deleted = HashMap::new();

        // treating deleted and updated paths as deletions
        prev_paths
            .difference(&preserved_paths)
            .cloned()
            .chain(updated_paths.keys().cloned())
            .for_each(|path| {
                if let Some(meta) = self.path2meta.remove(&path) {
                    let k = self.collisions.remove(&meta.id).unwrap_or(1);
                    if k > 1 {
                        self.collisions.insert(meta.id, k - 1);
                    } else {
                        log::debug!("Removing {:?} from index", meta.id);
                        self.ids.remove(&meta.id);
                        deleted.insert(path, meta);
                    }
                } else {
                    log::warn!("Path {} was not known", path.display());
                }
            });

        let added: HashMap<CanonicalPathBuf, ResourceMeta> =
            scan_metadata(&updated_paths)
                .into_iter()
                .chain({
                    log::info!("The same for new paths");
                    scan_metadata(&created_paths).into_iter()
                })
                .filter(|(_, meta)| !self.ids.contains(&meta.id))
                .collect();

        for (path, meta) in added.iter() {
            if deleted.contains_key(path) {
                // emitting the resource as both deleted and added
                // (renaming a duplicate might remain undetected)
                log::info!(
                    "Resource {:?} was moved to {}",
                    meta.id,
                    path.display()
                );
            }

            add_meta(
                path.clone(),
                meta.clone(),
                &mut self.path2meta,
                &mut self.collisions,
                &mut self.ids,
            );
        }

        Ok(IndexUpdate { deleted, added })
    }

    pub fn remove(&mut self, id: i64) -> Option<CanonicalPathBuf> {
        log::info!("removing id: {id}");
        let iter = self.path2meta.clone().into_iter();
        let mut pair_iter = iter.filter(|(_, meta)| meta.id.crc32 == id as u32);
        let val = pair_iter.next();
        log::info!("Removed: {:#?}", val);
        match val {
            Some((path, _)) => self
                .path2meta
                .remove(path.as_canonical_path())
                .map(|_| path),
            None => None,
        }
    }
    pub fn list_resources(
        &self,
        prefix: Option<String>,
    ) -> HashMap<CanonicalPathBuf, ResourceMeta> {
        match prefix {
            Some(prefix) => self
                .path2meta
                .iter()
                .filter(|(path, _)| path.starts_with(prefix.clone()))
                .map(|(a, b)| (a.clone(), b.clone()))
                .collect(),
            None => self.path2meta.clone(),
        }
    }

    pub fn get_path(&self, id: i64) -> Option<CanonicalPathBuf> {
        self.path2meta
            .iter()
            .find(|&x| x.1.id.crc32 as i64 == id)
            .map(|(p, _)| p.clone())
    }

    pub fn get_meta(&self, id: i64) -> Option<ResourceMeta> {
        self.path2meta
            .iter()
            .find(|&x| x.1.id.crc32 as i64 == id)
            .map(|(_, m)| m.clone())
    }

    pub fn update_resource(
        &mut self,
        path: CanonicalPathBuf,
        new_resource: ResourceMeta,
    ) {
        let new_res_id_crc32 = new_resource.id.crc32;
        match self.path2meta.insert(path, new_resource) {
            Some(v) => {
                log::info!("updated resource: {}", v.id.crc32);
            }
            None => {
                log::warn!(
                    "resource not found, added the resource: {}",
                    new_res_id_crc32
                );
            }
        }
    }
    /// Check if an id is in the index
    pub fn contains(&self, id: i64) -> bool {
        self.ids
            .iter()
            .find(|x| x.crc32 == id as u32)
            .is_some()
    }
    // presist cache into fs
    pub fn cache(&self) {}
    pub fn list_ids(&self) -> HashSet<ResourceId> {
        self.ids.clone()
    }
}

impl TryFrom<PathBuf> for ResourceIndex {
    type Error = Error;
    fn try_from(path: PathBuf) -> Result<Self, Self::Error> {
        let root = CanonicalPathBuf::canonicalize(path).unwrap();
        let index = fs::File::open(root.as_path())?;

        todo!()
    }
}

fn discover_paths<P: AsRef<Path>>(
    root_path: P,
) -> HashMap<CanonicalPathBuf, DirEntry> {
    log::info!(
        "Discovering all files under path {}",
        root_path.as_ref().display()
    );

    WalkDir::new(root_path)
        .into_iter()
        .filter_entry(|entry| !is_hidden(entry))
        .filter_map(|result| match result {
            Ok(entry) => {
                let path = entry.path();
                if !entry.file_type().is_dir() {
                    match CanonicalPathBuf::canonicalize(path) {
                        Ok(canonical_path) => Some((canonical_path, entry)),
                        Err(msg) => {
                            log::error!(
                                "Couldn't canonicalize {}:\n{}",
                                path.display(),
                                msg
                            );
                            None
                        }
                    }
                } else {
                    None
                }
            }
            Err(msg) => {
                log::error!("Error during walking: {}", msg);
                None
            }
        })
        .collect()
}

fn scan_metadata(
    entries: &HashMap<CanonicalPathBuf, DirEntry>,
) -> HashMap<CanonicalPathBuf, ResourceMeta> {
    log::info!("Scanning metadata");

    entries
        .into_iter()
        .filter_map(|(path, entry)| {
            log::trace!("\n\t{:?}\n\t\t{:?}", path, entry);

            let result = ResourceMeta::scan(path, entry);
            match result {
                Err(msg) => {
                    log::error!(
                        "Couldn't retrieve metadata for {}:\n{}",
                        path.display(),
                        msg
                    );
                    None
                }
                Ok(meta) => Some(meta),
            }
        })
        .collect()
}

// safely add meta by checking id to avoid hash collision.
fn add_meta(
    path: CanonicalPathBuf,
    meta: ResourceMeta,
    path2meta: &mut HashMap<CanonicalPathBuf, ResourceMeta>,
    collisions: &mut HashMap<ResourceId, usize>,
    ids: &mut HashSet<ResourceId>,
) {
    let id = meta.id.clone();
    path2meta.insert(path, meta);

    if ids.contains(&id) {
        if let Some(nonempty) = collisions.get_mut(&id) {
            *nonempty += 1;
        } else {
            collisions.insert(id, 2);
        }
    } else {
        ids.insert(id.clone());
    }
}

fn is_hidden(entry: &DirEntry) -> bool {
    entry
        .file_name()
        .to_str()
        .map(|s| s.starts_with("."))
        .unwrap_or(false)
}

type Paths = HashSet<CanonicalPathBuf>;

#[cfg(test)]
mod tests {
    use crate::id::ResourceId;
    use crate::index::discover_paths;
    use crate::ResourceIndex;
    use canonical_path::CanonicalPathBuf;
    use std::fs::{File, Permissions};
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use uuid::Uuid;

    const FILE_SIZE_1: u64 = 10;
    const FILE_SIZE_2: u64 = 11;

    const FILE_NAME_1: &str = "test1.txt";
    const FILE_NAME_2: &str = "test2.txt";
    const FILE_NAME_3: &str = "test3.txt";

    const CRC32_1: u32 = 3817498742;
    const CRC32_2: u32 = 1804055020;

    fn get_temp_dir() -> PathBuf {
        create_dir_at(std::env::temp_dir())
    }

    fn create_dir_at(path: PathBuf) -> PathBuf {
        let mut dir_path = path.clone();
        dir_path.push(Uuid::new_v4().to_string());
        std::fs::create_dir(&dir_path).expect("Could not create temp dir");
        dir_path
    }

    fn create_file_at(
        path: PathBuf,
        size: Option<u64>,
        name: Option<&str>,
    ) -> (File, PathBuf) {
        let mut file_path = path.clone();
        if let Some(file_name) = name {
            file_path.push(file_name);
        } else {
            file_path.push(Uuid::new_v4().to_string());
        }
        let file = File::create(file_path.clone())
            .expect("Could not create temp file");
        file.set_len(size.unwrap_or(0))
            .expect("Could not set file size");
        (file, file_path)
    }

    fn run_test_and_clean_up(
        test: impl FnOnce(PathBuf) -> () + std::panic::UnwindSafe,
    ) -> () {
        let path = get_temp_dir();
        let result = std::panic::catch_unwind(|| test(path.clone()));
        std::fs::remove_dir_all(path.clone())
            .expect("Could not clean up after test");
        if result.is_err() {
            panic!("{}", result.err().map(|_| "Test panicked").unwrap())
        }
        assert!(result.is_ok());
    }

    // resource index build

    #[test]
    fn should_build_resource_index_with_1_file_successfully() {
        run_test_and_clean_up(|path| {
            create_file_at(path.clone(), Some(FILE_SIZE_1), None);

            let actual = ResourceIndex::build(path.clone())
                .expect("Could not build index");

            assert_eq!(actual.root, path.clone());
            assert_eq!(actual.path2meta.len(), 1);
            assert_eq!(actual.ids.len(), 1);
            assert!(actual.ids.contains(&ResourceId {
                file_size: FILE_SIZE_1,
                crc32: CRC32_1,
            }));
            assert_eq!(actual.collisions.len(), 0);
            assert_eq!(actual.size(), 1);
        })
    }

    #[test]
    fn should_index_colliding_files_correctly() {
        run_test_and_clean_up(|path| {
            create_file_at(path.clone(), Some(FILE_SIZE_1), None);
            create_file_at(path.clone(), Some(FILE_SIZE_1), None);

            let actual = ResourceIndex::build(path.clone())
                .expect("Could not build index");

            assert_eq!(actual.root, path.clone());
            assert_eq!(actual.path2meta.len(), 2);
            assert_eq!(actual.ids.len(), 1);
            assert!(actual.ids.contains(&ResourceId {
                file_size: FILE_SIZE_1,
                crc32: CRC32_1,
            }));
            assert_eq!(actual.collisions.len(), 1);
            assert_eq!(actual.size(), 2);
        })
    }

    // resource index update

    #[test]
    fn should_update_index_with_renamed_file_correctly() {
        run_test_and_clean_up(|path| {
            create_file_at(path.clone(), Some(FILE_SIZE_1), Some(FILE_NAME_1));
            create_file_at(path.clone(), Some(FILE_SIZE_2), Some(FILE_NAME_2));

            let mut actual = ResourceIndex::build(path.clone())
                .expect("Could not build index");

            assert_eq!(actual.collisions.len(), 0);
            assert_eq!(actual.size(), 2);

            // rename test2.txt to test3.txt
            let mut name_from = path.clone();
            name_from.push(FILE_NAME_2);
            let mut name_to = path.clone();
            name_to.push(FILE_NAME_3);
            std::fs::rename(name_from, name_to)
                .expect("Should rename file successfully");

            let update = actual
                .update()
                .expect("Should update index correctly");

            assert_eq!(actual.collisions.len(), 0);
            assert_eq!(actual.size(), 2);
            assert_eq!(update.deleted.len(), 1);
            assert_eq!(update.added.len(), 1);
        })
    }

    #[test]
    fn should_update_resource_index_adding_1_additional_file_successfully() {
        run_test_and_clean_up(|path| {
            create_file_at(path.clone(), Some(FILE_SIZE_1), None);

            let mut actual = ResourceIndex::build(path.clone())
                .expect("Could not build index");

            let (_, expected_path) =
                create_file_at(path.clone(), Some(FILE_SIZE_2), None);

            let update = actual
                .update()
                .expect("Should update index correctly");

            assert_eq!(actual.root, path.clone());
            assert_eq!(actual.path2meta.len(), 2);
            assert_eq!(actual.ids.len(), 2);
            assert!(actual.ids.contains(&ResourceId {
                file_size: FILE_SIZE_1,
                crc32: CRC32_1,
            }));
            assert!(actual.ids.contains(&ResourceId {
                file_size: FILE_SIZE_2,
                crc32: CRC32_2,
            }));
            assert_eq!(actual.collisions.len(), 0);
            assert_eq!(actual.size(), 2);
            assert_eq!(update.deleted.len(), 0);
            assert_eq!(update.added.len(), 1);

            let added_key =
                CanonicalPathBuf::canonicalize(&expected_path.clone())
                    .expect("CanonicalPathBuf should be fine");
            assert_eq!(
                update
                    .added
                    .get(&added_key)
                    .expect("Key exists")
                    .id,
                ResourceId {
                    file_size: FILE_SIZE_2,
                    crc32: CRC32_2
                }
            )
        })
    }

    #[test]
    fn should_update_resource_index_deleting_1_additional_file_successfully() {
        run_test_and_clean_up(|path| {
            create_file_at(path.clone(), Some(FILE_SIZE_1), Some(FILE_NAME_1));

            let mut actual = ResourceIndex::build(path.clone())
                .expect("Could not build index");

            let mut file_path = path.clone();
            file_path.push(FILE_NAME_1);
            let updated_path =
                CanonicalPathBuf::new(file_path.clone()).unwrap();
            std::fs::remove_file(&file_path)
                .expect("Should remove file successfully");
            let update = actual
                .update()
                .expect("Should update index successfully");
            println!("{:?}", update);
            assert_eq!(actual.root, path.clone());
            assert_eq!(actual.path2meta.len(), 0);
            assert_eq!(actual.ids.len(), 0);
            assert_eq!(actual.collisions.len(), 0);
            assert_eq!(actual.size(), 0);
            assert_eq!(update.deleted.len(), 1);
            assert_eq!(update.added.len(), 0);

            assert!(update.deleted.contains_key(&updated_path))
        })
    }

    #[test]
    fn should_not_update_index_on_files_without_permissions() {
        run_test_and_clean_up(|path| {
            create_file_at(path.clone(), Some(FILE_SIZE_1), Some(FILE_NAME_1));
            let (file, _) = create_file_at(
                path.clone(),
                Some(FILE_SIZE_2),
                Some(FILE_NAME_2),
            );

            let mut actual = ResourceIndex::build(path.clone())
                .expect("Could not build index");

            assert_eq!(actual.collisions.len(), 0);
            assert_eq!(actual.size(), 2);

            file.set_permissions(Permissions::from_mode(0o222))
                .expect("Should be fine");

            let update = actual
                .update()
                .expect("Should update index correctly");

            assert_eq!(actual.collisions.len(), 0);
            assert_eq!(actual.size(), 2);
            assert_eq!(update.deleted.len(), 0);
            assert_eq!(update.added.len(), 0);
        })
    }

    // error cases

    #[test]
    fn should_not_index_empty_file() {
        run_test_and_clean_up(|path| {
            create_file_at(path.clone(), Some(0), None);
            let actual = ResourceIndex::build(path.clone())
                .expect("Could not generate index");

            assert_eq!(actual.root, path.clone());
            assert_eq!(actual.path2meta.len(), 0);
            assert_eq!(actual.ids.len(), 0);
            assert_eq!(actual.collisions.len(), 0);
        })
    }

    #[test]
    fn should_not_index_hidden_file() {
        run_test_and_clean_up(|path| {
            create_file_at(path.clone(), Some(FILE_SIZE_1), Some(".hidden"));
            let actual = ResourceIndex::build(path.clone())
                .expect("Could not generate index");

            assert_eq!(actual.root, path.clone());
            assert_eq!(actual.path2meta.len(), 0);
            assert_eq!(actual.ids.len(), 0);
            assert_eq!(actual.collisions.len(), 0);
        })
    }

    #[test]
    fn should_not_index_1_empty_directory() {
        run_test_and_clean_up(|path| {
            create_dir_at(path.clone());

            let actual = ResourceIndex::build(path.clone())
                .expect("Could not build index");

            assert_eq!(actual.root, path.clone());
            assert_eq!(actual.path2meta.len(), 0);
            assert_eq!(actual.ids.len(), 0);
            assert_eq!(actual.collisions.len(), 0);
        })
    }

    #[test]
    fn should_fail_when_indexing_file_without_read_rights() {
        run_test_and_clean_up(|path| {
            let (file, _) = create_file_at(path.clone(), Some(1), None);
            file.set_permissions(Permissions::from_mode(0o222))
                .expect("Should be fine");

            let actual =
                std::panic::catch_unwind(|| ResourceIndex::build(path.clone()));
            assert!(actual.is_err());
        })
    }

    #[test]
    fn discover_paths_should_not_walk_on_invalid_path() {
        run_test_and_clean_up(|path| {
            let mut missing_path = path.clone();
            missing_path.push("missing/directory");
            let actual = discover_paths(missing_path);
            assert_eq!(actual.len(), 0);
        })
    }
}
