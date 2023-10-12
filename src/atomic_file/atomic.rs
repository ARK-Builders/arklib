use std::fs::{self, File};
use std::io::{Error, ErrorKind, Read, Result};
#[cfg(target_os = "unix")]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

pub struct TmpFile {
    file: File,
    path: PathBuf,
}

impl TmpFile {
    pub fn create_in(temp_dir: impl AsRef<Path>) -> Result<Self> {
        let filename: String = std::iter::repeat_with(fastrand::alphanumeric)
            .take(10)
            .collect();
        let path = temp_dir.as_ref().join(&filename);
        let file = std::fs::File::create(&path)?;
        Ok(Self { file, path })
    }
}

impl std::io::Read for &TmpFile {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        (&self.file).read(buf)
    }
}

impl std::io::Write for &TmpFile {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        (&self.file).write(buf)
    }

    fn flush(&mut self) -> Result<()> {
        (&self.file).flush()
    }
}

impl Drop for TmpFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[derive(Clone)]
pub struct ReadOnlyFile {
    version: usize,
    path: PathBuf,
}

/// This struct is the only way to read the file. Both path and version are private
impl ReadOnlyFile {
    /// Open the underlying file, which can be read from but not written to.
    /// May return `Ok(None)`, which means that no version
    /// of the`AtomicFile` has been created yet.
    pub fn open(&self) -> Result<Option<File>> {
        if self.version != 0 {
            Ok(Some(File::open(&self.path)?))
        } else {
            Ok(None)
        }
    }

    pub fn read_to_string(&self) -> Result<String> {
        match self.open() {
            Ok(None) => Err(Error::new(ErrorKind::NotFound, "File not found")),
            Ok(Some(mut file)) => {
                let mut buff = String::new();
                file.read_to_string(&mut buff)?;
                Ok(buff)
            }
            Err(e) => Err(e),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct AtomicFile {
    directory: PathBuf,
    prefix: String,
}

fn parse_version(filename: &std::ffi::OsStr, prefix: &str) -> Option<usize> {
    let filename = filename.to_str()?;
    if !filename.starts_with(prefix) {
        return None;
    }
    filename[prefix.len()..].parse().ok()
}

impl AtomicFile {
    pub fn new(path: impl Into<PathBuf>) -> Result<Self> {
        let directory = path.into();
        std::fs::create_dir_all(&directory)?;
        let filename: &str = match directory.file_name() {
            Some(name) => name.to_str().unwrap(),
            None => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "`path` must specify a directory name",
                ));
            }
        };
        let prefix = format!("{}.", filename);
        Ok(Self { directory, prefix })
    }

    fn latest_version(&self) -> Result<usize> {
        let mut max_version = 0;
        for entry in fs::read_dir(&self.directory)? {
            if let Some(version) =
                parse_version(&entry?.file_name(), &self.prefix)
            {
                max_version = std::cmp::max(max_version, version);
            }
        }
        Ok(max_version)
    }

    fn path(&self, version: usize) -> PathBuf {
        self.directory
            .join(format!("{}{version}", self.prefix))
    }

    pub fn load(&self) -> Result<ReadOnlyFile> {
        let version = self.latest_version()?;
        let path = self.path(version);
        Ok(ReadOnlyFile { version, path })
    }

    pub fn make_temp(&self) -> Result<TmpFile> {
        TmpFile::create_in(&self.directory)
    }

    /// Replace the contents of the file with the contents of `new` if the
    /// latest version is the same as `current`.
    ///
    /// # Errors
    /// If `io::ErrorKind::AlreadyExists` is returned, it means that the latest
    /// version was not the same as `current` and the operation must be retried
    /// with a fresher version of the file. Any other I/O error is forwarded as
    /// well.
    pub fn compare_and_swap(
        &self,
        current: &ReadOnlyFile,
        new: TmpFile,
    ) -> Result<()> {
        let new_path = self.path(current.version + 1);
        (&new.file).sync_data()?;
        // May return `EEXIST`.
        let res = std::fs::hard_link(&new.path, &new_path);
        if let Err(err) = res {
            #[cfg(target_os = "unix")]
            // From open(2) manual page:
            //
            // "[...] create a unique file on the same filesystem (e.g.,
            // incorporating hostname and PID), and use link(2) to make a link
            // to the lockfile. If link(2) returns 0, the lock is successful.
            // Otherwise, use stat(2) on the unique file to check if its link
            // count has increased to 2, in which case the lock is also
            // succesful."
            if new.path.metadata()?.nlink() != 2 {
                Err(err)?;
            }
            #[cfg(not(target_os = "unix"))]
            Err(err)?;
        }
        Ok(())
    }
}
