use std::error::Error;
use std::fs;
use std::io;
use std::path::Path;
use std::collections::{HashMap as StdHashMap, HashSet as StdHashSet};
pub use linked_hash_map::LinkedHashMap as StdLinkedHashMap;
pub use std::collections::hash_state::DefaultState;
pub use fnv::FnvHasher;

pub type GenericError = Box<Error + Send + Sync>;
pub type HashMap<K, V> = StdHashMap<K, V, DefaultState<FnvHasher>>;
pub type HashSet<K> = StdHashSet<K, DefaultState<FnvHasher>>;
pub type LinkedHashMap<K, V> = StdLinkedHashMap<K, V, DefaultState<FnvHasher>>;

pub fn remove_file_if_exist<P: AsRef<Path>>(path: P) -> io::Result<()> {
    match fs::remove_file(path.as_ref()) {
        Err(ref err) if err.kind() == io::ErrorKind::NotFound => {
            Ok(())
        }
        result => result
    }
}

pub fn remove_dir_if_exist<P: AsRef<Path>>(path: P) -> io::Result<()> {
    match fs::remove_dir_all(path.as_ref()) {
        Err(ref err) if err.kind() == io::ErrorKind::NotFound => {
            Ok(())
        }
        result => result
    }
}

pub fn create_dir_if_not_exist<P: AsRef<Path>>(path: P) -> io::Result<()> {
    match fs::create_dir_all(path.as_ref()) {
        Err(ref err) if err.kind() == io::ErrorKind::AlreadyExists => {
            // small hack to detect race conditions
            // harmless on the real world but without it tests runing in parallel may fail
            if !path.as_ref().is_dir() {
                return create_dir_if_not_exist(path)
            }
            Ok(())
        }
        result => result
    }
}

macro_rules! assert_eq_repr {
    ($left:expr , $right:expr) => ({
        match (format!("{:?}", &$left), format!("{:?}", &$right)) {
            (left_val, right_val) => {
                if !(left_val == right_val) {
                    panic!("repr assertion failed: `(debug(left) == debug(right))` \
                           (left: `{:?}`, right: `{:?}`)", left_val, right_val)
                }
            }
        }
    })
}
