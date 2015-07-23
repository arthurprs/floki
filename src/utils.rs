use std::collections::HashMap as StdHashMap;
pub use linked_hash_map::LinkedHashMap as StdLinkedHashMap;
pub use std::collections::hash_state::DefaultState;
pub use fnv::FnvHasher;

pub type HashMap<K, V> = StdHashMap<K, V, DefaultState<FnvHasher>>;
pub type LinkedHashMap<K, V> = StdLinkedHashMap<K, V, DefaultState<FnvHasher>>;