use std::slice;
use std::mem::size_of;

#[derive(Debug)]
pub struct OffsetIndex {
    id_offset: u64,
    index: Vec<u32>,
}

impl OffsetIndex {
    pub fn new(id_offset: u64) -> OffsetIndex {
        OffsetIndex {
            id_offset: id_offset,
            index: Vec::with_capacity(1024),
        }
    }

    pub fn from_bytes(id_offset: u64, bytes: &[u8]) -> OffsetIndex {
        assert!(bytes.len() % size_of::<u32>() == 0, "Bytes len is invalid");
        let pairs: &[u32] = unsafe {
            slice::from_raw_parts(
                bytes.as_ptr() as *const u32, bytes.len() / size_of::<u32>()
            )
        };

        let mut index = Self::new(id_offset);
        index.index.extend(pairs);
        index
    }

    pub fn as_bytes(&self) -> &[u8] {
        unsafe {
            slice::from_raw_parts(
                self.index.as_ptr() as *const u8, size_of::<u32>() * self.index.len()
            )
        }
    }

    pub fn get_offset(&self, id: u64) -> Option<u32> {
        debug_assert!(id >= self.id_offset);
        self.index.get((id - self.id_offset) as usize).cloned()
    }

    pub fn push_offset(&mut self, id: u64, offset: u32) {
        assert_eq!(id, self.id_offset + self.index.len() as u64);
        self.index.push(offset);
    }
}
