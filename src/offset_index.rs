use std::slice;
use std::mem::size_of;
use spin::Mutex as SpinMutex;

#[derive(Debug)]
pub struct OffsetIndex {
    id_offset: u64,
    index: SpinMutex<Vec<u32>>,
}

impl OffsetIndex {
    pub fn new(id_offset: u64) -> OffsetIndex {
        OffsetIndex {
            id_offset: id_offset,
            index: SpinMutex::new(Vec::with_capacity(10 * 1024)),
        }
    }

    pub fn from_bytes(id_offset: u64, bytes: &[u8]) -> OffsetIndex {
        assert!(bytes.len() % size_of::<u32>() == 0, "Bytes len is invalid");
        let pairs: &[u32] = unsafe {
            slice::from_raw_parts(
                bytes.as_ptr() as *const u32, bytes.len() / size_of::<u32>()
            )
        };

        let index = Self::new(id_offset);
        index.index.lock().extend(pairs);
        index
    }

    pub fn as_bytes(&self) -> &[u8] {
        let locked_index = self.index.lock();
        unsafe {
            slice::from_raw_parts(
                locked_index.as_ptr() as *const u8, size_of::<u32>() * locked_index.len()
            )
        }
    }

    pub fn get_offset(&self, id: u64) -> Option<u32> {
        debug_assert!(id >= self.id_offset);
        self.index.lock().get((id - self.id_offset) as usize).cloned()
    }

    pub fn push_offset(&mut self, id: u64, offset: u32) {
        let mut locked_index = self.index.lock();
        assert_eq!(id, self.id_offset + locked_index.len() as u64);
        locked_index.push(offset);
    }
}
