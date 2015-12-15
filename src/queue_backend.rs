use std::ptr;
use std::slice;
use std::thread;
use std::cmp;
use std::mem::{self, size_of};
use std::io;
use std::io::prelude::*;
use std::time::Duration;
use std::os::unix::io::{RawFd, AsRawFd};
use std::fs::{self, File, OpenOptions};
use std::path::PathBuf;
use std::sync::Arc;
use nix::c_void;
use nix::sys::mman;
use spin::{Mutex as SpinLock, RwLock as SpinRwLock};
use rustc_serialize::json;
use twox_hash::XxHash;
use std::hash::Hasher;
use nix;
use fs2::FileExt;

use config::*;
use utils::*;
use offset_index::*;

const MAGIC_NUM: u32 = 0xF1031311u32;
const INVALID_TIMESTAMP: u32 = 0;

pub type QueueBackendResult<T> = Result<T, QueueBackendError>;

#[derive(Debug)]
pub enum QueueBackendError {
    MessageTooBig,
    SegmentFull,
    SegmentFileInvalid,
    Io(io::Error),
}

impl From<io::Error> for QueueBackendError {
    fn from(from: io::Error) -> QueueBackendError {
        QueueBackendError::Io(from)
    }
}

impl From<nix::Error> for QueueBackendError {
    fn from(from: nix::Error) -> QueueBackendError {
        QueueBackendError::Io(from.into())
    }
}

#[derive(Debug, Clone)]
struct InnerMessage {
    mmap_ptr: *const MessageHeader,
}

#[derive(Debug, Clone)]
pub struct Message {
    segment: Arc<Segment>,
    inner: InnerMessage,
}

impl Message {
    pub fn id(&self) -> u64 {
        unsafe { (*self.inner.mmap_ptr).id }
    }

    pub fn timestamp(&self) -> u32 {
        unsafe { (*self.inner.mmap_ptr).timestamp }
    }

    pub fn body(&self) -> &[u8] {
        unsafe {
            slice::from_raw_parts(
                (self.inner.mmap_ptr as *const u8)
                    .offset(size_of::<MessageHeader>() as isize),
                (*self.inner.mmap_ptr).len as usize)
        }
    }

    pub fn fd(&self) -> RawFd {
        self.segment.file.as_raw_fd()
    }

    pub fn fd_offset(&self) -> u32 {
        (self.inner.mmap_ptr as usize - self.segment.file_mmap as usize) as u32
    }
}

unsafe impl Send for Message {}

#[repr(packed)]
struct MessageHeader {
    hash: u32,
    id: u64,
    timestamp: u32,
    len: u32,
}

#[derive(Debug, Default, Eq, PartialEq, RustcDecodable, RustcEncodable)]
struct SegmentCheckpoint {
    tail: u64,
    head: u64,
    sync_offset: u32,
    closed: bool,
}

#[derive(Debug, Default, Eq, PartialEq, RustcDecodable, RustcEncodable)]
struct QueueBackendCheckpoint {
    segments: Vec<SegmentCheckpoint>
}

#[derive(Debug)]
pub struct Segment {
    file_path: PathBuf,
    // TODO: move mmaped file abstraction
    file: File,
    file_mmap: *mut u8,
    file_len: u32,
    file_offset: u32,
    sync_offset: u32,
    first_timestamp: u32,
    last_timestamp: u32,
    tail: u64,
    head: u64,
    closed: bool,
    deleted: bool,
    index: SpinLock<OffsetIndex>,
    // dirty_bytes: usize,
    // dirty_messages: usize,
}

#[derive(Debug)]
pub struct QueueBackend {
    config: QueueConfig,
    segments: SpinRwLock<Vec<Arc<Segment>>>,
    head: u64,
    tail: u64
}

impl Segment {
    fn gen_file_path(config: &QueueConfig, start_id: u64, ext: &str) -> PathBuf {
        config.data_directory.join(format!("{:015}.{}", start_id, ext))
    }

    fn hash_segment_message(header: &MessageHeader) -> u32 {
        let mut hasher = XxHash::with_seed(0);
        unsafe {
            hasher.write(slice::from_raw_parts(
                (header as *const _ as *const u8).offset(size_of::<u32>() as isize),
                size_of::<MessageHeader>() + header.len as usize - size_of::<u32>()
            ));
        }
        hasher.finish() as u32
    }

    fn create(config: &QueueConfig, start_id: u64) -> QueueBackendResult<Segment> {
        let file_path = Self::gen_file_path(config, start_id, DATA_EXTENSION);
        debug!("[{}] creating data file {:?}", config.name, file_path);
        let file = try!(OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&file_path));
        try!(file.allocate(config.segment_size));

        let mut segment = try!(Self::new(config, file, file_path, start_id));
        unsafe {
            *(segment.file_mmap as *mut u32) = MAGIC_NUM;
        }
        segment.file_offset = size_of::<u32>() as u32;

        Ok(segment)
    }

    fn open(config: &QueueConfig, checkpoint: &SegmentCheckpoint) -> QueueBackendResult<Segment> {
        let file_path = Self::gen_file_path(config, checkpoint.tail, DATA_EXTENSION);
        debug!("[{}] opening data file {:?}", config.name, file_path);
        let file = try!(OpenOptions::new().read(true).write(true).open(&file_path));
        let mut segment = try!(Self::new(config, file, file_path, checkpoint.tail));
        try!(segment.recover(checkpoint));
        Ok(segment)
    }

    fn new(_: &QueueConfig, file: File, file_path: PathBuf, start_id: u64) -> QueueBackendResult<Segment> {
        let file_len = try!(file.metadata()).len();
        if file_len <= 4 {
            return Err(QueueBackendError::SegmentFileInvalid)
        }

        let file_mmap = try!(mman::mmap(
            ptr::null_mut(), file_len,
            mman::PROT_READ | mman::PROT_WRITE, mman::MAP_SHARED,
            file.as_raw_fd(), 0)) as *mut u8;
        try!(mman::madvise(
            file_mmap as *mut c_void,
            file_len,
            mman::MADV_SEQUENTIAL));

        Ok(Segment {
            file_path: file_path,
            file: file,
            file_mmap: file_mmap,
            file_len: file_len as u32,
            file_offset: 0,
            sync_offset: 0,
            first_timestamp: INVALID_TIMESTAMP,
            last_timestamp: INVALID_TIMESTAMP,
            tail: start_id,
            head: start_id,
            closed: false,
            deleted: false,
            index: SpinLock::new(OffsetIndex::new(start_id))
            // dirty_messages: 0,
            // dirty_bytes: 0,
        })
    }
    
    fn get(&self, id: u64) -> Result<InnerMessage, ()> {
        let message_offset = if let Some(message_offset) = self.index.lock().get_offset(id) {
            message_offset
        } else {
            return Err(())
        };

        let header: &MessageHeader = unsafe {
            mem::transmute(self.file_mmap.offset(message_offset as isize))
        };

        // check id and possible overflow
        assert!(message_offset <= self.file_offset,
            "Corrupt file, message start offset {} is past file offset {}", message_offset, self.file_offset);
        let message_end_offset = message_offset + size_of::<MessageHeader>() as u32 + header.len as u32;
        assert!(message_end_offset <= self.file_offset,
            "Corrupt file, message end offset {} is past file offset {}", message_end_offset, self.file_offset);
        assert!(header.id == id,
            "Corrupt file, ids don't match {} {} at offset {}", header.id, id, message_offset);

        let message = InnerMessage {
            mmap_ptr: header,
        };

        Ok(message)
    }

    fn push(&mut self, body: &[u8], clock: u32) -> QueueBackendResult<u64> {
        let header_size = size_of::<MessageHeader>() as u32;
        let message_total_len = header_size + body.len() as u32;

        if message_total_len > self.file_len - self.file_offset {
            if message_total_len + size_of::<u32>() as u32 >= self.file_len {
                return Err(QueueBackendError::MessageTooBig)
            }
            self.closed = true;
            return Err(QueueBackendError::SegmentFull)
        }

        let id = self.head;

        unsafe {
            let header: &mut MessageHeader = mem::transmute(self.file_mmap.offset(self.file_offset as isize));
            header.id = self.head;
            header.timestamp = clock;
            header.len = body.len() as u32;
            ptr::copy_nonoverlapping(
                body.as_ptr(),
                self.file_mmap.offset(self.file_offset as isize + size_of::<MessageHeader>() as isize),
                body.len());
            header.hash = Self::hash_segment_message(header);
        }

        if self.first_timestamp == INVALID_TIMESTAMP {
            self.first_timestamp = clock;
        }
        self.last_timestamp = clock;
        self.file_offset += message_total_len;
        self.head += 1;
        // self.dirty_bytes += message_total_len;
        // self.dirty_messages += 1;
        // push_offset is the sync point between readers and writers, do it last
        self.index.lock().push_offset(id, self.file_offset - message_total_len);

        Ok(id)
    }

    fn sync(&mut self, full: bool) {
        let sync_offset = if full || self.closed {
            self.file_offset
        } else {
            self.file_offset.saturating_sub(1024 * 1024)
        };
        if sync_offset > 0 && sync_offset > self.sync_offset {
            mman::msync(self.file_mmap as *mut c_void, sync_offset as u64, mman::MS_SYNC).unwrap();
            self.sync_offset = sync_offset;
        }
    }

    fn recover(&mut self, checkpoint: &SegmentCheckpoint) -> QueueBackendResult<()> {
        debug!("[{:?}] checkpoint loaded: {:?}", self.file_path, checkpoint);
        assert_eq!(self.tail, checkpoint.tail);
        self.tail = checkpoint.tail;
        self.head = checkpoint.tail;
        self.sync_offset = checkpoint.sync_offset;
        self.closed = checkpoint.closed;

        try!(mman::madvise(
            self.file_mmap as *mut c_void,
            self.sync_offset as u64,
            mman::MADV_WILLNEED));

        self.file_offset = size_of::<u32>() as u32;
        unsafe {
            if *(self.file_mmap as *mut u32) != MAGIC_NUM {
                warn!("[{:?}] incorrect magic number", self.file_path);
                *(self.file_mmap as *mut u32) = MAGIC_NUM
            }
        }

        let header_size = size_of::<MessageHeader>() as u32;
        let mut locked_index = self.index.lock();
        while self.file_offset + header_size < self.file_len {
            let header: &MessageHeader = unsafe {
                mem::transmute(self.file_mmap.offset(self.file_offset as isize))
            };
            let message_total_len = header_size + header.len;
            if header.id != self.head {
                warn!("[{:?}] expected id {} got {} when recovering @{}",
                    self.file_path, self.head, header.id, self.file_offset);
                break
            }
            if self.file_offset + message_total_len < self.file_len {
                if header.hash != Self::hash_segment_message(header) {
                    warn!("[{:?}] corrupt message with id {} when recovering @{}",
                        self.file_path, header.id, self.file_offset);
                    break
                }
            } else {
                warn!("[{:?}] message with id {} would overflow file @{}",
                    self.file_path, header.id, self.file_offset);
                break
            }

            if self.first_timestamp == INVALID_TIMESTAMP {
                self.first_timestamp = header.timestamp;
            }
            self.last_timestamp = header.timestamp;
            self.file_offset += header_size + header.len;
            self.head += 1;
            locked_index.push_offset(header.id, self.file_offset - message_total_len);
        }

        self.sync_offset = self.file_offset;
        Ok(())
    }

    fn checkpoint(&mut self, full: bool) -> SegmentCheckpoint {
        // FIXME: reset and log stats
        let mut checkpoint = SegmentCheckpoint {
            tail: self.tail,
            head: self.head,
            sync_offset: self.sync_offset,
            closed: self.closed,
        };
        self.sync(full);
        // update sync_offset
        checkpoint.sync_offset = self.sync_offset;
        checkpoint
    }

    fn purge(&mut self) {
        assert!(!self.deleted);
        self.deleted = true;
    }

    #[allow(mutable_transmutes)]
    pub fn as_mut(&self) -> &mut Self {
        unsafe { mem::transmute(self) }
    }
}

impl Drop for Segment {
    fn drop(&mut self) {
        let mmap = self.file_mmap as *mut c_void;
        mman::madvise(mmap, self.file_len as u64, mman::MADV_DONTNEED).unwrap();
        mman::munmap(mmap, self.file_len as u64).unwrap();
        if self.deleted {
            remove_file_if_exist(&self.file_path).unwrap();
        }
    }
}

impl QueueBackend {
    pub fn new(config: QueueConfig, recover: bool) -> QueueBackend {
        let mut backend = QueueBackend {
            config: config,
            segments: SpinRwLock::new(Vec::new()),
            head: 1,
            tail: 1
        };
        if recover {
            backend.recover();
        }
        backend
    }

    pub fn segments_count(&self) -> usize {
        self.segments.read().len()
    }

    pub fn tail(&self) -> u64 {
        self.tail
    }

    pub fn head(&self) -> u64 {
        self.head
    }

    fn find_segment(&self, id: u64) -> Option<Arc<Segment>> {
        for segment in self.segments.read().iter() {
            if id < segment.head && segment.tail < segment.head {
                return Some(segment.clone())
            }
        }
        None
    }

    /// returns the smallest id that when requested will yield messages
    /// with timestamps >= the requested timestamp
    pub fn find_id_for_timestamp(&self, timestamp: u32) -> u64 {
        let mut tail = self.tail;
        let mut head = self.head;
        // narrow search window using segment info, it may same some IO in the next step
        for segment in self.segments.read().iter() {
            if segment.first_timestamp < timestamp {
                tail = segment.tail
            }
            if segment.last_timestamp > timestamp {
                head = segment.head
            }
        }
        // binary search style search inside the segments
        while head > tail {
            let id = tail + (head - tail) / 2;
            if let Some(msg) = self.get(id) {
                if msg.timestamp() >= timestamp {
                    head = id;
                } else {
                    tail = id + 1;
                }
            } else {
                head = id;
            }
        }
        return tail
    }

    /// Put a message at the end of the Queue, return the message id if succesfull
    /// Note: it's the caller responsability to serialize write calls
    pub fn push(&mut self, body: &[u8], timestamp: u32) -> QueueBackendResult<u64> {
        let result = if let Some(segment) = self.segments.read().last() {
            segment.as_mut().push(body, timestamp)
        } else {
            Err(QueueBackendError::SegmentFull)
        };

        match result {
            Ok(id) => {
                assert_eq!(id, self.head);
                self.head += 1;
                return result
            },
            Err(QueueBackendError::SegmentFull) => (),
            Err(_) => return result
        }

        // create a new segment
        let segment = Arc::new(try!(Segment::create(&self.config, self.head)));
        self.segments.write().push(segment.clone());

        let id = try!(segment.as_mut().push(body, timestamp));
        assert_eq!(id, self.head);
        self.head += 1;
        Ok(id)
    }

    /// Get a new message with the specified id
    /// if not possible, return the next available message
    pub fn get(&self, id: u64) -> Option<Message> {
        if let Some(segment) = self.find_segment(id) {
            if let Ok(inner) = segment.get(cmp::max(id, segment.tail)) { 
                return Some(Message {
                    inner: inner,
                    segment: segment,
                })
            }
        }
        None
    }

    pub fn purge(&mut self) {
        self.tail = self.head;
        self.segments.write().last_mut().map(|last| last.as_mut().closed = true);
    }

    fn wait_delete_segment(segment: Arc<Segment>) {
        let mut wait_count = 0;
        while Arc::strong_count(&segment) > 1 {
            thread::sleep(Duration::from_millis(100));
            wait_count += 1;
            if wait_count % 10 == 0 {
                warn!("Still waiting for Segment {:?} to be freed {}", segment, wait_count);
            }
        }
        segment.as_mut().purge()
    }

    pub fn delete(&mut self) {
        let path = self.config.data_directory.join(BACKEND_CHECKPOINT_FILE);
        remove_file_if_exist(&path).unwrap();
        let mut locked_segments = self.segments.write();
        for segment in locked_segments.drain(..) {
            Self::wait_delete_segment(segment);
        }
    }

    fn recover(&mut self) {
        let path = self.config.data_directory.join(BACKEND_CHECKPOINT_FILE);
        let backend_checkpoint: QueueBackendCheckpoint = match File::open(path) {
            Ok(mut file) => {
                let mut contents = String::new();
                let _ = file.read_to_string(&mut contents);
                let checkpoint_result = json::decode(&contents);
                match checkpoint_result {
                    Ok(state) => state,
                    Err(error) => {
                        error!("[{}] error parsing checkpoint information: {}",
                            self.config.name, error);
                        return;
                    }
                }
            }
            Err(error) => {
                warn!("[{}] error reading checkpoint information: {}",
                    self.config.name, error);
                return;
            }
        };

        info!("[{}] checkpoint loaded: {:?}", self.config.name, backend_checkpoint);

        let mut locked_segments = self.segments.write();
        for segment_checkpoint in &backend_checkpoint.segments {
            let segment = match Segment::open(&self.config, segment_checkpoint) {
                Ok(inner_segment) => Arc::new(inner_segment),
                Err(error) => {
                    warn!("[{}] error opening segment from checkpoint {:?}: {:?}",
                        self.config.name, segment_checkpoint, error);
                    continue
                },
            };
            if !segment.closed && locked_segments.last_mut().is_some() {
                // make sure we only have one open segment, the last
                locked_segments.last_mut().unwrap().as_mut().closed = true;
            }
            locked_segments.push(segment);
        }

        if let Some(first_file) = locked_segments.first() {
            self.tail = first_file.tail
        }

        if let Some(last_file) = locked_segments.last() {
            self.head = last_file.head;
        }
    }

    pub fn checkpoint(&mut self, full: bool) {
        let segments_copy = self.segments.read().clone();
        let file_checkpoints: Vec<_> = segments_copy.into_iter().filter_map(|segment| {
            if segment.tail >= self.tail {
                Some(segment.as_mut().checkpoint(full))
            } else {
                None
            }
        }).collect();

        let checkpoint = QueueBackendCheckpoint {
            segments: file_checkpoints
        };

        let tmp_path = self.config.data_directory.join(TMP_BACKEND_CHECKPOINT_FILE);
        let result = File::create(&tmp_path)
            .and_then(|mut file| {
                try!(write!(file, "{}", json::as_pretty_json(&checkpoint)));
                file.sync_data()
            }).and_then(|_| {
                let final_path = tmp_path.with_file_name(BACKEND_CHECKPOINT_FILE);
                fs::rename(tmp_path, final_path)
            });

        match result {
            Ok(_) => {
                info!("[{}] checkpointed: {:?}", self.config.name, checkpoint.segments);
            }
            Err(error) => {
                warn!("[{}] error writing checkpoint information: {}",
                    self.config.name, error);
            }
        }
    }

    pub fn gc(&mut self, smallest_tail: u64, clock: u32) {
        let mut gc_seg_count = 0;
        {
            let locked_segments = self.segments.read();
            let mut total_rem_size = locked_segments.iter().map(|s| s.file_len as u64).sum::<u64>();
            for segment in locked_segments.iter() {
                if ! segment.closed {
                    break
                }

                let contains_channel_msg = smallest_tail < segment.head;
                let last_msg_age = clock - segment.last_timestamp;
                let pass_hard_retention_period = last_msg_age <= self.config.hard_retention_period;
                let pass_hard_retention_size = total_rem_size <= self.config.hard_retention_size;
                let pass_retention_period = last_msg_age <= self.config.retention_period;
                let pass_retention_size = total_rem_size <= self.config.retention_size;

                if pass_hard_retention_period && pass_hard_retention_size {
                    if contains_channel_msg || pass_retention_period || pass_retention_size {
                        break
                    }
                }
                gc_seg_count += 1;
                total_rem_size -= segment.file_len as u64;
            }
        }
        if gc_seg_count != 0 {
            info!("[{}] {} segments available for gc", self.config.name, gc_seg_count);
            // purge them
            let mut gc_segments: Vec<_> = Vec::with_capacity(gc_seg_count);
            {
                let mut locked_segments = self.segments.write();
                gc_segments.extend(locked_segments.drain(..gc_seg_count));
                if let Some(first_segment) = locked_segments.first() {
                    self.tail = first_segment.tail;
                } else {
                    self.tail = self.head;
                }
            }
            for segment in gc_segments {
                Self::wait_delete_segment(segment)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use config::*;
    use std::fs;
    use std::thread;
    use utils;

    fn get_backend_opt(name: &str, recover: bool) -> QueueBackend {
        let mut server_config = ServerConfig::read();
        server_config.data_directory = "./test_data".into();
        server_config.default_queue_config.segment_size = 4 * 1024 * 1024;
        server_config.default_queue_config.retention_period = 1;
        server_config.default_queue_config.hard_retention_period = 2;
        let mut queue_config = server_config.new_queue_config(name);
        queue_config.message_timeout = 1;
        utils::create_dir_if_not_exist(&queue_config.data_directory).unwrap();
        QueueBackend::new(queue_config, recover)
    }

    fn get_backend() -> QueueBackend {
        get_backend_opt(thread::current().name().unwrap(), false)
    }

    fn get_backend_recover() -> QueueBackend {
        get_backend_opt(thread::current().name().unwrap(), true)
    }

    fn gen_message() -> &'static [u8] {
        return b"333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333";
    }

    #[test]
    fn test_find_timestamp() {
        let mut backend = get_backend();
        let mut num_writes = 1u64;
        while backend.segments_count() < 3 {
            backend.push(gen_message(), num_writes as u32).unwrap();
            num_writes += 1;
        }

        let get_timestamp = |timestamp: u32| {
            backend.get(backend.find_id_for_timestamp(timestamp)).unwrap().timestamp()
        };

        assert_eq!(backend.find_id_for_timestamp(0), backend.tail);
        assert_eq!(backend.find_id_for_timestamp(num_writes as u32), backend.head);
        assert_eq!(get_timestamp(0), 1);

        for ts in 1..num_writes as u32 {
            assert_eq!(get_timestamp(ts), ts);
        }

        // remove middle file
        backend.segments.write().remove(1);

        for segment in backend.segments.read().iter() {
            for ts in segment.tail as u32..segment.head as u32 {
                assert_eq!(get_timestamp(ts), ts);
            }
        }
    }

    #[test]
    fn test_find_timestamp2() {
        let mut backend = get_backend();
        let mut num_writes = 1u64;
        while backend.segments_count() < 3 {
            backend.push(gen_message(), (num_writes / 10) as u32).unwrap();
            num_writes += 1;
        }

        assert_eq!(backend.find_id_for_timestamp(0), 1);
        for id in 10..num_writes {
            // must return the first id with timestamp >= requested timestamp
            assert_eq!(backend.find_id_for_timestamp((id / 10) as u32), id - id % 10);
        }
    }

    #[test]
    fn test_find_timestamp3() {
        let mut backend = get_backend();
        // when empty
        assert_eq!(backend.find_id_for_timestamp(0), backend.tail);
        assert_eq!(backend.find_id_for_timestamp(100000), backend.tail);
        // simulate get misses
        backend.head = 10000;
        assert_eq!(backend.find_id_for_timestamp(0), backend.tail);
        assert_eq!(backend.find_id_for_timestamp(100000), backend.tail);
    }

    #[test]
    fn test_retention_period() {
        let mut backend = get_backend();
        while backend.segments_count() < 3 {
            let msg_timestamp = backend.segments_count() as u32;
            backend.push(gen_message(), msg_timestamp).unwrap();
        }
        let head = backend.head;
        // check segments last timestamps to validade the tests bellow
        assert_eq!(backend.segments.read()[0].last_timestamp, 1);
        assert_eq!(backend.segments.read()[1].last_timestamp, 2);
        assert_eq!(backend.segments.read()[2].last_timestamp, 2);

        // retention period will keep segments from beeing deleted
        backend.gc(1, 2);
        assert_eq!(backend.segments_count(), 3);
        backend.gc(head, 2);
        assert_eq!(backend.segments_count(), 3);
        // soft retention period expired, gc should get rid of the first segment
        // but only if it's cleared by the smallest tail
        backend.gc(1, 3);
        assert_eq!(backend.segments_count(), 3);
        backend.gc(head, 3);
        assert_eq!(backend.segments_count(), 2);
        // hard retention period will forcefully purge the second segment
        // leaving only the last, which is still open
        backend.gc(head, 4);
        assert_eq!(backend.segments_count(), 1);
    }

    // #[test]
    // fn test_hard_retention_period() {
    //     let mut backend = get_backend();
    //     while backend.segments_count() < 3 {
    //         backend.push(gen_message(), 1).unwrap();
    //     }
    //     let head = backend.head;

    //     // har retention period will purge old segments even if not cleared by the smallest tail
    //     backend.gc(1, 4);
    //     assert_eq!(backend.segments_count(), 1);
    // }

    #[test]
    fn test_corrupt_files() {
        let mut backend = get_backend();
        let mut num_writes = 0;
        while backend.segments_count() < 3 {
            backend.push(gen_message(), 0).unwrap();
            num_writes += 1;
        }
        backend.checkpoint(true);
        {
            // zero second half of first file
            let mut segment = backend.segments.read()[0].clone();
            segment.as_mut().file.set_len(segment.file_len as u64 / 2).unwrap();
            segment.as_mut().file.set_len(segment.file_len as u64).unwrap();
            // nuke the second
            segment = backend.segments.read()[1].clone();
            segment.as_mut().file.set_len(0).unwrap();
            segment.as_mut().file.set_len(segment.file_len as u64).unwrap();
        }

        let mut backend = get_backend_recover();
        let mut num_reads = 0;
        let mut id = 1;
        let mut holes = 0;
        while let Some(message) = backend.get(id) {
            num_reads += 1;
            if message.id() != id {
                holes += 1
            }
            id = message.id() + 1;
        }
        assert_eq!(id, backend.head());
        assert_eq!(num_reads, num_writes / 4 + 1);
        assert_eq!(holes, 1);

        {
            // truncate the third
            let segment = backend.segments.read()[2].clone();
            segment.as_mut().file.set_len(0).unwrap();
        }

        backend = get_backend_recover();
        num_reads = 0;
        id = 1;
        holes = 0;
        while let Some(message) = backend.get(id) {
            num_reads += 1;
            if message.id() != id {
                holes += 1
            }
            id = message.id() + 1;
        }
        assert_eq!(id, backend.segments.read()[0].head);
        assert_eq!(num_reads, num_writes / 4);
        assert_eq!(holes, 0);

        // push to the third file, that we nuked
        let prev_reads = num_reads;
        backend.push(gen_message(), 0).unwrap() == num_writes + 1;
        backend.push(gen_message(), 0).unwrap() == num_writes + 2;

        num_reads = 0;
        id = 1;
        holes = 0;
        while let Some(message) = backend.get(id) {
            num_reads += 1;
            if message.id() != id {
                holes += 1
            }
            id = message.id() + 1;
        }
        assert_eq!(id, backend.head());
        assert_eq!(num_reads, prev_reads + 2);
        assert_eq!(holes, 1);
    }

    #[test]
    fn test_missing_file() {
        let mut num_writes = 0;
        let file_paths: Vec<_> = {
            let mut backend = get_backend();
            while backend.segments_count() < 3 {
                backend.push(gen_message(), 0).unwrap();
                num_writes += 1;
            }
            backend.checkpoint(true);

            let locked_segments = backend.segments.read();
            locked_segments.iter().map(|s| s.file_path.clone()).collect()
        };

        for (segment_num, file_path) in file_paths.iter().enumerate() {
            fs::remove_file(&file_path).unwrap();

            let backend = get_backend_recover();
            let mut num_reads = 0;
            let mut id = 1;
            let mut holes = 0;
            while let Some(message) = backend.get(id) {
                num_reads += 1;
                if message.id() != id {
                    holes += 1
                }
                id = message.id() + 1;
            }
            assert_eq!(id, backend.head());
            match segment_num {
                0 => {
                    assert_eq!(num_reads, (num_writes - 1) / 2 + 1);
                    assert_eq!(holes, 1);
                },
                1 => {
                    assert_eq!(num_reads, 1);
                    assert_eq!(holes, 1);
                },
                2 => {
                    assert_eq!(num_reads, 0);
                    assert_eq!(holes, 0);
                },
                _ => unreachable!()
            }
        }
    }
}
