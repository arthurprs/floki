use std::ptr;
use std::slice;
use std::thread;
use std::mem::{self, size_of};
use std::io::prelude::*;
use std::os::unix::io::{RawFd, AsRawFd};
use std::fs::{self, File, OpenOptions};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use nix::c_void;
use nix::sys::mman;
use spin::RwLock as SpinRwLock;
use rustc_serialize::json;

use config::*;
use utils::*;
use offset_index::*;

const MAGIC_NUM: u32 = 0xF1031311u32;

#[derive(Debug, Clone)]
struct InnerMessage {
    mmap_ptr: *mut u8,
    fd_offset: u32,
    id: u64,
    len: u32,
}

#[derive(Debug, Clone)]
pub struct Message {
    segment: Arc<Segment>,
    inner: InnerMessage,
}

impl Message {
    pub fn id(&self) -> u64 {
        self.inner.id
    }

    pub fn len(&self) -> u32 {
        self.inner.len
    }

    pub fn body(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.inner.mmap_ptr, self.inner.len as usize) }
    }

    pub fn fd(&self) -> RawFd {
        self.segment.file.as_raw_fd()
    }

    pub fn fd_offset(&self) -> u32 {
        self.inner.fd_offset
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

#[derive(Debug, Default, RustcDecodable, RustcEncodable)]
struct QueueBackendCheckpoint {
    segments: Vec<SegmentCheckpoint>
}

#[derive(Debug)]
pub struct Segment {
    data_path: PathBuf,
    // TODO: move mmaped file abstraction
    file: File,
    file_mmap: *mut u8,
    file_size: u32,
    file_offset: u32,
    sync_offset: u32,
    // dirty_bytes: usize,
    // dirty_messages: usize,
    tail: u64,
    head: u64,
    closed: bool,
    deleted: bool,
    index: OffsetIndex,
}

#[derive(Debug)]
pub struct QueueBackend {
    config: Rc<QueueConfig>,
    segments: SpinRwLock<Vec<Arc<Segment>>>,
    head: u64,
    tail: u64
}

impl Segment {
    fn gen_file_path(config: &QueueConfig, start_id: u64, ext: &str) -> PathBuf {
        config.data_directory.join(format!("{:015}.{}", start_id, ext))
    }

    fn create(config: &QueueConfig, start_id: u64) -> Segment {
        let data_path = Self::gen_file_path(config, start_id, DATA_EXTENSION);
        debug!("[{}] creating data file {:?}", config.name, data_path);
        let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&data_path).unwrap();
        // TODO: try to use fallocate if present
        // hopefully the filesystem supports sparse segments
        file.set_len(config.segment_size).unwrap();
        let mut segment = Self::new(config, file, data_path, start_id);
        unsafe {
            let magic_num = segment.file_mmap as *mut u32;
            *magic_num = MAGIC_NUM;
        }
        segment.file_offset = size_of::<u32>() as u32;
        segment
    }

    fn open(config: &QueueConfig, checkpoint: SegmentCheckpoint) -> Segment {
        let data_path = Self::gen_file_path(config, checkpoint.tail, DATA_EXTENSION);
        debug!("[{}] opening data file {:?}", config.name, data_path);
        let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&data_path).unwrap();
        let mut segment = Self::new(config, file, data_path, checkpoint.tail);
        segment.recover(checkpoint);
        segment
    }

    fn new(config: &QueueConfig, file: File, data_path: PathBuf, start_id: u64) -> Segment {
        let file_size = file.metadata().unwrap().len();
        assert_eq!(file_size, config.segment_size);

        let file_mmap = mman::mmap(
            ptr::null_mut(), file_size,
            mman::PROT_READ | mman::PROT_WRITE, mman::MAP_SHARED,
            file.as_raw_fd(), 0).unwrap() as *mut u8;
        mman::madvise(
            file_mmap as *mut c_void,
            file_size,
            mman::MADV_SEQUENTIAL).unwrap();

        Segment {
            data_path: data_path,
            file: file,
            file_mmap: file_mmap,
            file_size: file_size as u32,
            file_offset: 0,
            sync_offset: 0,
            tail: start_id,
            head: start_id,
            // dirty_messages: 0,
            // dirty_bytes: 0,
            closed: false,
            deleted: false,
            index: OffsetIndex::new(start_id)
        }
    }
    
    fn get(&self, id: u64) -> Result<InnerMessage, ()> {
        let message_offset = if let Some(message_offset) = self.index.get_offset(id) {
            message_offset
        } else {
            return Err(())
        };

        let header: &MessageHeader = unsafe {
            mem::transmute(self.file_mmap.offset(message_offset as isize))
        };

        // check id and possible overflow
        assert!(message_offset <= self.file_offset,
            "Corrupt file, message start offset {} is past file file offset {}", message_offset, self.file_offset);
        let message_end_offset = message_offset + size_of::<MessageHeader>() as u32 + header.len as u32;
        assert!(message_end_offset <= self.file_offset,
            "Corrupt file, message end offset {} is past file file offset {}", message_end_offset, self.file_offset);
        assert!(header.id == id,
            "Corrupt file, ids don't match {} {}", header.id, id);

        let mmap_ptr = unsafe { self.file_mmap.offset(message_offset as isize + size_of::<MessageHeader>() as isize) };

        let message = InnerMessage {
            id: id,
            len: header.len,
            mmap_ptr: mmap_ptr,
            fd_offset: message_offset + size_of::<MessageHeader>() as u32,
        };

        Ok(message)
    }

    fn push(&mut self, body: &[u8], clock: u32) -> Result<u64, ()> {
        let message_total_len = size_of::<MessageHeader>() + body.len();

        if message_total_len as u32 > self.file_size - self.file_offset {
            self.closed = true;
            return Err(())
        }

        let header = MessageHeader {
            hash: 0,
            id: self.head,
            timestamp: clock,
            len: body.len() as u32
        };

        unsafe {
            ptr::copy_nonoverlapping(
                mem::transmute(&header),
                self.file_mmap.offset(self.file_offset as isize),
                size_of::<MessageHeader>());
            ptr::copy_nonoverlapping(
                body.as_ptr(),
                self.file_mmap.offset(self.file_offset as isize + size_of::<MessageHeader>() as isize),
                body.len());
        }
        // unsafe {
        //     self.file.write_all(slice::from_raw_parts::<u8>(
        //         mem::transmute(&header), size_of::<MessageHeader>()
        //     )).unwrap();
        //     self.file.write_all(body.body).unwrap();
        // }
        self.index.push_offset(header.id, self.file_offset);
        self.head += 1;
        self.file_offset += message_total_len as u32; 
        // self.dirty_bytes += message_total_len;
        // self.dirty_messages += 1;

        Ok(header.id)
    }

    fn sync(&mut self, full: bool) {
        let sync_offset = if full { self.file_offset } else { self.file_offset.saturating_sub(1024 * 1024) };
        if sync_offset > 0 && sync_offset > self.sync_offset {
            mman::msync(self.file_mmap as *mut c_void, sync_offset as u64, mman::MS_SYNC).unwrap();
            self.sync_offset = sync_offset;
        }
    }

    fn recover(&mut self, checkpoint: SegmentCheckpoint) {
        debug!("[{:?}] checkpoint loaded: {:?}", self.data_path, checkpoint);
        assert_eq!(self.tail, checkpoint.tail);
        self.tail = checkpoint.tail;
        self.head = checkpoint.tail;
        self.sync_offset = checkpoint.sync_offset;
        self.closed = checkpoint.closed;
        self.file_offset = size_of::<u32>() as u32;
        self.sync_offset = self.file_offset;

        unsafe {
            let magic_num = self.file_mmap as *mut u32;
            if *magic_num != MAGIC_NUM {
                warn!("[{:?}] incorrect magic number", self.data_path);
                *magic_num = MAGIC_NUM;
                return;
            }
        }

        let header_size = size_of::<MessageHeader>() as u32;
        while self.file_offset + header_size < self.file_size {
            let header: &MessageHeader = unsafe {
                mem::transmute(self.file_mmap.offset(self.file_offset as isize))
            };
            if header.id != self.head {
                warn!("[{:?}] expected id {} got {} when recovering @{}",
                    self.data_path, self.head, header.id, self.file_offset);
                break
            }
            if self.file_offset + header_size + header.len < self.file_size {
                if header.hash != 0 {
                    warn!("[{:?}] corrupt message with id {} when recovering @{}",
                        self.data_path, header.id, self.file_offset);
                    break
                }
            } else {
                warn!("[{:?}] message with id {} would overflow file @{}",
                    self.data_path, header.id, self.file_offset);
                break
            }
            self.index.push_offset(header.id, self.file_offset);
            self.file_offset += header_size + header.len;
            self.head += 1;
        }

        self.sync_offset = self.file_offset;
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
        mman::madvise(mmap, self.file_size as u64, mman::MADV_DONTNEED).unwrap();
        mman::munmap(mmap, self.file_size as u64).unwrap();
        if self.deleted {
            remove_file_if_exist(&self.data_path).unwrap();
        }
    }
}

impl QueueBackend {
    pub fn new(config: Rc<QueueConfig>, recover: bool) -> QueueBackend {
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
        for segment in self.segments.read().iter().rev() {
            if segment.tail <= id {
                return Some(segment.clone())
            }
        }
        None
    }

    /// Put a message at the end of the Queue, return the message id if succesfull
    /// Note: it's the caller responsability to serialize write calls
    pub fn push(&mut self, body: &[u8], timestamp: u32) -> Option<u64> {
        let result = if let Some(segment) = self.segments.read().last() {
            segment.as_mut().push(body, timestamp).ok()
        } else {
            None
        };

        if let Some(id) = result {
            assert_eq!(id, self.head);
            self.head += 1;
            return result
        }

        // create a new segment
        let segment = Arc::new(Segment::create(&self.config, self.head));
        self.segments.write().push(segment.clone());

        let id = segment.as_mut().push(body, timestamp)
            .expect("Can't write to a newly created file!");
        assert_eq!(id, self.head);
        self.head += 1;
        Some(id)
    }

    /// Get a new message with the specified id
    pub fn get(&mut self, id: u64) -> Option<Message> {
        if let Some(segment) = self.find_segment(id) {
            if let Ok(inner) = segment.get(id) { 
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
        while Arc::strong_count(&segment) > 1 {
            thread::sleep_ms(100);
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
        for file_checkpoint in backend_checkpoint.segments {
            let segment = Arc::new(Segment::open(&self.config, file_checkpoint));
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
        let file_checkpoints: Vec<_> = segments_copy.into_iter().map(|segment| {
            segment.as_mut().checkpoint(full)
        }).collect();

        let checkpoint = QueueBackendCheckpoint {
            segments: file_checkpoints
        };

        let tmp_path = self.config.data_directory.join(TMP_BACKEND_CHECKPOINT_FILE);
        let result = File::create(&tmp_path)
            .and_then(|mut file| {
                write!(file, "{}", json::as_pretty_json(&checkpoint)).unwrap();
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

    pub fn gc(&mut self, smallest_tail: u64) {
        let mut gc_index = 0;
        // collect until the first still open or with head > smallest_tail
        for segment in &*self.segments.read() {
            if ! segment.closed || segment.head > smallest_tail {
                break
            }
            gc_index += 1;
        }
        info!("[{}] {} segments available for gc", self.config.name, gc_index);
        if gc_index != 0 {
            // purge them
            let mut locked_segments = self.segments.write();
            let gc_segments: Vec<_> = locked_segments.drain(..gc_index).collect();
            drop(locked_segments);
            self.tail = gc_segments.last().unwrap().head;
            for segment in gc_segments {
                Self::wait_delete_segment(segment)
            }
        }
    }
}
