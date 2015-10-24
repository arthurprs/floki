use std::ptr;
use std::slice;
use std::thread;
use std::mem::{self, size_of};
use std::io::prelude::*;
use std::os::unix::io::{RawFd, AsRawFd};
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;
use nix::c_void;
use nix::sys::mman;
use spin::RwLock as SpinRwLock;
use rustc_serialize::json;

use config::*;
use utils::*;
use offset_index::*;

#[derive(Debug)]
struct InnerMessage {
    mmap_ptr: *mut u8,
    file_offset: u32,
    id: u64,
    len: u32,
}

#[derive(Debug)]
pub struct Message {
    file: Arc<QueueFile>,
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
        self.file.file.as_raw_fd()
    }

    pub fn fd_offset(&self) -> u32 {
        self.inner.file_offset
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
struct QueueFileCheckpoint {
    tail: u64,
    head: u64,
    sync_offset: u32,
    closed: bool,
}

#[derive(Debug, Default, RustcDecodable, RustcEncodable)]
struct QueueBackendCheckpoint {
    files: Vec<QueueFileCheckpoint>
}

#[derive(Debug)]
pub struct QueueFile {
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
    files: SpinRwLock<Vec<Arc<QueueFile>>>,
    head: u64,
    tail: u64
}

impl QueueFile {
    fn gen_file_path(config: &QueueConfig, start_id: u64, ext: &str) -> PathBuf {
        config.data_directory.join(format!("{:015}.{}", start_id, ext))
    }

    fn create(config: &QueueConfig, start_id: u64) -> QueueFile {
        let data_path = Self::gen_file_path(config, start_id, DATA_EXTENSION);
        debug!("[{}] creating data file {:?}", config.name, data_path);
        let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&data_path).unwrap();
        // TODO: try to use fallocate if present
        // hopefully the filesystem supports sparse files
        file.set_len(config.segment_size).unwrap();
        Self::new(config, file, data_path, start_id)
    }

    fn open(config: &QueueConfig, checkpoint: QueueFileCheckpoint) -> QueueFile {
        let data_path = Self::gen_file_path(config, checkpoint.tail, DATA_EXTENSION);
        debug!("[{}] opening data file {:?}", config.name, data_path);
        let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&data_path).unwrap();
        let mut queue_file = Self::new(config, file, data_path, checkpoint.tail);
        queue_file.recover(checkpoint);
        queue_file
    }

    fn new(config: &QueueConfig, file: File, data_path: PathBuf, start_id: u64) -> QueueFile {
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

        QueueFile {
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
            file_offset: message_offset + size_of::<MessageHeader>() as u32,
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
        use std::cmp::max;
        let sync_offset = if full { self.file_offset } else { self.file_offset.saturating_sub(1024 * 1024) };
        if sync_offset > 0 && sync_offset > self.sync_offset {
            mman::msync(self.file_mmap as *mut c_void, sync_offset as u64, mman::MS_SYNC).unwrap();
            self.sync_offset = sync_offset;
        }
    }

    fn recover(&mut self, checkpoint: QueueFileCheckpoint) {
        //debug!("[{:?}] checkpoint loaded: {:?}", self.base_path, checkpoint);
        assert_eq!(self.tail, checkpoint.tail);
        self.tail = checkpoint.tail;
        self.head = checkpoint.head;
        self.sync_offset = checkpoint.sync_offset;
        self.closed = checkpoint.closed;

        self.file_offset = 0;
        let mut expected_id = self.tail;
        let header_size = size_of::<MessageHeader>() as u32;
        while self.file_offset + header_size < self.file_size {
            let header: &MessageHeader = unsafe {
                mem::transmute(self.file_mmap.offset(self.file_offset as isize))
            };
            if header.id != expected_id {
                warn!("[{:?}] expected id {} got {} when recovering @{}",
                    self.data_path, expected_id, header.id, self.file_offset);
                break
            }
            if self.file_offset + header_size + header.len < self.file_size {
                if header.hash != 0 {
                    warn!("[{:?}] corrupt message with id {} when recovering @{}",
                        self.data_path, header.id, self.file_offset);
                    break
                }
            }
            self.index.push_offset(header.id, self.file_offset);
            self.file_offset += header_size + header.len;
            expected_id += 1;
        }

        self.sync_offset = self.file_offset;
        self.head = expected_id;
    }

    fn checkpoint(&mut self, full: bool) -> QueueFileCheckpoint {
        // FIXME: reset and log stats
        let mut checkpoint = QueueFileCheckpoint {
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

impl Drop for QueueFile {
    fn drop(&mut self) {
        mman::munmap(self.file_mmap as *mut c_void, self.file_size as u64).unwrap();
        if self.deleted {
            remove_file_if_exist(&self.data_path).unwrap();
        }
    }
}

impl QueueBackend {
    pub fn new(config: Rc<QueueConfig>, recover: bool) -> QueueBackend {
        let mut backend = QueueBackend {
            config: config,
            files: SpinRwLock::new(Vec::new()),
            head: 1,
            tail: 1
        };
        if recover {
            backend.recover();
        }
        backend
    }

    pub fn files_count(&self) -> usize {
        self.files.read().len()
    }

    pub fn tail(&self) -> u64 {
        self.tail
    }

    fn find_file(&self, id: u64) -> Option<Arc<QueueFile>> {
        for q_file in  self.files.read().iter().rev() {
            if q_file.tail <= id {
                return Some(q_file.clone())
            }
        }
        None
    }

    /// Put a message at the end of the Queue, return the message id if succesfull
    /// Note: it's the caller responsability to serialize write calls
    pub fn push(&mut self, body: &[u8], timestamp: u32) -> Option<u64> {
        let result = if let Some(q_file) = self.files.read().last() {
            q_file.as_mut().push(body, timestamp).ok()
        } else {
            None
        };

        if let Some(id) = result {
            assert_eq!(id, self.head);
            self.head += 1;
            return result
        }

        let q_file: &mut QueueFile = {
            let queue_file = Arc::new(QueueFile::create(
                &self.config, self.head));
            let q_file_ptr = (&*queue_file) as *const QueueFile;
            self.files.write().push(queue_file);
            unsafe { mem::transmute(q_file_ptr) }
        };

        let id = q_file.push(body, timestamp).expect("Can't write to a newly created file!");
        assert_eq!(id, self.head);
        self.head += 1;
        Some(id)
    }

    /// Get a new message with the specified id
    pub fn get(&mut self, id: u64) -> Option<Message> {
        if let Some(q_file) = self.find_file(id) {
            if let Ok(inner) = q_file.get(id) { 
                return Some(Message {
                    inner: inner,
                    file: q_file,
                })
            }
        }
        None
    }

    pub fn purge(&mut self) {
        let mut locked_files = self.files.write();
        for q_file in locked_files.drain(..) {
            while Arc::strong_count(&q_file) > 1 {
                thread::sleep_ms(100);
            }
            q_file.as_mut().purge()
        }
        self.tail = 1;
        self.head = 1;
    }

    pub fn delete(&mut self) {
        self.files.write().clear();
        let path = self.config.data_directory.join(BACKEND_CHECKPOINT_FILE);
        remove_file_if_exist(&path).unwrap();
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

        let mut locked_files = self.files.write();
        for file_checkpoint in backend_checkpoint.files {
            let q_file = Arc::new(QueueFile::open(&self.config, file_checkpoint));
            locked_files.push(q_file);
        }

        if let Some(first_file) = locked_files.first() {
            self.tail = first_file.tail
        }

        if let Some(last_file) = locked_files.last() {
            self.head = last_file.head;
        }
    }

    pub fn checkpoint(&mut self, full: bool) {
        let files_copy = self.files.read().clone();
        let file_checkpoints: Vec<_> = files_copy.into_iter().map(|q_file| {
            q_file.as_mut().checkpoint(full)
        }).collect();

        let checkpoint = QueueBackendCheckpoint {
            files: file_checkpoints
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
                info!("[{}] checkpointed: {:?}", self.config.name, checkpoint.files);
            }
            Err(error) => {
                warn!("[{}] error writing checkpoint information: {}",
                    self.config.name, error);
            }
        }
    }

    pub fn gc(&mut self, smallest_tail: u64) {
        let mut gc_index = 0;
        // collect files_nums until the first still open or with head > smallest_tail
        for q_file in &*self.files.read() {
            if ! q_file.closed || q_file.head > smallest_tail {
                break
            }
            gc_index += 1;
        }
        info!("[{}] {} files available for gc", self.config.name, gc_index);
        if gc_index != 0 {
            // purge them
            let mut locked_files = self.files.write();
            let gc_files: Vec<_> = locked_files.drain(..gc_index).collect();
            drop(locked_files);
            self.tail = gc_files.last().unwrap().head;
            for q_file in gc_files {
                while Arc::strong_count(&q_file) > 1 {
                    thread::sleep_ms(100);
                }
                q_file.as_mut().purge()
            }
        }
    }
}
