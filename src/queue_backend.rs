use std::ptr;
use std::io::prelude::*;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::os::unix::io::AsRawFd;
use std::collections::BTreeMap;
use std::sync::Arc;
use nix::c_void;
use nix::sys::mman;
use spin::RwLock as SpinRwLock;
use std::rc::Rc;
use std::slice;
use std::mem::{self, size_of};
use vec_map::VecMap;
use rustc_serialize::json;
use std::os::unix::io::RawFd;

use config::*;
use utils::*;

#[derive(Debug)]
struct InnerMessage {
    mmap_ptr: *mut u8,
    file_offset: u64,
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

    pub fn fd_offset(&self) -> u64 {
        self.inner.file_offset
    }
}

unsafe impl Send for Message {}

#[repr(packed)]
struct MessageHeader {
    id: u64,
    len: u32,
    // hash: u32,
    // timestamp: u32,
}

#[derive(Debug, Default, Eq, PartialEq, RustcDecodable, RustcEncodable)]
struct QueueFileCheckpoint {
    tail: u64,
    head: u64,
    sync_offset: u64,
    closed: bool,
}

#[derive(Debug, Default, RustcDecodable, RustcEncodable)]
struct QueueBackendCheckpoint {
    files: BTreeMap<usize, QueueFileCheckpoint>
}

#[derive(Debug, Default)]
struct QueueFileIndex {
    offset: u64,
    index: Vec<(u32, u32)>,
}

#[derive(Debug)]
pub struct QueueFile {
    data_path: PathBuf,
    // TODO: move mmaped file abstraction
    file: File,
    file_size: u64,
    file_mmap: *mut u8,
    file_num: usize,
    sync_offset: u64,
    // dirty_bytes: usize,
    // dirty_messages: usize,
    tail: u64,
    head: u64,
    closed: bool,
    deleted: bool,
    index: QueueFileIndex,
}

#[derive(Debug)]
pub struct QueueBackend {
    config: Rc<QueueConfig>,
    files: SpinRwLock<VecMap<Arc<QueueFile>>>,
    head: u64,
    tail: u64
}

impl QueueFile {
    fn gen_data_file_path(config: &QueueConfig, file_num: usize) -> PathBuf {
        config.data_directory.join(format!("{:019}.{}", file_num, DATA_EXTENSION))
    }

    fn create(config: &QueueConfig, file_num: usize) -> QueueFile {
        let data_path = Self::gen_data_file_path(config, file_num);
        debug!("[{}] creating data file {:?}", config.name, data_path);
        let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&data_path).unwrap();
        // hopefully the filesystem supports sparse files
        file.set_len(config.segment_size).unwrap();
        Self::new(config, file, data_path, file_num)
    }

    fn open(config: &QueueConfig, file_num: usize, checkpoint: QueueFileCheckpoint) -> QueueFile {
        let data_path = Self::gen_data_file_path(config, file_num);
        debug!("[{}] opening data file {:?}", config.name, data_path);
        let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&data_path).unwrap();
        let mut queue_file = Self::new(config, file, data_path, file_num);
        queue_file.recover(checkpoint);
        queue_file
    }

    fn new(config: &QueueConfig, file: File, data_path: PathBuf, file_num: usize) -> QueueFile {
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
            file_size: file_size as u64,
            file_mmap: file_mmap,
            file_num: file_num,
            sync_offset: 0,
            tail: file_num as u64 * config.segment_size,
            head: file_num as u64 * config.segment_size,
            // dirty_messages: 0,
            // dirty_bytes: 0,
            closed: false,
            deleted: false,
            index: Default::default()
        }
    }
    
    fn get(&self, id: u64) -> Result<(u64, InnerMessage), bool> {
        if id >= self.head {
            return Err(self.closed)
        }

        let header: &MessageHeader = unsafe {
            mem::transmute(self.file_mmap.offset((id - self.tail) as isize))
        };

        // check id and possible overflow
        assert_eq!(header.id, id);
        assert!(header.id + size_of::<MessageHeader>() as u64 + header.len as u64 <= self.head);

        let message_total_len = (size_of::<MessageHeader>() + header.len as usize) as u64;
        let mmap_ptr = unsafe { self.file_mmap.offset((id - self.tail + size_of::<MessageHeader>() as u64) as isize) };

        let message = InnerMessage {
            id: id,
            len: header.len,
            mmap_ptr: mmap_ptr,
            file_offset: id - self.tail + size_of::<MessageHeader>() as u64,
        };

        Ok((id + message_total_len as u64, message))
    }

    fn append(&mut self, message: &[u8]) -> Option<u64> {
        let message_total_len = size_of::<MessageHeader>() + message.len();
        let file_offset = self.head - self.tail;

        if file_offset + message_total_len as u64 > self.file_size {
            self.closed = true;
            return None
        }

        let header = MessageHeader {
            id: self.head,
            len: message.len() as u32
        };

        unsafe {
            ptr::copy_nonoverlapping(
                mem::transmute(&header),
                self.file_mmap.offset(file_offset as isize),
                size_of::<MessageHeader>());
            ptr::copy_nonoverlapping(
                message.as_ptr(),
                self.file_mmap.offset(file_offset as isize + size_of::<MessageHeader>() as isize),
                message.len());
        }
        // unsafe {
        //     self.file.write_all(slice::from_raw_parts::<u8>(
        //         mem::transmute(&header), size_of::<MessageHeader>()
        //     )).unwrap();
        //     self.file.write_all(message.body).unwrap();
        // }
        self.head += message_total_len as u64;
        // self.dirty_bytes += message_total_len;
        // self.dirty_messages += 1;

        Some(header.id)
    }

    fn sync(&mut self, full: bool) {
        use std::cmp::max;
        let file_offset = (self.head - self.tail) as i64;
        let sync_offset = if full { file_offset } else { file_offset - 1024 * 1024 };
        if sync_offset > 0 && sync_offset as u64 > self.sync_offset {
            mman::msync(self.file_mmap as *mut c_void, sync_offset as u64, mman::MS_SYNC).unwrap();
            self.sync_offset = sync_offset as u64;
        }
    }

    fn recover(&mut self, checkpoint: QueueFileCheckpoint) {
        //debug!("[{:?}] checkpoint loaded: {:?}", self.base_path, checkpoint);
        assert_eq!(self.tail, checkpoint.tail);
        self.tail = checkpoint.tail;
        self.head = checkpoint.head;
        self.sync_offset = checkpoint.sync_offset;
        self.closed = checkpoint.closed;
        // TODO: read forward checking the message hashes
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
            files: SpinRwLock::new(VecMap::new()),
            head: 0,
            tail: 0
        };
        if recover {
            backend.recover();
        }
        backend
    }

    /// this relies on Box beeing at the same place even if this vector is reallocated
    /// also, this doen't do any ref count, so one must make sure the mmap is alive while there
    /// are messages pointing to this QueueFile
    #[inline]
    fn get_queue_file(&self, index: usize) -> Option<Arc<QueueFile>> {
        self.files.read().get(&index).map(|q| q.clone())
    }

    #[inline]
    fn get_queue_file_mut(&mut self, index: usize) -> Option<&mut QueueFile> {
        self.files.read().get(&index).map(|file_box_ref| {
            unsafe { mem::transmute(&**file_box_ref as *const QueueFile) }
        })
    }

    pub fn files_count(&self) -> usize {
        self.files.read().len()
    }

    pub fn tail(&self) -> u64 {
        self.tail
    }

    /// Put a message at the end of the Queue, return the message id if succesfull
    /// Note: it's the caller responsability to serialize write calls
    pub fn put(&mut self, message: &[u8]) -> Option<u64> {
        let mut head_file = (self.head / self.config.segment_size) as usize;
        let result = if let Some(q_file) = self.get_queue_file_mut(head_file) {
            q_file.append(message)
        } else {
            None
        };

        if let Some(new_head) = result {
            self.head = new_head;
            return result
        }

        if self.head != 0 {
            head_file += 1
        }

        let q_file: &mut QueueFile = {
            let queue_file = Arc::new(QueueFile::create(
                &self.config, head_file));
            let q_file_ptr = (&*queue_file) as *const QueueFile;
            assert!(self.files.write().insert(head_file, queue_file).is_none());
            unsafe { mem::transmute(q_file_ptr) }
        };

        let new_head = q_file.append(message).expect("Can't write to a newly created file!");
        self.head = new_head;
        Some(new_head)
    }

    /// Get a new message from the Queue just after the specified tail
    pub fn get(&mut self, mut tail: u64) -> Option<(u64, Message)> {
        let mut tail_file = (tail / self.config.segment_size) as usize;
        for _ in (0..2) {
            match self.get_queue_file(tail_file) {
                Some(q_file) => {
                    match q_file.get(tail) {
                        Ok((new_tail, inner_message)) => return Some((new_tail, Message {
                            file: q_file,
                            inner: inner_message
                        })),
                        Err(false) => return None,
                        Err(true) => {
                            // retry in the beginning of the next file
                            tail_file += 1;
                            tail = self.config.segment_size * tail_file as u64;
                        }
                    }
                }
                None => return None
            }
        }
        unreachable!();
    }

    pub fn purge(&mut self) {
        // BAD CODE
        let file_numbers: Vec<usize> = self.files.read().keys().collect();
        for file_num in file_numbers {
            self.get_queue_file_mut(file_num).unwrap().purge();
        }
        let mut locked_files = self.files.write();
        locked_files.clear();
        self.tail = 0;
        self.head = 0;
        let path = self.config.data_directory.join(BACKEND_CHECKPOINT_FILE);
        remove_file_if_exist(&path).unwrap();
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
        for (file_num, file_checkpoint) in backend_checkpoint.files {
            let queue_file = Arc::new(QueueFile::open(
                &self.config, file_num, file_checkpoint));
            locked_files.insert(file_num, queue_file);
        }

        if let Some(first_file) = locked_files.values().next() {
            self.tail = first_file.tail
        }

        if let Some(last_file) = locked_files.values().last() {
            self.head = last_file.head;
        }
    }

    pub fn checkpoint(&mut self, full: bool) {
        let file_nums: Vec<_> = self.files.read().keys().collect();
        let file_checkpoints: BTreeMap<_, _> =  file_nums.into_iter().map(|file_num| {
            (file_num, self.get_queue_file_mut(file_num).unwrap().checkpoint(full))
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
        let mut file_nums = Vec::new();
        // collect files_nums until the first still open or with head >= smallest_tail
        for (file_num, queue_file) in &*self.files.read() {
            if ! queue_file.closed || queue_file.head >= smallest_tail {
                break
            }
            file_nums.push(file_num);
        }
        if !file_nums.is_empty() {
            info!("[{}] {} files available for gc", self.config.name, file_nums.len());
        }
        // purge them
        for file_num in file_nums {
            self.get_queue_file_mut(file_num).unwrap().purge();
            self.files.write().remove(&file_num).unwrap();
        }
    }
}
