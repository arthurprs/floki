use std::ptr;
use std::io::prelude::*;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::os::unix::io::AsRawFd;
use std::collections::BTreeMap;
use nix::c_void;
use nix::sys::mman;
use spin::RwLock as SpinRwLock;
use std::rc::Rc;
use std::slice;
use std::mem::{self, size_of};
use vec_map::VecMap;
use rustc_serialize::json;

use config::*;

#[derive(Debug)]
pub struct Message<'a> {
    pub id: u64,
    pub body: &'a[u8]
}

#[repr(packed)]
struct MessageHeader {
    // TODO: needs a hash
    id: u64,
    len: u32,
}

#[derive(Debug, Default, Eq, PartialEq, RustcDecodable, RustcEncodable)]
struct QueueFileCheckpoint {
    head: u64,
    closed: bool
}

#[derive(Debug, Default, RustcDecodable, RustcEncodable)]
struct QueueBackendCheckpoint {
    files: BTreeMap<usize, QueueFileCheckpoint>
}

#[derive(Debug)]
pub struct QueueFile {
    base_path: PathBuf,
    // TODO: move mmaped file abstraction
    file: File,
    file_size: u64,
    file_mmap: *mut u8,
    file_num: usize,
    // dirty_bytes: usize,
    // dirty_messages: usize,
    tail: u64,
    head: u64,
    closed: bool,
}

#[derive(Debug)]
pub struct QueueBackend {
    config: Rc<QueueConfig>,
    files: SpinRwLock<VecMap<Box<QueueFile>>>,
    head: u64,
    tail: u64
}

impl QueueFile {
    fn gen_base_file_path(config: &QueueConfig, file_num: usize) -> PathBuf {
        config.data_directory.join(format!("{:08}", file_num))
    }

    fn create(config: &QueueConfig, file_num: usize) -> QueueFile {
        let base_path = Self::gen_base_file_path(config, file_num);
        let data_path = base_path.with_extension(DATA_EXTENSION);
        debug!("[{}] creating data file {:?}", config.name, data_path);
        let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(data_path).unwrap();
        // hopefully the filesystem supports sparse files
        file.set_len(config.segment_size).unwrap();
        Self::new(config, file, base_path, file_num)
    }

    fn open(config: &QueueConfig, file_num: usize, checkpoint: QueueFileCheckpoint) -> QueueFile {
        let base_path = Self::gen_base_file_path(config, file_num);
        let data_path = base_path.with_extension(DATA_EXTENSION);
        debug!("[{}] opening data file {:?}", config.name, data_path);
        let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(data_path).unwrap();
        let mut queue_file = Self::new(config, file, base_path, file_num);
        queue_file.recover(checkpoint);
        queue_file
    }

    fn new(config: &QueueConfig, file: File, base_path: PathBuf, file_num: usize) -> QueueFile {
        let file_size = file.metadata().unwrap().len();
        assert_eq!(file_size, config.segment_size);

        let file_mmap = mman::mmap(
            ptr::null_mut(), file_size,
            mman::PROT_READ | mman::PROT_WRITE, mman::MAP_SHARED,
            file.as_raw_fd(), 0).unwrap() as *mut u8;
        
        QueueFile {
            base_path: base_path,
            file: file,
            file_size: file_size as u64,
            file_mmap: file_mmap,
            file_num: file_num,
            tail: file_num as u64 * config.segment_size,
            head: file_num as u64 * config.segment_size,
            // dirty_messages: 0,
            // dirty_bytes: 0,
            closed: false,
        }
    }
    
    fn get(&self, id: u64) -> Result<(u64, Message), bool> {
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
        
        let body = unsafe {
            slice::from_raw_parts(
                self.file_mmap.offset((id - self.tail + size_of::<MessageHeader>() as u64) as isize),
                header.len as usize)
        };

        let message = Message {
            id: id,
            body: body
        };

        Ok((id + message_total_len, message))
    }

    fn append(&mut self, message: &Message) -> Option<u64> {
        let message_total_len = size_of::<MessageHeader>() + message.body.len();
        let file_offset = self.head - self.tail;

        if file_offset + message_total_len as u64 > self.file_size {
            self.closed = true;
            return None
        }

        let header = MessageHeader {
            id: self.head,
            len: message.body.len() as u32
        };

        unsafe {
            ptr::copy_nonoverlapping(
                mem::transmute(&header),
                self.file_mmap.offset(file_offset as isize),
                size_of::<MessageHeader>());
            ptr::copy_nonoverlapping(
                message.body.as_ptr(),
                self.file_mmap.offset(file_offset as isize + size_of::<MessageHeader>() as isize),
                message.body.len());
        }

        self.head += message_total_len as u64;
        // self.dirty_bytes += message_total_len;
        // self.dirty_messages += 1;

        Some(header.id)
    }

    fn sync(&mut self, sync: bool) {
        let flags = if sync { mman::MS_SYNC } else { mman::MS_ASYNC };
        mman::msync(self.file_mmap as *mut c_void, self.file_size as u64, flags).unwrap();
    }

    fn recover(&mut self, checkpoint: QueueFileCheckpoint) {
        info!("[{:?}] checkpoint loaded: {:?}", self.base_path, checkpoint);
        self.head = checkpoint.head;
        self.closed = checkpoint.closed;
        // TODO: read forward checking the message hashes
    }

    fn checkpoint(&mut self) -> QueueFileCheckpoint {
        // FIXME: reset and log stats
        let checkpoint = QueueFileCheckpoint {
            head: self.head,
            closed: self.closed,
        };
        self.sync(true);
        checkpoint
    }

    fn purge(self) {
        let mut base_path = self.base_path.clone();
        drop(self);
        base_path.set_extension(DATA_EXTENSION);
        let _ = fs::remove_file(&base_path);
    }
}

impl Drop for QueueFile {
    fn drop(&mut self) {
        mman::munmap(self.file_mmap as *mut c_void, self.file_size as u64).unwrap();
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
    fn get_queue_file(&self, index: usize) -> Option<&QueueFile> {
        let files = self.files.read();
        match files.get(&index) {
            Some(file_box_ref) => unsafe {
                Some(mem::transmute(&(**file_box_ref)))
            },
            _ => None
        }
    }

    fn get_queue_file_mut(&mut self, index: usize) -> Option<&mut QueueFile> {
        unsafe { mem::transmute(self.get_queue_file(index)) }
    }

    pub fn files_count(&self) -> usize {
        self.files.read().len()
    }

    /// Put a message at the end of the Queue, return the message id if succesfull
    /// Note: it's the caller responsability to serialize write calls
    pub fn put(&mut self, message: &Message) -> Option<u64> {
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
            let mut queue_file = Box::new(QueueFile::create(
                &self.config, head_file));
            let q_file_ptr = (&mut *queue_file) as *mut QueueFile;
            assert!(self.files.write().insert(head_file, queue_file).is_none());
            unsafe { mem::transmute(q_file_ptr) }
        };

        let result = q_file.append(message);
        if let Some(new_head) = result {
            self.head = new_head;
            result
        } else {
            panic!("Can't write to a newly created file!")
        }
    }

    /// Get a new message from the Queue just after the specified tail
    pub fn get(&mut self, mut tail: u64) -> Option<(u64, Message)> {
        let mut tail_file = (tail / self.config.segment_size) as usize;
        for _ in (0..2) {
            match self.get_queue_file(tail_file) {
                Some(q_file) => {
                    match q_file.get(tail) {
                        Ok(result) => return Some(result),
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
        // FIXME: terribly broken as msgs may still be pointing to the QueueFiles
        let mut locked_files = self.files.write();
        for (_, queue_file) in locked_files.drain() {
            queue_file.purge();
        }
        self.tail = 0;
        self.head = 0;
        let mut path = self.config.data_directory.join(BACKEND_CHECKPOINT_FILE);
        fs::remove_file(&path);
        path.set_file_name(TMP_BACKEND_CHECKPOINT_FILE);
        fs::remove_file(&path);
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

        let mut locked_files = self.files.write();
        for (file_num, file_checkpoint) in backend_checkpoint.files {
            info!("[{}] recovering file: {}", self.config.name, file_num);
            let queue_file = Box::new(QueueFile::open(
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

    pub fn checkpoint(&mut self) {
        let file_nums: Vec<_> = self.files.read().keys().collect();
        let file_checkpoints: BTreeMap<_, _> =  file_nums.into_iter().map(|file_num| {
            (file_num, self.get_queue_file_mut(file_num).unwrap().checkpoint())
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
                info!("[{:?}] checkpointed: {:?}", self.config.name, checkpoint);
            }
            Err(error) => {
                warn!("[{:?}] error writing checkpoint information: {}",
                    self.config.name, error);
            }
        }
    }

    pub fn gc(&mut self, smallest_tail: u64) {
        let mut file_nums = Vec::new();
        // collect files_nums until the first still open or with tail >= smallest_tail
        for (file_num, queue_file) in &*self.files.read() {
            if ! queue_file.closed || queue_file.tail < smallest_tail {
                break
            }
            file_nums.push(file_num);
        }
        // purge them
        for file_num in file_nums {
            let queue_file = self.files.write().remove(&file_num).unwrap();
            queue_file.purge();
        }
    }
}
