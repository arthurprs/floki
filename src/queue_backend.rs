use std::ptr;
use std::io::prelude::*;
use std::fs::{self, File, OpenOptions};
use std::path::PathBuf;
use std::os::unix::io::AsRawFd;
use nix::c_void;
use nix::sys::mman;
use std::sync::RwLock;
use std::rc::Rc;
use std::slice;
use std::mem::{self, size_of};
use std::collections::VecMap;

use config::*;

#[derive(Debug)]
pub struct Message<'a> {
    pub id: u64,
    pub body: &'a[u8]
}

#[repr(packed)]
struct MessageHeader {
    id: u64,
    len: u32,
}

#[derive(Debug)]
pub struct QueueFile {
    file: File,
    base_path: PathBuf,
    file_size: usize,
    file_mmap: *mut u8,
    file_num: usize,
    start_id: u64,
    // usize to allow atomic reads on 32bit platforms
    // FIXME: must make sure these don't cross cache lines
    offset: usize,
    dirty_bytes: usize,
    dirty_messages: usize,
    closed: bool,
}

#[derive(Debug)]
pub struct QueueBackend {
    config: Rc<QueueConfig>,
    files: RwLock<VecMap<Box<QueueFile>>>,
    head: u64,
    tail: u64
}


impl QueueFile {
    fn base_file_path(config: &QueueConfig, file_num: usize) -> PathBuf {
        config.data_directory.join(format!("{:08}", file_num))
    }

    fn create(config: &QueueConfig, file_num: usize) -> QueueFile {
        let base_path = Self::base_file_path(config, file_num);
        let data_path = base_path.with_extension("bin");
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

    fn open(config: &QueueConfig, file_num: usize) -> QueueFile {
        let base_path = Self::base_file_path(config, file_num);
        let data_path = base_path.with_extension("data");
        debug!("[{}] opening data file {:?}", config.name, data_path);
        let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(data_path).unwrap();
        let mut queue_file = Self::new(config, file, base_path, file_num);
        queue_file.recover();
        queue_file
    }

    fn new(config: &QueueConfig, file: File, base_path: PathBuf, file_num: usize) -> QueueFile {
        let file_len = file.metadata().unwrap().len();
        assert_eq!(file_len, config.segment_size);

        let file_mmap = mman::mmap(
            ptr::null_mut(), file_len,
            mman::PROT_READ | mman::PROT_WRITE, mman::MAP_SHARED,
            file.as_raw_fd(), 0).unwrap() as *mut u8;
        
        QueueFile {
            file: file,
            base_path: base_path,
            file_size: config.segment_size as usize,
            file_mmap: file_mmap,
            file_num: file_num,
            start_id: file_num as u64 * config.segment_size,
            offset: 0,
            dirty_messages: 0,
            dirty_bytes: 0,
            closed: false,
        }
    }
    
    fn get(&self, id: u64) -> Result<(u64, Message), bool> {
        if id >= self.start_id + self.offset as u64 {
            return Err(self.closed)
        }

        let header: &MessageHeader = unsafe {
            mem::transmute(self.file_mmap.offset((id - self.start_id) as isize))
        };

        // check id and possible overflow
        assert_eq!(header.id, id);
        assert!(id - self.start_id + size_of::<MessageHeader>() as u64 + header.len as u64 <= self.offset as u64);

        let message_total_len = (size_of::<MessageHeader>() + header.len as usize) as u64;
        
        let body = unsafe {
            slice::from_raw_parts(
                self.file_mmap.offset((id - self.start_id + size_of::<MessageHeader>() as u64) as isize),
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
        if self.offset + message_total_len > self.file_size {
            self.closed = true;
            return None
        }

        let header = MessageHeader {
            id: self.start_id + self.offset as u64,
            len: message.body.len() as u32
        };

        unsafe {
            ptr::copy_nonoverlapping(
                mem::transmute(&header),
                self.file_mmap.offset(self.offset as isize),
                size_of::<MessageHeader>());
            ptr::copy_nonoverlapping(
                message.body.as_ptr(),
                self.file_mmap.offset(self.offset as isize + size_of::<MessageHeader>() as isize),
                message.body.len());
        }

        self.offset += message_total_len;
        self.dirty_bytes += message_total_len;
        self.dirty_messages += 1;

        Some(header.id)
    }

    fn sync(&mut self, sync: bool) {
        let flags = if sync { mman::MS_SYNC } else { mman::MS_ASYNC };
        mman::msync(self.file_mmap as *mut c_void, self.file_size as u64, flags).unwrap();
    }

    fn recover(&mut self) {
        let path = self.base_path.with_extension("checkpoint");
        match File::open(path) {
            Ok(mut file) => {
                let mut contents = String::new();
                let _ = file.read_to_string(&mut contents);
                match contents.parse::<usize>() {
                    Ok(checkpoint) => {
                        info!("[{:?}] checkpoint loaded with offset {}",
                            self.base_path, checkpoint);
                        self.offset = checkpoint;
                    }
                    Err(error) => {
                        warn!("[{:?}] error parsing checkpoint information: {}",
                            self.base_path, error);
                        return;
                    }
                }
            }
            Err(error) => {
                warn!("[{:?}] error reading checkpoint information: {}",
                    self.base_path, error);
                return;
            }
        }

        // TODO: read forward checking the message hashes
    }

    fn checkpoint(&mut self) {
        // FIXME: reset and log stats
        let offset = self.offset;
        self.sync(true);

        let tmp_path = self.base_path.with_extension("checkpoint.tmp");
        let result = File::create(&tmp_path)
            .and_then(|mut file| {
                write!(file, "{}", offset);
                file.sync_data()
            }).and_then(|_| {
                let final_path = self.base_path.with_extension("checkpoint");
                fs::rename(tmp_path, final_path)
            });

        match result {
            Ok(_) =>
                info!("[{:?}] checkpointed: {}",
                    self.base_path, offset),
            Err(error) =>
                warn!("[{:?}] error writing checkpoint information: {}",
                    self.base_path, error)
        }
    }
}

impl Drop for QueueFile {
    fn drop(&mut self) {
        mman::munmap(self.file_mmap as *mut c_void, self.file_size as u64).unwrap();
    }
}

impl QueueBackend {
    pub fn new(config: Rc<QueueConfig>) -> QueueBackend {
        QueueBackend {
            config: config,
            files: RwLock::new(VecMap::new()),
            head: 0,
            tail: 0
        }
    }

    fn recover(&mut self) {
        let result = fs::read_dir(&self.config.data_directory).map(|dir| {
            let file_nums = dir.filter_map(|entry_opt| {
                entry_opt.ok().and_then(|entry| {
                    let entry_path = entry.path();
                    match (entry_path.file_stem(), entry_path.extension()) {
                        (Some(stem), Some(ext)) if ext == "data" => {
                            stem.to_str().and_then(|s| s.parse::<usize>().ok())
                        }
                        _ => None
                    }
                })
            });
            let mut locked_files = self.files.write().unwrap();
            for file_num in file_nums {
                let queue_file = Box::new(QueueFile::open(&self.config, file_num));
                locked_files.insert(file_num, queue_file);
            }
        });

        if let Err(error) = result {
            warn!("[{}] error while recovering queue: {}", self.config.name, error);
        }
    }

    /// this relies on Box beeing at the same place even if this vector is reallocated
    /// also, this doen't do any ref count, so one must make sure the mmap is alive while there
    /// are messages pointing to this QueueFile
    fn get_queue_file(&self, index: usize) -> Option<&QueueFile> {
        let files = self.files.read().unwrap();
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

    fn files_count(&self) -> usize {
        self.files.read().unwrap().len()
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
            assert!(self.files.write().unwrap().insert(head_file, queue_file).is_none());
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
        self.files.write().unwrap().clear();
        fs::remove_dir_all(&self.config.data_directory).unwrap();
        fs::create_dir_all(&self.config.data_directory).unwrap();
        self.tail = 0;
        self.head = 0;
    }
}
