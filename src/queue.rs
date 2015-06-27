use std::borrow::Borrow;
use std::hash::Hash;
use std::io::prelude::*;
use std::ops::Deref;
use std::fs::{File, OpenOptions};
use std::os::unix::io::AsRawFd;
use std::cell::UnsafeCell;
use std::sync::{Arc, Mutex, RwLock};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::collections::hash_state::DefaultState;
use std::ptr;
use std::fs;
use std::mem::{self, size_of};
use std::slice;
use std::rc::Rc;
use std::path::{PathBuf, Path};
use nix::c_void;
use nix::sys::mman;
use linked_hash_map::LinkedHashMap;
use time::precise_time_s;
use fnv::FnvHasher;

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
struct QueueFile {
    file: File,
    file_size: u64,
    file_mmap: *mut u8,
    start_id: u64,
    // u32 to allow atomic reads on 32bit platforms
    offset: u32,
    dirty_bytes: u64,
    dirty_messages: u64,
    closed: bool,
}

#[derive(Debug)]
struct QueueBackend {
    config: Rc<QueueConfig>,
    files: RwLock<Vec<Option<Box<QueueFile>>>>,
    head: u64,
    tail: u64
}

#[derive(Debug)]
pub struct Channel {
    tail: u64,
    message_count: u64,
    in_flight: LinkedHashMap<u64, u32, DefaultState<FnvHasher>>
}

#[derive(Debug)]
pub struct Queue {
    config: Rc<QueueConfig>,
    // backend writes don't block readers
    backend_wlock: Mutex<()>,
    backend_rlock: RwLock<()>,
    backend: QueueBackend,
    channels: RwLock<HashMap<String, Mutex<Channel>, DefaultState<FnvHasher>>>,
    clock: u32
}

#[derive(Clone)]
pub struct ArcQueue(Arc<Queue>);

unsafe impl Sync for ArcQueue {}
unsafe impl Send for ArcQueue {}

impl Deref for ArcQueue {
    type Target = Queue;
    fn deref(&self) -> &Self::Target {
        let &ArcQueue(ref queue) = self;
        queue
    }
}

impl ArcQueue {
    pub fn new(queue: Queue) -> ArcQueue {
        ArcQueue(Arc::new(queue))
    }
}

impl QueueFile {
    fn create<P: AsRef<Path>>(config: &QueueConfig, path: P, start_id: u64) -> QueueFile {
        let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&path).unwrap();
        file.set_len(config.segment_size).unwrap();
        Self::new(config, file, start_id)
    }

    fn open<P: AsRef<Path>>(config: &QueueConfig, path: P, start_id: u64) -> QueueFile {
        let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path).unwrap();
        Self::new(config, file, start_id)
    }

    fn new(config: &QueueConfig, file: File, start_id: u64) -> QueueFile {
        let f_len = file.metadata().unwrap().len();
        assert_eq!(f_len, config.segment_size);

        let f_mmap_ptr = mman::mmap(
            ptr::null_mut(), f_len,
            mman::PROT_READ | mman::PROT_WRITE, mman::MAP_SHARED,
            file.as_raw_fd(), 0).unwrap() as *mut u8;
        
        QueueFile {
            file: file,
            file_size: config.segment_size,
            file_mmap: f_mmap_ptr,
            start_id: start_id,
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
        let message_total_len = (size_of::<MessageHeader>() + message.body.len()) as u64;
        if self.offset as u64 + message_total_len > self.file_size {
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
        
        self.offset += message_total_len as u32;
        self.dirty_bytes += message_total_len;
        self.dirty_messages += 1;

        Some(header.id)
    }

    fn sync(&mut self, sync: bool) {
        let flags = if sync { mman::MS_SYNC } else { mman::MS_ASYNC };
        mman::msync(self.file_mmap as *mut c_void, self.file_size, flags).unwrap();
    }
}

impl Drop for QueueFile {
    fn drop(&mut self) {
        mman::munmap(self.file_mmap as *mut c_void, self.file_size).unwrap();
    }
}

impl QueueBackend {
    fn new(config: Rc<QueueConfig>) -> QueueBackend {
        QueueBackend {
            config: config,
            files: RwLock::new(Vec::new()),
            head: 0,
            tail: 0
        }
    }

    fn gen_file_path(&self, file_num: usize) -> PathBuf {
        let file_name = format!("data{:08}.bin", file_num);
        self.config.data_directory.join(file_name)
    }

    /// this relies on Box beeing at the same place even if this vector is reallocated
    /// also, this don't do any ref count, so one must make sure the mmap is alive while there
    /// are messages pointing to this QueueFile
    fn get_queue_file(&self, index: usize) -> Option<&QueueFile> {
        let files = self.files.read().unwrap();
        match files.get(index) {
            Some(&Some(ref file_box_ref)) => unsafe {
                let file_box_ptr: *const *const QueueFile = mem::transmute(file_box_ref);
                Some(mem::transmute(*file_box_ptr))
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
    fn put(&mut self, message: &Message) -> Option<u64> {
        let mut head_file = (self.head / self.config.segment_size) as usize;
        let result = if let Some(q_file) = self.get_queue_file_mut(head_file) {
            q_file.append(message)
        } else {
            None
        };

        if let Some(new_head) = result {
            self.head = new_head;
            return result
        } else {
            if self.head == 0 {
                head_file = 0
            } else {
                head_file += 1
            }
        }
        let q_file: &mut QueueFile = {
            let mut queue_file = Box::new(QueueFile::create(
                &self.config,
                self.gen_file_path(head_file),
                head_file as u64 * self.config.segment_size));
            let q_file_ptr = (&mut *queue_file) as *mut QueueFile;
            self.files.write().unwrap().push(Some(queue_file));
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
    fn get(&mut self, mut tail: u64) -> Option<(u64, Message)> {
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

    fn purge(&mut self) {
        self.files.write().unwrap().clear();
        fs::remove_dir_all(&self.config.data_directory).unwrap();
        fs::create_dir_all(&self.config.data_directory).unwrap();
        self.tail = 0;
        self.head = 0;
    }
}

impl Queue {

    pub fn new(config: QueueConfig) -> Queue {
        let rc_config = Rc::new(config);
        let mut queue = Queue {
            config: rc_config.clone(),
            backend_wlock: Mutex::new(()),
            backend_rlock: RwLock::new(()),
            backend: QueueBackend::new(rc_config.clone()),
            channels: RwLock::new(Default::default()),
            clock: 0,
        };
        queue.tick();
        queue
    }

    pub fn create_channel<S>(&mut self, channel_name: S) -> bool
            where String: From<S> {
        let channel_name: String = channel_name.into();
        let mut locked_channel = self.channels.write().unwrap();
        if let Entry::Vacant(vacant_entry) = locked_channel.entry(channel_name) {
            vacant_entry.insert(
                Mutex::new(
                    Channel {
                        tail: 0,
                        message_count: 0,
                        in_flight: Default::default()
                    }
                )
            );
            true
        } else {
            false
        }
    }

    /// get access is suposed to be thread-safe, even while writing
    pub fn get(&mut self, channel_name: &str) -> Option<Message> {
        let _ = self.backend_rlock.read().unwrap();
        let locked_channels = self.channels.read().unwrap();
        if let Some(channel) = locked_channels.get(channel_name) {
            let mut locked_channel = channel.lock().unwrap();

            // check in flight queue for timeouts
            if let Some((&id, &until)) = locked_channel.in_flight.front() {
                if until > self.clock {
                    // double get bellow, not ideal
                    let new_until = locked_channel.in_flight.get_refresh(&id).unwrap();
                    *new_until = self.clock + self.config.time_to_live;
                    return Some(self.backend.get(id).unwrap().1)
                }
            }

            // fetch from the backend
            if let Some((new_tail, message)) = self.backend.get(locked_channel.tail) {
                locked_channel.tail = new_tail;
                return Some(message)
            }
        }
        None
    }

    /// all calls are serialized internally
    pub fn put(&mut self, message: &Message) -> Option<u64> {
        let _ = self.backend_wlock.lock().unwrap();
        self.backend.put(message)
    }

    /// delete access is suposed to be thread-safe, even while writing
    pub fn delete(&mut self, channel_name: &str, id: u64) -> Option<bool> {
        let _ = self.backend_rlock.read().unwrap();
        let locked_channels = self.channels.read().unwrap();
        if let Some(channel) = locked_channels.get(channel_name) {
            let removed_opt = channel.lock().unwrap().in_flight.remove(&id);
            return Some(removed_opt.is_some())
        }
        None
    }

    pub fn purge(&mut self) {
        let _ = self.backend_rlock.write().unwrap();
        let _ = self.backend_wlock.lock().unwrap();
        self.backend.purge();
    }

    pub fn maintenance(&mut self) {
        // add gc code here
    }

    pub fn tick(&mut self) {
        self.clock = precise_time_s() as u32;
    }

    #[allow(mutable_transmutes)]
    pub fn as_mut(&self) -> &mut Self {
        unsafe { mem::transmute(self) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use config::*;
    use std::thread;
    use test;

    fn get_queue() -> Queue {
        let server_config = ServerConfig::read();
        let thread = thread::current();
        Queue::new(server_config.new_queue_config(thread.name().unwrap()))
    }

    fn gen_message(id: u64) -> Message<'static> {
        Message {
            id: id,
            body: b"333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333"
        }
    }

    #[test]
    fn test_fill() {
        let mut q = get_queue();
        let message = gen_message(0);
        for i in (0..100_000) {
            let r = q.put(&message);
            assert!(r.is_some());
        }
    }

    #[test]
    fn test_put_get() {
        let mut q = get_queue();
        let message = gen_message(0);
        assert!(q.create_channel("test"));
        for i in (0..100_000) {
            assert!(q.put(&message).is_some());
            let m = q.get("test");
            assert!(m.is_some());
        }
    }

    #[test]
    fn test_create_channel() {
        let mut q = get_queue();
        let message = gen_message(0);
        assert!(q.get("test").is_none());
        assert!(q.put(&message).is_some());
        assert!(q.create_channel("test") == true);
        assert!(q.create_channel("test") == false);
        assert!(q.get("test").is_some());
    }

    #[test]
    fn test_channel_in_flight() {
        let mut q = get_queue();
        let message = gen_message(0);
        assert!(q.get("test").is_none());
        assert!(q.put(&message).is_some());
        assert!(q.create_channel("test") == true);
        assert!(q.create_channel("test") == false);
        assert!(q.get("test").is_some());
    }

    #[bench]
    fn put_like_crazy(b: &mut test::Bencher) {
        let mut q = get_queue();
        let m = &gen_message(0);
        b.bytes = m.body.len() as u64;
        b.iter(|| {
            let r = q.put(m);
            assert!(r.is_some());
            r
        });
    }

    #[bench]
    fn put_get_like_crazy(b: &mut test::Bencher) {
        let mut q = get_queue();
        let m = &gen_message(0);
        q.create_channel("test");
        b.bytes = m.body.len() as u64;
        b.iter(|| {
            let p = q.put(m);
            let r = q.get("test");
            p.unwrap() == r.unwrap().id
        });
    }
}
