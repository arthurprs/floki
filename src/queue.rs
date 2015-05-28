use std::io::prelude::*;
use std::fs::{File, OpenOptions};
use std::os::unix::io::AsRawFd;
use std::sync::{Mutex, RwLock};
use std::sync::atomic::{AtomicIsize, Ordering};
use std::collections::VecDeque;
use nix::c_void;	
use nix::sys::mman;
use std::ptr;
use std::fs;
use std::mem::{self, size_of};
use std::slice;
use std::rc::Rc;
use std::path::{PathBuf, Path};

use config::*;

pub struct Message<'a> {
	pub id: u64,
	pub body: &'a[u8]
}

#[repr(C)]
struct MessageHeader {
	id: u64,
	len: u32,
}

struct QueueFile {
	file: File,
	file_size: u64,
	file_mmap: *mut u8,
	start_id: u64,
	offset: u64,
	dirty_bytes: u64,
	dirty_messages: u64,
	closed: bool,
}

struct QueueBackend {
	config: Rc<QueueConfig>,
	files: RwLock<Vec<Option<Box<QueueFile>>>>,
	head: u64,
	tail: u64
}

pub struct Queue {
	config: Rc<QueueConfig>,
	backend_wlock: Mutex<()>,
	backend_rlock: RwLock<()>,
	backend: QueueBackend,
	tail: u64,
	message_count: u64,
	total_message_count: u64
}

// pub struct Channel {
//     config: Rc<QueueConfig>,
//     tail: u64
// }

impl QueueFile {
	fn create<P: AsRef<Path>>(config: &QueueConfig, path: P, start_id: u64) -> QueueFile {
		let file = OpenOptions::new()
				.read(true)
				.write(true)
				.create(true)
				.open(path.as_ref()).unwrap();
		file.set_len(config.segment_size).unwrap();
		Self::new(config, file, start_id)
	}

	fn open<P: AsRef<Path>>(config: &QueueConfig, path: P, start_id: u64) -> QueueFile {
		let file = OpenOptions::new()
				.read(true)
				.write(true)
				.open(path.as_ref()).unwrap();
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
		if id >= self.start_id + self.offset {
			return Err(self.closed)
		}

		let header: &MessageHeader = unsafe {
			mem::transmute(self.file_mmap.offset((id - self.start_id) as isize))
		};

		// check id and possible overflow
		assert_eq!(header.id, id);
		assert!(id - self.start_id + size_of::<MessageHeader>() as u64 + header.len as u64 <= self.offset);

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
		if self.offset + message_total_len > self.file_size {
			self.closed = true;
			return None
		}

		let header = MessageHeader {
			id: self.start_id + self.offset,
			len: message.body.len() as u32
		};

		unsafe {
			ptr::copy_nonoverlapping(
				mem::transmute(&header),
				self.file_mmap.offset(self.offset as isize),
				size_of::<MessageHeader>());
			ptr::copy_nonoverlapping(
				message.body.as_ptr(),
				self.file_mmap.offset((self.offset + size_of::<MessageHeader>() as u64) as isize),
				message.body.len());
		}
		
		self.offset += message_total_len;
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
	pub fn new(config: Rc<QueueConfig>) -> QueueBackend {
		QueueBackend {
			config: config,
			files: RwLock::new(Vec::new()),
			head: 0,
			tail: 0
		}
	}

	fn gen_file_path(&self, file_num: usize) -> PathBuf {
		let file_name = format!("data{}.bin", file_num);
		self.config.data_directory.join(file_name)
	}

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
							tail += self.config.segment_size - tail % self.config.segment_size;
							tail_file += 1;
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
		let	rc_config = Rc::new(config);
		Queue {
			config: rc_config.clone(),
			backend_wlock: Mutex::new(()),
			backend_rlock: RwLock::new(()),
			backend: QueueBackend::new(rc_config.clone()),
			tail: 0,
			message_count: 0,
			total_message_count: 0
		}
	}

	/// read access is suposed to be thread-safe, even while writing
	pub fn get(&mut self) -> Option<Message> {
		let _ = self.backend_rlock.read().unwrap();
		if let Some((next_tail, message)) = self.backend.get(self.tail) {
			self.tail = next_tail;
			Some(message)
		} else {
			None
		}
	}

	/// all calls are serialized internally
	pub fn put(&mut self, message: &Message) -> Option<u64> {
		let _ = self.backend_wlock.lock().unwrap();
		let result = self.backend.put(message);
		if result.is_some() {
			self.message_count += 1;
			self.total_message_count += 1;
		}
		result
	}
	
	pub fn delete(&mut self, id: u64) -> bool {
		false
	}

	pub fn in_flight_count(&self) -> u32 {
		0
	}

	pub fn message_count(&self) -> u32 {
		0
	}

	pub fn purge(&mut self) {
		let _ = self.backend_rlock.write().unwrap();
		let _ = self.backend_wlock.lock().unwrap();
		self.backend.purge();
	}

	pub fn maintenance(&mut self) {
		// add gc code here
	}
}


#[cfg(test)]
mod tests {
	use super::*;
	use config::*;
	use test;

	fn get_queue() -> Queue {
		let server_config = ServerConfig::read();
	    let mut queue_configs = server_config.read_queue_configs();
	    Queue::new(
	    	queue_configs.pop().unwrap_or_else(|| server_config.new_queue_config("test"))
		)
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
		for i in (0..100_000) {
			assert!(q.put(&message).is_some());
			let m = q.get();
			assert!(m.is_some());
		}
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
		b.bytes = m.body.len() as u64;
		b.iter(|| {
			let p = q.put(m);
			let r = q.get();
			p.unwrap() == r.unwrap().id
		});
	}
}