use std::slice;
use mio::{Buf, MutBuf};
use std::mem::{self, size_of};

const REQUEST_MAGIC: u8 = 0x80;
const RESPONSE_MAGIC: u8 = 0x81;

const DATA_TYPE_RAW: u8 = 0x0;

#[derive(Debug)]
#[repr(C)]
enum Opcode {
    Get = 0x0,
    Set = 0x1,
    Delete = 0x4,
    Flush = 0x8
}

#[derive(Debug)]
#[repr(C)]
enum Status {
	Ok = 0x0,
	ValueTooLarge = 0x3,
	InvalidArguments = 0x4,
	NotStored = 0x5,
	UknownCommand = 0x81,
	OutOfMemory = 0x82
}

#[derive(Debug)]
#[repr(packed)]
pub struct RequestHeader {
    magic: u8,
    opcode: u8,
    key_len: u16,
    extras_len: u8,
    data_type: u8,
    vbucket_id: u16,
    total_body_len: u32,
    opaque: u32,
    cas: u64
}

#[derive(Debug)]
#[repr(packed)]
struct ResponseHeader {
    magic: u8,
    opcode: u8,
    key_len: u16,
    extras_len: u8,
    data_type: u8,
    status: u16,
    total_body_len: u32,
    opaque: u32,
    cas: u64
}

impl RequestHeader {
	pub fn total_len(&self) -> usize {
		size_of::<Self>() + self.total_body_len as usize
	}

	pub fn value_len(&self) -> usize {
		self.total_body_len as usize - self.extras_len as usize - self.key_len as usize
	}

	pub fn total_body_len(&self) -> usize {
		self.total_body_len as usize
	}

	pub fn key_len(&self) -> usize {
		self.key_len as usize
	}

	fn extras_len(&self) -> usize {
		self.extras_len as usize
	}
}

#[derive(Debug)]
pub struct RequestBuffer {
    header: RequestHeader,
    body: Vec<u8>,
    bytes_read: usize
}

impl RequestBuffer {
	pub fn new() -> RequestBuffer {
		RequestBuffer {
			header: unsafe { mem::uninitialized() },
			body: Vec::new(),
			bytes_read: 0,
		}
	}

	pub fn clear(&mut self) {
		self.body.clear();
		self.bytes_read = 0;
	}

	// pub fn header(&self) -> &RequestHeader {
	// 	&self.header
	// }
	
	pub fn key_slice(&self) -> &[u8] {
		debug_assert!(self.is_complete());
		&self.body[self.header.extras_len()..self.header.extras_len() + self.header.key_len()]
	}

	pub fn value_slice(&self) -> &[u8] {
		debug_assert!(self.is_complete());
		&self.body[self.header.extras_len() + self.header.key_len()..]
	}

	pub fn is_complete(&self) -> bool {
		self.bytes_read == size_of::<RequestHeader>() + self.body.capacity()
	}

	fn is_too_large(&self) -> bool {
		if self.bytes_read <= size_of::<RequestHeader>() {
			false
		} else {
			self.header.key_len() > 256 || self.header.total_body_len > 256 * 1024
		}
	}
}

impl MutBuf for RequestBuffer {
	fn remaining(&self) -> usize {
		if self.bytes_read < size_of::<RequestHeader>() {
			size_of::<RequestHeader>() - self.bytes_read
		} else {
			self.header.total_len() - self.bytes_read
		}
	}
    fn advance(&mut self, cnt: usize) {
    	if self.bytes_read < size_of::<RequestHeader>() &&
    			self.bytes_read + cnt >= size_of::<RequestHeader>() {
			debug_assert!(self.bytes_read + cnt == size_of::<RequestHeader>());
    		self.body.reserve_exact(self.header.total_body_len());
    		unsafe { self.body.set_len(self.header.total_body_len()) };
		}
    	self.bytes_read += cnt;
    }
    fn mut_bytes(&mut self) -> &mut [u8] {
    	debug_assert!(!self.is_too_large() && !self.is_complete());
    	unsafe {
    		let u8_ptr = if self.bytes_read < size_of::<RequestHeader>() {
		    	(&mut self.header as *mut RequestHeader as *mut u8).offset(self.bytes_read as isize)
	    	} else {
	    		self.body.as_mut_ptr().offset((self.bytes_read - size_of::<RequestHeader>()) as isize)
	    	};
    		slice::from_raw_parts_mut(u8_ptr, self.remaining())
    	}
    }
}


#[cfg(test)]
mod tests {
	use super::*;
	use mio::{Buf, MutBuf};
	use std::io::Write;
	use std::slice;
	use std::mem::{self, size_of};

	const REQ_HEADER_SAMPLE: RequestHeader = RequestHeader {
	    magic: 0,
	    opcode: 0,
	    key_len: 2,
	    extras_len: 2,
	    data_type: 0,
	    vbucket_id: 0,
	    total_body_len: 10,
	    opaque: 0,
	    cas: 0
	};

	#[test]
	fn test_fill_clear_loop() {
		let mut buf = RequestBuffer::new();
		for _ in (0..3) {
			assert!(!buf.is_complete());
			assert_eq!(buf.remaining(), size_of::<RequestHeader>());
			buf.mut_bytes().write_all(unsafe {
				slice::from_raw_parts(
					&REQ_HEADER_SAMPLE as *const RequestHeader as *const u8,
					size_of::<RequestHeader>())
			}).unwrap();
			buf.advance(size_of::<RequestHeader>());

			assert!(!buf.is_complete());
			assert_eq!(buf.remaining(), 10);
			buf.mut_bytes().write_all(&[0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9]).unwrap();
			buf.advance(10);
			assert_eq!(buf.remaining(), 0);
			assert!(buf.is_complete());

			assert_eq!(buf.key_slice(), &[2u8, 3]);
			assert_eq!(buf.value_slice(), &[4u8, 5, 6, 7, 8, 9]);
			buf.clear();
		}
	}

	#[test]
	fn test_partial_fill() {
		let mut buf = RequestBuffer::new();
		let sample_ptr = &REQ_HEADER_SAMPLE as *const RequestHeader as *const u8;
		buf.mut_bytes().write_all(unsafe {
			slice::from_raw_parts(sample_ptr, size_of::<RequestHeader>() - 12)
		}).unwrap();
		buf.advance(size_of::<RequestHeader>() - 12);
		assert_eq!(buf.remaining(), 12);

		buf.mut_bytes().write_all(unsafe {
			slice::from_raw_parts(sample_ptr.offset(size_of::<RequestHeader>() as isize - 12), 12)
		}).unwrap();
		buf.advance(12);
		assert_eq!(buf.remaining(), 10);


		buf.mut_bytes().write_all(&[0u8, 1, 2, 3, 4]).unwrap();
		buf.advance(5);
		assert_eq!(buf.remaining(), 5);


		buf.mut_bytes().write_all(&[5u8, 6, 7, 8, 9]).unwrap();
		buf.advance(5);
		assert_eq!(buf.remaining(), 0);

		assert_eq!(buf.key_slice(), &[2u8, 3]);
		assert_eq!(buf.value_slice(), &[4u8, 5, 6, 7, 8, 9]);
		assert!(buf.is_complete());
	}

}