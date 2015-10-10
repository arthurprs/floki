use mio::{Buf, MutBuf};
use std::slice;
use std::mem::{self, size_of};
use std::io::Write;
use std::str;
use std::fmt;
use std::os::unix::io::RawFd;

use queue_backend::Message;

const REQUEST_MAGIC: u8 = 0x80;
const RESPONSE_MAGIC: u8 = 0x81;

const DATA_TYPE_RAW: u8 = 0x0;

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
#[repr(u8)]
pub enum OpCode {
    Get = 0x0,
    Set = 0x1,
    Delete = 0x4,
    Exit = 0x7,
    Flush = 0x8,
    GetQ = 0x9,
    Stat = 0x10,
    NoOp = 0xA,
    Version = 0xB,
    GetK = 0xC,
    GetKQ = 0xD
}

impl OpCode {
    pub fn include_key(&self) -> bool {
        match *self {
            OpCode::GetK | OpCode::GetKQ => true,
            _ => false
        }
    }
    pub fn is_quiet(&self) -> bool {
        match *self {
            OpCode::GetQ | OpCode::GetKQ => true,
            _ => false
        }
    }
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
#[repr(u16)]
pub enum Status {
    NoError = 0x0,
    KeyNotFound = 0x1,
    ValueTooLarge = 0x3,
    InvalidArguments = 0x4,
    UknownCommand = 0x81
}

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

#[repr(packed)]
pub struct ResponseHeader {
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

impl fmt::Debug for RequestHeader {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("RequestHeader")
            .field("magic", &self.magic.to_be())
            .field("opcode", &self.opcode.to_be())
            .field("key_len", &self.key_len.to_be())
            .field("extras_len", &self.extras_len.to_be())
            .field("data_type", &self.data_type.to_be())
            .field("vbucket_id", &self.vbucket_id.to_be())
            .field("total_body_len", &self.total_body_len.to_be())
            .field("opaque", &self.opaque.to_be())
            .field("cas", &self.cas.to_be())
            .finish()
    }
}

impl fmt::Debug for ResponseHeader {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("ResponseHeader")
            .field("magic", &self.magic.to_be())
            .field("opcode", &self.opcode.to_be())
            .field("key_len", &self.key_len.to_be())
            .field("extras_len", &self.extras_len.to_be())
            .field("data_type", &self.data_type.to_be())
            .field("status", &self.status.to_be())
            .field("total_body_len", &self.total_body_len.to_be())
            .field("opaque", &self.opaque.to_be())
            .field("cas", &self.cas.to_be())
            .finish()
    }
}

impl RequestHeader {
    pub fn get_total_len(&self) -> usize {
        size_of::<Self>() + self.total_body_len.to_be() as usize
    }

    pub fn get_value_len(&self) -> usize {
        self.total_body_len.to_be() as usize - self.extras_len.to_be() as usize - self.key_len.to_be() as usize
    }

    pub fn get_total_body_len(&self) -> usize {
        self.total_body_len.to_be() as usize
    }

    pub fn get_key_len(&self) -> usize {
        self.key_len.to_be() as usize
    }

    pub fn get_extras_len(&self) -> usize {
        self.extras_len.to_be() as usize
    }
}

impl ResponseHeader {
    pub fn get_total_len(&self) -> usize {
        size_of::<Self>() + self.total_body_len.to_be() as usize
    }

    pub fn get_value_len(&self) -> usize {
        self.total_body_len.to_be() as usize - self.extras_len.to_be() as usize - self.key_len.to_be() as usize
    }

    pub fn get_total_body_len(&self) -> usize {
        self.total_body_len.to_be() as usize
    }

    pub fn get_key_len(&self) -> usize {
        self.key_len.to_be() as usize
    }

    pub fn get_extras_len(&self) -> usize {
        self.extras_len.to_be() as usize
    }

    pub fn set_total_body_len(&mut self, value: usize) {
        self.total_body_len = (value as u32).to_be()
    }

    pub fn set_key_len(&mut self, value: usize) {
        self.key_len = (value as u16).to_be()
    }

    pub fn set_extras_len(&mut self, value: usize) {
        self.extras_len = (value as u8).to_be()
    }
}

#[derive(Debug)]
pub struct RequestBuffer {
    body: Vec<u8>,
    bytes_read: u32,
    arg_offsets: [(u16, u16); 3],
}

#[derive(Debug)]
pub struct ResponseBuffer {
    body: Vec<u8>,
    bytes_written: usize,
    pub message: Option<Message>,
}

impl RequestBuffer {
    pub fn new() -> RequestBuffer {
        let mut v = Vec::with_capacity(4096);
        unsafe { v.set_len(4096) };
        RequestBuffer {
            body: v,
            bytes_read: 0,
            arg_offsets: [(0xFFFF, 0xFFFF); 3],
        }
    }

    fn parse_args(&mut self) {
        let mut arg_count = 0;
        let mut start = 0;
        let mut arg_offsets = [(0xFFFF, 0xFFFF); 3];
        for (colon_offset, _) in self.key_str().match_indices(':') {
            arg_offsets[arg_count] = (start as u16, colon_offset as u16);
            start = colon_offset + 1;
            arg_count += 1;
            if arg_count >= 2 {
                break
            }
        }
        arg_offsets[arg_count] = (start as u16, self.key_str().len() as u16);
        self.arg_offsets = arg_offsets;
    }

    #[inline(always)]
    fn header(&self) -> &RequestHeader {
        unsafe { mem::transmute(self.body.as_ptr()) }
    }

    #[inline(always)]
    fn header_mut(&self) -> &mut RequestHeader {
        unsafe { mem::transmute(self.body.as_ptr()) }
    }

    pub fn clear(&mut self) {
        unsafe { self.body.set_len(4096) };
        self.bytes_read = 0;
    }

    pub fn arg_count(&self) -> usize {
        self.arg_offsets.len()
    }

    pub fn arg_str(&self, arg_number: usize) -> Option<&str> {
        self.arg_offsets.get(arg_number).and_then(|&(start, end)| {
            if start != 0xFFFF {
                Some(unsafe { str::from_utf8_unchecked(&self.key_slice()[start as usize..end as usize]) })
            } else {
                None
            }
        })
    }

    pub fn arg_uint(&self, arg_number: usize) -> Option<u64> {
        self.arg_str(arg_number).and_then(|s| s.parse::<u64>().ok())
    }

    pub fn key_str(&self) -> &str {
        unsafe { str::from_utf8_unchecked(self.key_slice()) }
    }
    
    pub fn key_slice(&self) -> &[u8] {
        debug_assert!(self.is_complete());
        &self.body[size_of::<RequestHeader>() + self.header().get_extras_len()..size_of::<RequestHeader>() + self.header().get_extras_len() + self.header().get_key_len()]
    }

    pub fn value_slice(&self) -> &[u8] {
        debug_assert!(self.is_complete());
        &self.body[size_of::<RequestHeader>() + self.header().get_extras_len() + self.header().get_key_len()..]
    }

    pub fn is_complete(&self) -> bool {
        if (self.bytes_read as usize) < size_of::<RequestHeader>() {
            false
        } else {
            (self.bytes_read as usize) == self.header().get_total_len()
        }
    }

    pub fn is_too_large(&self) -> bool {
        if (self.bytes_read as usize) <= size_of::<RequestHeader>() {
            false
        } else {
            self.header().get_key_len() > 256 || self.header().get_total_body_len() > 256 * 1024
        }
    }

    pub fn opcode(&self) -> OpCode {
        unsafe { mem::transmute(self.header().opcode) }
    }
}

impl MutBuf for RequestBuffer {
    fn remaining(&self) -> usize {
        self.body[self.bytes_read as usize..].len()
    }

    fn advance(&mut self, cnt: usize) {
        if (self.bytes_read as usize) < size_of::<RequestHeader>() &&
                (self.bytes_read as usize) + cnt >= size_of::<RequestHeader>() {
            let total_len = self.header().get_total_len();
            if self.body.capacity() < total_len {
                self.body.reserve_exact(total_len);
            }
            unsafe { self.body.set_len(total_len) };
        }
        self.bytes_read += cnt as u32;
        if self.is_complete() {
            self.parse_args();
        }
    }

    fn mut_bytes(&mut self) -> &mut [u8] {
        &mut self.body[self.bytes_read as usize..]
    }
}

impl ResponseBuffer {
    pub fn new(opcode: OpCode, status: Status) -> ResponseBuffer {
        let mut response = ResponseBuffer {
            body: Vec::with_capacity(24),
            bytes_written: 0,
            message: None
        };
        unsafe { response.body.set_len(24) };
        *response.header_mut() = unsafe { mem::zeroed() };
        response.header_mut().magic = RESPONSE_MAGIC;
        response.header_mut().opcode = opcode as u8;
        response.header_mut().status = (status as u16).to_be();
        response
    }

    pub fn new_set_response() -> ResponseBuffer {
        let mut response = ResponseBuffer {
            body: Vec::with_capacity(24),
            bytes_written: 0,
            message: None
        };
        unsafe { response.body.set_len(24) };
        *response.header_mut() = unsafe { mem::zeroed() };
        response.header_mut().magic = RESPONSE_MAGIC;
        response.header_mut().opcode = OpCode::Set as u8;
        response
    }

    pub fn new_get_response(request: &RequestBuffer, message: Message) -> ResponseBuffer {
        let mut response = ResponseBuffer {
            body: Vec::with_capacity(4096),
            bytes_written: 0,
            message: None
        };
        unsafe { response.body.set_len(24) };
        *response.header_mut() = unsafe { mem::zeroed() };
        response.header_mut().magic = RESPONSE_MAGIC;
        response.header_mut().opcode = request.header().opcode;
        response.header_mut().cas = message.id().to_be();
        let key: &[u8] = if request.opcode().include_key() {
            request.key_slice()
        } else {
            b""
        };
        let total_body_len = 4 + key.len() + message.body().len();
        response.header_mut().set_extras_len(4);
        response.header_mut().set_key_len(key.len());
        response.header_mut().set_total_body_len(total_body_len);
        response.body.reserve(total_body_len);
        response.body.write_all(b"\0\0\0\0").unwrap();
        response.body.write_all(key).unwrap();
        response.body.write_all(message.body()).unwrap();
        response
    }

    pub fn new_get_response_fd(request: &RequestBuffer, message: Message) -> ResponseBuffer {
        let mut response = ResponseBuffer {
            body: Vec::with_capacity(128),
            bytes_written: 0,
            message: None,
        };
        unsafe { response.body.set_len(24) };
        *response.header_mut() = unsafe { mem::zeroed() };
        response.header_mut().magic = RESPONSE_MAGIC;
        response.header_mut().opcode = request.header().opcode;
        response.header_mut().cas = message.id().to_be();
        let key: &[u8] = if request.opcode().include_key() {
            request.key_slice()
        } else {
            b""
        };
        let internal_body_len = 4 + key.len();
        response.header_mut().set_extras_len(4);
        response.header_mut().set_key_len(key.len());
        response.header_mut().set_total_body_len(internal_body_len + message.body().len());
        response.body.reserve(internal_body_len);
        response.body.write_all(b"\0\0\0\0").unwrap();
        response.body.write_all(key).unwrap();
        response.message = Some(message);
        response
    }


    #[inline(always)]
    fn header(&self) -> &ResponseHeader {
        unsafe { mem::transmute(self.body.as_ptr()) }
    }

    #[inline(always)]
    fn header_mut(&self) -> &mut ResponseHeader {
        unsafe { mem::transmute(self.body.as_ptr()) }
    }

    pub fn clear(&mut self) {
        self.body.clear();
        self.bytes_written = 0;
    }

    pub fn is_complete(&self) -> bool {
        self.bytes_written == self.header().get_total_len()
    }

    pub fn opcode(&self) -> OpCode {
        unsafe { mem::transmute(self.header().opcode) }
    }
}

impl Buf for ResponseBuffer {
    
    fn remaining(&self) -> usize {
        self.header().get_total_len() - self.bytes_written
    }

    fn bytes(&self) -> &[u8] {
        &self.body[self.bytes_written..]
    }

    fn advance(&mut self, cnt: usize) {
        self.bytes_written += cnt;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mio::{Buf, MutBuf};
    use std::io::Write;
    use std::slice;
    use std::mem::{self, size_of};

    #[test]
    fn test_fill_clear_loop() {
        let req_header_sample = RequestHeader {
            magic: 0,
            opcode: 0,
            key_len: 2u16.to_be(),
            extras_len: 2u8.to_be(),
            data_type: 0,
            vbucket_id: 0,
            total_body_len: 10u32.to_be(),
            opaque: 0,
            cas: 0
        };

        let mut buf = RequestBuffer::new();
        for _ in (0..3) {
            assert!(!buf.is_complete());
            assert!(buf.remaining() > 0);
            buf.mut_bytes().write_all(unsafe {
                slice::from_raw_parts(
                    mem::transmute(&req_header_sample), size_of::<RequestHeader>())
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
        let req_header_sample = RequestHeader {
            magic: 0,
            opcode: 0,
            key_len: 2u16.to_be(),
            extras_len: 2u8.to_be(),
            data_type: 0,
            vbucket_id: 0,
            total_body_len: 10u32.to_be(),
            opaque: 0,
            cas: 0
        };

        let mut buf = RequestBuffer::new();
        let sample_ptr = &req_header_sample as *const RequestHeader as *const u8;
        buf.mut_bytes().write_all(unsafe {
            slice::from_raw_parts(sample_ptr, size_of::<RequestHeader>() - 12)
        }).unwrap();
        buf.advance(size_of::<RequestHeader>() - 12);
        assert!(buf.remaining() >= 12);

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
