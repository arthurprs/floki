use mio::{Buf, MutBuf};
use std::mem;
use std::io::Write;
use std::cmp;
use std::str;
use std::fmt;
use tendril;
use queue_backend::Message;

pub type ByteTendril = tendril::Tendril<tendril::fmt::Bytes, tendril::Atomic>;
pub type StrTendril = tendril::Tendril<tendril::fmt::UTF8, tendril::Atomic>;

#[derive(Eq, PartialEq, Debug)]
pub enum ProtocolError {
    Incomplete,
    Invalid(&'static str),
}

impl From<&'static str> for ProtocolError {
    fn from(from: &'static str) -> ProtocolError {
        ProtocolError::Invalid(from)
    }
}

pub type ProtocolResult<T> = Result<T, ProtocolError>;

#[derive(Clone)]
pub enum Value {
    Nil,
    Int(i64),
    Data(ByteTendril),
    Array(Vec<Value>),
    Status(StrTendril),
    Error(StrTendril),
    Message(Message),
}

impl Value {
    fn serialize_to(self, f: &mut Vec<u8>) {
        match self {
            Value::Nil =>
                write!(f, "$-1\r\n"),
            Value::Int(v) =>
                write!(f, ":{}\r\n", v),
            Value::Data(v) => {
                write!(f, "${}\r\n", v.len32()).unwrap();
                f.write_all(v.as_ref()).unwrap();
                write!(f, "\r\n")
            }
            Value::Array(a) => {
                write!(f, "*{}\r\n", a.len()).unwrap();
                for v in a {
                    v.serialize_to(f);
                }
                Ok(())
            },
            Value::Status(v) =>
                write!(f, "+{}\r\n", v.as_ref()),
            Value::Error(v) =>
                write!(f, "-{}\r\n", v.as_ref()),
            Value::Message(message) => {
                write!(f, "*2\r\n").unwrap();
                write!(f, ":{}\r\n${}\r\n", message.id(), message.len()).unwrap();
                f.write(message.body()).unwrap();
                write!(f, "\r\n")
            }
        }.unwrap()
    }
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            Value::Nil =>
                write!(f, "Nil"),
            Value::Int(v) =>
                write!(f, "Int({:?})", v),
            Value::Data(ref v) =>
                match str::from_utf8(v) {
                    Ok(s) => write!(f, "Data({:?})", s),
                    Err(_) => write!(f, "Data({:?})", v.as_ref())
                },
            Value::Array(ref b) => {
                try!(write!(f, "Array("));
                try!(f.debug_list().entries(b).finish());
                write!(f, ")")
            },
            Value::Status(ref v) =>
                write!(f, "Status({:?})", v.as_ref()),
            Value::Error(ref v) =>
                write!(f, "Error({:?})", v.as_ref()),
            Value::Message(_) =>
                write!(f, "Message(..)"),
        }
    }
}

#[derive(Debug)]
pub struct ResponseBuffer {
    body: Vec<u8>,
    bytes_written: usize,
}

impl ResponseBuffer {
    pub fn new() -> ResponseBuffer {
        ResponseBuffer {
            body: Vec::new(),
            bytes_written: 0,
        }
    }

    pub fn push_value(&mut self, value: Value) {
        value.serialize_to(&mut self.body);
    }
}

impl Buf for ResponseBuffer {
    fn remaining(&self) -> usize {
        self.body.len() - self.bytes_written
    }

    fn advance(&mut self, cnt: usize) {
        self.bytes_written += cnt;
        // FIXME: poor mans vecdeque
        if self.bytes_written == self.body.len() {
            self.body.clear();
            self.bytes_written = 0;
        }
    }

    fn bytes(&self) -> &[u8] {
        &self.body[self.bytes_written..]
    }
}

#[derive(Debug)]
pub struct RequestBuffer {
    body: ByteTendril,
    bytes_read: u32,
}

impl RequestBuffer {
    pub fn new() -> RequestBuffer {
        RequestBuffer {
            body: ByteTendril::new(),
            bytes_read: 0,
        }
    }

    pub fn pop_value(&mut self) -> ProtocolResult<Value> {
        let mut parser = Parser::new(self.body.subtendril(0, self.bytes_read));
        let parsed = parser.parse();
        if parsed.is_ok() {
            self.body = parser.body;
            self.bytes_read = self.body.len32();
        }
        parsed
    }
}

const MIN_REQUESTBUFFER_SIZE: u32 = 4 * 1024;
impl MutBuf for RequestBuffer {
    fn remaining(&self) -> usize {
        cmp::max(MIN_REQUESTBUFFER_SIZE, self.body.len32() - self.bytes_read) as usize
    }

    fn advance(&mut self, cnt: usize) {
        self.bytes_read += cnt as u32
    }

    fn mut_bytes(&mut self) -> &mut [u8] {
        let cur_len = self.body.len32();
        if cur_len - self.bytes_read < MIN_REQUESTBUFFER_SIZE {
            self.body.reserve(MIN_REQUESTBUFFER_SIZE);
            let capacity = self.body.capacity();
            unsafe { self.body.set_len(capacity); }
        }
        &mut self.body[self.bytes_read as usize..]
    }
}

/// The internal redis response parser.
struct Parser {
    body: ByteTendril,
}

impl Parser {
    fn new<T: Into<ByteTendril>>(body: T) -> Parser {
        Parser {
            body: body.into()
        }
    }

    /// parses a single value out of the stream.  If there are multiple
    /// values you can call this multiple times.
    fn parse(&mut self) -> ProtocolResult<Value> {
        match try!(self.read_byte()) {
            b'+' => self.parse_status(),
            b':' => self.parse_int(),
            b'$' => self.parse_data(),
            b'*' => self.parse_array(),
            b'-' => self.parse_error(),
            _ => Err("Invalid response when parsing value".into()),
        }
    }

    #[inline]
    fn read_byte(&mut self) -> ProtocolResult<u8> {
        if self.body.len() >= 1 {
            let byte = self.body[0];
            self.body.pop_front(1);
            return Ok(byte)
        } else {
            Err(ProtocolError::Incomplete)
        }
    }

    #[inline]
    fn read(&mut self, bytes: usize) -> ProtocolResult<ByteTendril> {
        if self.body.len() >= bytes {
            let (first_part, second_part) = Self::split_at(self.body.clone(), bytes);
            self.body = second_part;
            return Ok(first_part)
        } else {
            Err(ProtocolError::Incomplete)
        }
    }

    fn split_at(mut tendril: ByteTendril, position: usize) -> (ByteTendril, ByteTendril) {
        let offset = position as u32;
        let len = tendril.len32() - offset;
        let second_part = tendril.subtendril(offset, len);
        tendril.pop_back(len);
        (tendril, second_part)
    }

    fn read_line(&mut self) -> ProtocolResult<ByteTendril> {
        let nl_pos = match self.body.iter().position(|&b| b == b'\r') {
            Some(nl_pos) => nl_pos,
            None => return Err("Missing line separator".into()),
        };
        let (first_part, second_part) = Self::split_at(try!(self.read(nl_pos + 2)), nl_pos);
        if second_part.as_ref() != b"\r\n" {
            return Err("Invalid line separator".into())
        }

        Ok(first_part)
    }

    fn read_string_line(&mut self) -> ProtocolResult<StrTendril> {
        let line = try!(self.read_line());
        match str::from_utf8(line.as_ref()) {
            Ok(_) => Ok(unsafe { mem::transmute(line) }),
            Err(_) => Err("Expected valid string, got garbage".into())
        }
    }

    fn read_int_line(&mut self) -> ProtocolResult<i64> {
        let line = try!(self.read_line());
        let line_str = unsafe { str::from_utf8_unchecked(line.as_ref()) };
        match line_str.parse::<i64>() {
            Err(_) => Err("Expected integer, got garbage".into()),
            Ok(value) => Ok(value)
        }
    }

    fn parse_status(&mut self) -> ProtocolResult<Value> {
        let line = try!(self.read_string_line());
        Ok(Value::Status(line))
    }

    fn parse_int(&mut self) -> ProtocolResult<Value> {
        Ok(Value::Int(try!(self.read_int_line())))
    }

    fn parse_data(&mut self) -> ProtocolResult<Value> {
        let length = try!(self.read_int_line());
        if length < 0 {
            Ok(Value::Nil)
        } else {
            let length = length as usize;
            let (data, line_sep) = Self::split_at(try!(self.read(length + 2)), length);
            if line_sep.as_ref() != b"\r\n" {
                return Err("Invalid line separator".into())
            }
            Ok(Value::Data(data))
        }
    }

    fn parse_array(&mut self) -> ProtocolResult<Value> {
        let length = try!(self.read_int_line());
        if length < 0 {
            Ok(Value::Nil)
        } else {
            let mut rv = Vec::with_capacity(length as usize);
            for _ in 0..length {
                let value = try!(self.parse());
                rv.push(value);
            }
            Ok(Value::Array(rv))
        }
    }

    fn parse_error(&mut self) -> ProtocolResult<Value> {
        let line = try!(self.read_string_line());
        Ok(Value::Error(line))
    }
}

#[cfg(test)]
mod tests {
    use super::{RequestBuffer, ProtocolResult, ProtocolError, Parser, Value};
    use std::io::Write;
    use mio::MutBuf;

    fn parse(slice: &[u8]) -> ProtocolResult<Value> {
        Parser::new(slice).parse()
    }

    macro_rules! assert_eq_repr {
        ($left:expr , $right:expr) => ({
            match (format!("{:?}", &$left), format!("{:?}", &$right)) {
                (left_val, right_val) => {
                    if !(left_val == right_val) {
                        panic!("repr assertion failed: `(debug(left) == debug(right))` \
                               (left: `{:?}`, right: `{:?}`)", left_val, right_val)
                    }
                }
            }
        })
    }

    #[test]
    fn parse_incomplete() {
        let r = parse(b"*2\r\n$3\r\nfoo");
        assert_eq_repr!(r.unwrap_err(), ProtocolError::Incomplete);
    }

    #[test]
    fn parse_error() {
        let r = parse(b"-foo\r\n");
        assert_eq_repr!(r.unwrap(), Value::Error("foo".into()));

        let r = parse(b"-invalid line sep\r\r");
        assert!(if let ProtocolError::Invalid(_) = r.unwrap_err() {true} else {false});
    }

    #[test]
    fn parse_valid_array() {
        let r = parse(b"*2\r\n$3\r\nfoo\r\n$4\r\nbarz\r\n");
        assert!(r.is_ok(), "{:?} not ok", r.unwrap_err());
        assert_eq_repr!(
            r.unwrap(),
            Value::Array(vec![
                Value::Data(b"foo".as_ref().into()),
                Value::Data(b"barz".as_ref().into())
            ])
        );
    }

    #[test]
    fn parser_multiple2() {
        let mut parser = Parser::new(
            b"*2\r\n$3\r\nfoo\r\n$4\r\nbarz\r\n*2\r\n$3\r\nfoo\r\n$4\r\nbarz\r\n".as_ref(),
        );
        for _ in 0..2 {
            let r = parser.parse();
            assert!(r.is_ok(), "{:?} not ok", r.unwrap_err());
            assert_eq_repr!(
                r.unwrap(),
                Value::Array(vec![
                    Value::Data(b"foo".as_ref().into()),
                    Value::Data(b"barz".as_ref().into())
                ])
            );
        }
        let r = parser.parse();
        assert_eq_repr!(r.unwrap_err(), ProtocolError::Incomplete);
    }

    #[test]
    fn request_parse() {
        let mut req = RequestBuffer::new();
        req.mut_bytes().write_all(b"+OK\r").unwrap();
        req.advance(4);
        let r = req.pop_value();
        assert_eq_repr!(r.unwrap_err(), ProtocolError::Incomplete);
        req.mut_bytes().write_all(b"\n:100\r\n").unwrap();
        req.advance(7);
        let r = req.pop_value();
        assert!(r.is_ok(), "{:?} not ok", r.unwrap_err());
        assert_eq_repr!(r.unwrap(), Value::Status("OK".into()));
        let r = req.pop_value();
        assert!(r.is_ok(), "{:?} not ok", r.unwrap_err());
        assert_eq_repr!(r.unwrap(), Value::Int(100));
    }
}