use std::str::{self, FromStr};
use std::net::{SocketAddr, lookup_host, SocketAddrV4};
use std::io::{Read, Write};
use std::mem;
use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use mio::tcp::{TcpStream, TcpListener};
use mio::util::{Slab};
use mio::{Buf, MutBuf, Token, EventLoop, Interest, PollOpt, ReadHint, Timeout, Handler, Sender};
use threadpool::ThreadPool;
use num_cpus::get as get_num_cpus;
use serde::json;

use queue::*;
use queue_backend::Message;
use config::*;
use protocol::*;

const SERVER: Token = Token(0);
const FIRST_CLIENT: Token = Token(1);

#[derive(Debug)]
pub enum NotifyMessage {
    Response(ResponseBuffer),
    Available(String),
}

struct Connection {
   	stream: TcpStream,
   	token: Token,
   	request_buffer: RequestBuffer,
   	response_buffer: Option<ResponseBuffer>,
   	interest: Interest
}

struct ServerBackend {
    config: ServerConfig,
    queues: HashMap<String, ArcQueue>,
    // waiting_queue: HashMap<String, Vec<Token>>,
    // waiting_token: HashMap<Token, Vec<String>>,
}

pub struct Server {
	listener: TcpListener,
	state: Arc<RwLock<ServerBackend>>,
    thread_pool: ThreadPool
}

pub struct ServerHandler {
    server: Server,
    connections: Slab<Connection>,
}

struct Dispatcher {
	token: Token,
	request: RequestBuffer,
	state: Arc<RwLock<ServerBackend>>,
	sender: Sender<(Token, NotifyMessage)>
}

impl ServerBackend {
	fn get_queue(&self, name: &str) -> Option<ArcQueue> {
		self.queues.get(name).map(|q| q.clone())
	}

	fn get_or_create_queue(&mut self, name: &str) -> ArcQueue {
		if let Some(queue) = self.queues.get(name) {
			return queue.clone()
		}
		info!("Creating queue {:?}", name);
		let queue = ArcQueue::new(Queue::new(self.config.new_queue_config(name)));
		trace!("done creating queue {:?}", name);
		self.queues.insert(name.into(), queue.clone());
		queue
	}
	
}

impl Dispatcher {
	fn list_queues(&self) -> String {	
		let locked_state = self.state.read().unwrap();
		let queue_names: Vec<_> = locked_state.queues.keys().collect();
	 	json::to_string(&queue_names).unwrap()
	}

	fn split_colon(composed: &str) -> (&str, Option<&str>) {
		if let Some(pos) = composed.find(":") {
			(&composed[..pos], Some(&composed[pos + 1..]))
		} else {
			(composed, None)
		}
	}

	fn get(&self, opcode: OpCode, key_str_slice: &str) -> ResponseBuffer {
		let (queue_name, channel_name_opt) = Self::split_colon(key_str_slice);
		let channel_name = channel_name_opt.unwrap();
		let queue_opt = self.state.read().unwrap().get_queue(queue_name);
		if let Some(queue) = queue_opt {
			if let Some(message) = queue.as_mut().get(channel_name) {
				let key_opt = if opcode.include_key() { Some(key_str_slice.as_bytes()) } else { None };
				ResponseBuffer::new_get_response(key_opt, message.id, message.body)
			} else {
				debug!("queue {:?} channel {:?} has no messages", queue_name, channel_name);
				ResponseBuffer::new(opcode, Status::KeyNotFound)
			}
		} else {
			debug!("queue {:?} not found", queue_name);
			ResponseBuffer::new(opcode, Status::InvalidArguments)
		}
	}

	fn put(&self, opcode: OpCode, key_str_slice: &str, value_slice: &[u8]) -> ResponseBuffer {
		let (queue_name, channel_name_opt) = Self::split_colon(key_str_slice);
		let queue_opt = self.state.read().unwrap().get_queue(queue_name);
		let queue = queue_opt.unwrap_or_else(|| self.state.write().unwrap().get_or_create_queue(queue_name));
		if let Some(channel_name) = channel_name_opt {
			info!("creating queue {:?} channel {:?}", queue_name, channel_name);
			queue.as_mut().create_channel(channel_name);
		} else {
			debug!("inserting into {:?} {:?}", key_str_slice, value_slice);
			let id = queue.as_mut().put(&Message{id:0, body: value_slice}).unwrap();
			trace!("inserted message into {:?} with id {:?}", key_str_slice, id);
		}
		ResponseBuffer::new_set_response()
	}

	fn delete(&self, opcode: OpCode, key_str_slice: &str, value_slice: &[u8]) -> ResponseBuffer {
		let (queue_name, channel_name_opt) = Self::split_colon(key_str_slice);
		let channel_name = channel_name_opt.unwrap();
		let queue_opt = self.state.read().unwrap().get_queue(queue_name);
		let queue = queue_opt.unwrap();
		let id: u64 = str::from_utf8(value_slice).unwrap().parse().unwrap();
		debug!("deleting message {:?} from {:?}", id, key_str_slice);
		queue.as_mut().delete(channel_name, id);
		trace!("deleted message {:?} from {:?}", id, key_str_slice);
		ResponseBuffer::new(opcode, Status::NoError)
	}

	fn purge(&self, opcode: OpCode) -> ResponseBuffer {
		ResponseBuffer::new(opcode, Status::NoError)
	}

	fn dispatch(self) {
		let opcode = self.request.get_opcode();

		if opcode == OpCode::Stat || opcode == OpCode::Exit {
			return
		}

		let key_str_slice = str::from_utf8(self.request.key_slice()).unwrap();
		let value_slice = self.request.value_slice();

		debug!("dispatch {:?} {:?} {:?} {:?}", self.token, opcode, key_str_slice, value_slice);

		let response = match opcode {
			OpCode::Get | OpCode::GetK | OpCode::GetQ | OpCode::GetKQ if value_slice.is_empty() => {
				self.get(opcode, key_str_slice)
			}
			OpCode::Set => {
				self.put(opcode, key_str_slice, value_slice)
			}
			OpCode::Delete if ! value_slice.is_empty() => {
				self.delete(opcode, key_str_slice, value_slice)
			}
			OpCode::NoOp if value_slice.is_empty() => {
				ResponseBuffer::new(opcode, Status::NoError)
			}
			_ => {
				ResponseBuffer::new(opcode, Status::InvalidArguments)
			}
		};
		let composed_msg = (self.token, NotifyMessage::Response(response));
		self.sender.send(composed_msg).unwrap();
	}
}

impl Connection {
	fn new(stream: TcpStream) -> Connection {
		Connection {
			stream: stream,
			token: FIRST_CLIENT,
			request_buffer: RequestBuffer::new(),
			response_buffer: None,
			interest: Interest::all() - Interest::writable()
		}
	}

	fn readable(&mut self, server: &mut Server, event_loop: &mut EventLoop<ServerHandler>, hint: ReadHint) -> bool {
		if hint.is_hup() || hint.is_error() {
			debug!("received hint {:?} for token {:?}", hint,  self.token);
			event_loop.deregister(&self.stream).unwrap();
			return false
		}

		while let Ok(bytes_read) = self.stream.read(self.request_buffer.mut_bytes()) {
			if bytes_read == 0 {
				break
			}
			self.request_buffer.advance(bytes_read);
			trace!("filled request_buffer with {} bytes, remaining: {}", bytes_read, self.request_buffer.remaining());
			
			if self.request_buffer.is_complete() {
				self.interest = Interest::none();
				event_loop.reregister(&self.stream, self.token, self.interest, PollOpt::level()).unwrap();
				
				debug!("dispatching request {:?} for token {:?}", self.request_buffer, self.token);
				let dispatcher = Dispatcher {
					token: self.token,
					request: mem::replace(&mut self.request_buffer, RequestBuffer::new()),
					state: server.state.clone(),
					sender: event_loop.channel(),
				};
				server.thread_pool.execute(move || { dispatcher.dispatch() });
				break
			} else {
				// keep reading
			}
		}

		true
	}

	fn writable(&mut self, server: &mut Server, event_loop: &mut EventLoop<ServerHandler>) -> bool {
		let response_buffer = self.response_buffer.as_mut().expect("writable with None response_buffer");
		while let Ok(bytes_written) = self.stream.write(response_buffer.bytes()) {
			if bytes_written == 0 {
				break
			}
			response_buffer.advance(bytes_written);
			trace!("filled response_buffer with {} bytes, remaining: {}", bytes_written, response_buffer.remaining());

			if response_buffer.is_complete() {
				debug!("done sending response {:?} to token {:?}", response_buffer, self.token);
				self.interest = Interest::all() - Interest::writable();
				event_loop.reregister(&self.stream, self.token, self.interest, PollOpt::level()).unwrap();
				break
			} else {
				// keep writing
			}
		}
		true
	}

	fn notify(&mut self, server: &mut Server, event_loop: &mut EventLoop<ServerHandler>, msg: NotifyMessage) -> bool {
		match msg {
			NotifyMessage::Response(response_buffer) => {
				self.response_buffer = Some(response_buffer);
				self.interest = Interest::all() - Interest::readable();
				event_loop.reregister(&self.stream, self.token, self.interest, PollOpt::level()).unwrap();
			}
			_ => panic!("can't handle msg {:?}", msg)
		}
		true
	}
}

impl Server {
	pub fn new(config: ServerConfig) -> (ServerHandler, EventLoop<ServerHandler>) {
		let addr = SocketAddr::from_str("127.0.0.1:9797").unwrap();

		debug!("binding tcp socket to {:?}", addr);
		let listener = TcpListener::bind(&addr).unwrap();

		let num_cpus = get_num_cpus();
		let num_threads = num_cpus * 3 / 2;
		debug!("detected {} cpus, using {} threads", num_cpus, num_threads);

		let server = Server {
			listener: listener,
			state: Arc::new(RwLock::new(ServerBackend {
				config: config,
				queues: HashMap::new(),
			})),
			thread_pool: ThreadPool::new(num_threads)
		};

		let mut event_loop = EventLoop::new().unwrap();
		event_loop.register_opt(&server.listener, SERVER, Interest::all() - Interest::writable(), PollOpt::edge()).unwrap();

		let server_handler = ServerHandler {
			server: server,
			connections: Slab::new_starting_at(FIRST_CLIENT, 1024)
		};

		(server_handler, event_loop)
	}

	fn accept(&mut self, connections: &mut Slab<Connection>, event_loop: &mut EventLoop<ServerHandler>) {
        let (stream, connection_addr) = self.listener.accept().unwrap();
        debug!("incomming connection from {:?}", connection_addr);

        // Don't buffer output in TCP - kills latency sensitive benchmarks
        {
        	use std::os::unix::io::AsRawFd;
	        use nix::sys::socket;
	        socket::setsockopt(
	        	stream.as_raw_fd(), socket::SockLevel::Tcp, socket::sockopt::TcpNoDelay, &true
	        ).unwrap();
	    }

        // stream.set_tcp_nodelay(false);

        let connection = Connection::new(stream);

        let token = connections.insert(connection).ok().unwrap();

        connections[token].token = token;
        debug!("assigned token {:?} to client {:?}", token, connection_addr);

        event_loop.register_opt(
            &connections[token].stream, token,
            connections[token].interest,
            PollOpt::level()
        ).unwrap();
	}
}

impl Handler for ServerHandler {
	type Timeout = ();
    type Message = (Token, NotifyMessage);

	#[inline]
	fn readable(&mut self, event_loop: &mut EventLoop<Self>, token: Token, hint: ReadHint) {
		trace!("readable event for token {:?} hint {:?}", token, hint);
		match token {
			SERVER => self.server.accept(&mut self.connections, event_loop),
			token => if ! self.connections[token].readable(&mut self.server, event_loop, hint) {
				self.connections.remove(token);
			}
		}
		trace!("done readable event for token {:?}", token);
	}

	#[inline]
	fn writable(&mut self, event_loop: &mut EventLoop<Self>, token: Token) {
		trace!("writable event for token {:?}", token);
		match token {
			SERVER => unreachable!(),
			token => if ! self.connections[token].writable(&mut self.server, event_loop) {
				self.connections.remove(token);
			}
		}
		trace!("done writable event for token {:?}", token);
	}

	fn notify(&mut self, event_loop: &mut EventLoop<Self>, composed_msg: Self::Message) {
		let (token, message) = composed_msg;
		trace!("notify event for token {:?} with {:?}", token, message);
		match token {
			SERVER => unreachable!(),
			token => if ! self.connections[token].notify(&mut self.server, event_loop, message) {
				self.connections.remove(token);
			}
		}
		trace!("end notify event for token {:?}", token);
	}

	fn timeout(&mut self, event_loop: &mut EventLoop<Self>, timeout: Self::Timeout) {
		warn!("timeout {:?}", timeout);
	}

 	fn interrupted(&mut self, event_loop: &mut EventLoop<Self>) {
 		panic!("interrupted");
	}
}
