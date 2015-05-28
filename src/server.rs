use std::net::{SocketAddr, lookup_host, SocketAddrV4};
use std::io::{Error, ErrorKind};
use mio::tcp::{TcpStream, TcpListener};
use mio::util::{Slab};
use mio::{Token, EventLoop, EventLoopConfig, Interest, PollOpt, ReadHint, Timeout, Handler, Sender};
use std::str::FromStr;

use std::collections::HashMap;
use threadpool::ThreadPool;

use num_cpus::get as get_num_cpus;

use queue::*;
use config::*;
use protocol::*;

const SERVER: Token = Token(0);
const FIRST_CLIENT: Token = Token(1);

struct Connection {
   	stream: TcpStream,
   	token: Token,
   	request_buffer: RequestBuffer,
   	interest: Interest
}

struct Server {
	listener: TcpListener,
    config: ServerConfig,
    connections: Slab<Connection>,
    queues: HashMap<String, Queue>,
    thread_pool: ThreadPool
}

impl Connection {
	fn new(stream: TcpStream) -> Connection {
		Connection {
			stream: stream,
			token: FIRST_CLIENT,
			request_buffer: RequestBuffer::new(),
			interest: Interest::readable()
		}
	}


	fn readable(&mut self, server: &mut ServerConfig, event_loop: &mut EventLoop<Server>, hint: ReadHint) {
		
		event_loop.reregister(&self.stream, self.token, self.interest, PollOpt::edge());
	}

	fn writable(&mut self, event_loop: &mut EventLoop<Server>) {
		
	}
}


impl Server {
	fn new(config: ServerConfig) -> (Server, EventLoop<Server>) {
		let addr = SocketAddr::from_str("127.0.0.1:13265").unwrap();
		let listener = TcpListener::bind(&addr).unwrap();

		let num_cpus = get_num_cpus();
		let num_threads = num_cpus * 3 / 2;
		debug!("detected {} cpus, using {} threads", num_cpus, num_threads);

		let server = Server {
			listener: listener,
			config: config,
			connections: Slab::new_starting_at(FIRST_CLIENT, 1024),
			queues: HashMap::new(),
			thread_pool: ThreadPool::new(num_threads)
		};

		let event_loop = EventLoop::new().unwrap();
		
		(server, event_loop)
	}

	fn accept(&mut self, event_loop: &mut EventLoop<Self>) {
		loop {
            let stream = match self.listener.accept().unwrap() {
                (stream, _) => stream,
            };

            let connection = Connection::new(stream);

            let token = self.connections.insert(connection).ok().unwrap();

            self.connections[token].token = token;

            event_loop.register_opt(
                &self.connections[token].stream, token,
                Interest::readable(),
                PollOpt::edge() | PollOpt::oneshot()
            ).unwrap();
        }
	}
}

impl Handler for Server {
	type Timeout = ();
    type Message = ();

	fn readable(&mut self, event_loop: &mut EventLoop<Self>, token: Token, hint: ReadHint) {
		match token {
			SERVER => self.accept(event_loop),
			token => self.connections[token].readable(&mut self.config, event_loop, hint)
		}
	}
	fn writable(&mut self, event_loop: &mut EventLoop<Self>, token: Token) {
		match token {
			SERVER => unreachable!(),
			token => self.connections[token].writable(event_loop)
		}
	}
	fn notify(&mut self, event_loop: &mut EventLoop<Self>, msg: Self::Message) {
		warn!("msg {:?}", msg);
	}
	fn timeout(&mut self, event_loop: &mut EventLoop<Self>, timeout: Self::Timeout) {
		warn!("timeout {:?}", timeout);
	}
 	fn interrupted(&mut self, event_loop: &mut EventLoop<Self>) {
 		panic!("interrupted");
	}
}
