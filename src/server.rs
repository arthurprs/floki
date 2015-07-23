use std::ops::{Deref, DerefMut};
use std::str::{self, FromStr};
use std::net::{SocketAddr, lookup_host, SocketAddrV4};
use std::io::{Read, Write};
use std::mem;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicUsize, Ordering};
use spin::Mutex as SpinLock;
use std::collections::{HashSet};
use mio::tcp::{TcpStream, TcpListener};
use mio::util::{Slab};
use mio::{Buf, MutBuf, Token, EventLoop, Interest, PollOpt, ReadHint, Timeout, Handler, Sender};
use threadpool::ThreadPool;
use num_cpus::get as get_num_cpus;
use rustc_serialize::json;

use queue::*;
use queue_backend::Message;
use config::*;
use protocol::*;
use utils::*;

const SERVER: Token = Token(0);
const INVALID_CLIENT: Token = Token(1);
const FIRST_CLIENT: Token = Token(2);

const BUSY_WAIT: Token = Token(1000000);

#[derive(Debug)]
struct Connection<'a> {
    stream: TcpStream,
    state: &'a ServerBackend,
    token: Token,
    request: RequestBuffer,
    response: Option<ResponseBuffer>,
    interest: Interest
}

#[derive(Debug)]
struct ServerQueue {
    queue: Arc<Queue>,
    waiting_clients: Vec<Token>
}

#[derive(Debug)]
struct ServerBackend {
    config: ServerConfig,
    queues: HashMap<String, ServerQueue>,
}

#[derive(Debug)]
pub enum NotifyMessage {
    Response(ResponseBuffer),
    Available(Arc<ServerQueue>),
}

pub struct Server {
    listener: TcpListener,
    backend: ServerBackend,
    thread_pool: ThreadPool
}

pub struct ServerHandler<'a> {
    server: Server,
    connections: Slab<Connection<'a>>,
}

type ResponseResult = Result<ResponseBuffer, Status>;

impl ServerBackend {
    #[inline]
    fn get_queue(&self, name: &str) -> Option<&ServerQueue> {
        self.queues.get(name)
    }

    fn delete_queue(&mut self, name: &str) {
        if let Some(q) = self.queues.remove(name) {
            q.queue.as_mut().set_state(QueueState::Deleting);
            // TODO: connections on wait_list should return errors
        }
    }

    fn get_or_create_queue(&mut self, name: &str) -> &ServerQueue {
        self.queues.entry(name.into()).or_insert_with(|| {

            info!("Creating queue {:?}", name);
            let inner_queue = Queue::new(self.config.new_queue_config(name), true);
            trace!("done creating queue {:?}", name);

            ServerQueue {
                queue: Arc::new(inner_queue),
                waiting_clients: Default::default(),
            }
        })
    }
}

impl<'a> Connection<'a> {
    fn list_queues(&self) -> String {   
        let queue_names: Vec<_> = self.state.queues.keys().collect();
        json::encode(&queue_names).unwrap()
    }

    fn split_colon(composed: &str) -> (&str, Option<&str>) {
        if let Some(pos) = composed.find(":") {
            (&composed[..pos], Some(&composed[pos + 1..]))
        } else {
            (composed, None)
        }
    }

    fn get(&self, opcode: OpCode, key_str_slice: &str) -> ResponseResult {
        let (queue_name, channel_name_opt) = Self::split_colon(key_str_slice);
        let channel_name = channel_name_opt.unwrap();
        let sq_opt = self.state.get_queue(queue_name);
        if let Some(sq) = sq_opt {
            if let Some(message) = sq.queue.as_mut().get(channel_name) {
                Ok(ResponseBuffer::new_get_response(&self.request, message.id, message.body))
            } else {
                debug!("queue {:?} channel {:?} has no messages", queue_name, channel_name);
                Err(Status::KeyNotFound)
            }
        } else {
            debug!("queue {:?} not found", queue_name);
            Err(Status::InvalidArguments)
        }
    }

    fn put(&self, opcode: OpCode, key_str_slice: &str, value_slice: &[u8]) -> ResponseResult {
        let (queue_name, channel_name_opt) = Self::split_colon(key_str_slice);
        let sq_opt = self.state.get_queue(queue_name);
        let sq = sq_opt.unwrap_or_else(|| {
            self.state.write().unwrap().get_or_create_queue(queue_name)
        });

        if let Some(channel_name) = channel_name_opt {
            info!("creating queue {:?} channel {:?}", queue_name, channel_name);
            sq.queue.as_mut().create_channel(channel_name);
        } else {
            debug!("inserting into {:?} {:?}", key_str_slice, value_slice);
            let id = sq.queue.as_mut().put(&Message{id:0, body: value_slice}).unwrap();
            trace!("inserted message into {:?} with id {:?}", key_str_slice, id);
        }
        Ok(ResponseBuffer::new_set_response())
    }

    fn delete(&self, opcode: OpCode, key_str_slice: &str) -> ResponseResult {
        let (queue_name, _opt) = Self::split_colon(key_str_slice);
        let sq_opt = self.state.get_queue(queue_name);
        let sq = if let Some(queue) = sq_opt {
            queue
        } else {
            return Err(Status::KeyNotFound)
        };

        let (command_name, id_str_opt) = Self::split_colon(_opt.unwrap());
        if command_name.starts_with('_') {
            match command_name {
                "_purge" => {
                    sq.queue.as_mut().purge();
                },
                "_delete" => {
                    self.state.write().unwrap().delete_queue(queue_name);
                },
                _ => return Err(Status::InvalidArguments)
            }
        } else if let Some(id_str) = id_str_opt {
            let channel_name = command_name;
            let id = if let Ok(id) = id_str_opt.unwrap().parse() {
                id
            } else {
                return Err(Status::InvalidArguments)
            };
            debug!("deleting message {:?} from {:?}", id, command_name);
            if sq.queue.as_mut().delete(channel_name, id).is_none() {
                return Err(Status::KeyNotFound)
            }
        } else {
            debug!("deleting channel {:?}", command_name);
            if ! sq.queue.as_mut().delete_channel(command_name) {
                return Err(Status::KeyNotFound)
            }
        }
        Ok(ResponseBuffer::new(opcode, Status::NoError))
    }

    fn dispatch(&mut self) {
        let opcode = self.request.opcode();

        let key_str_slice = str::from_utf8(self.request.key_slice()).unwrap();
        let value_slice = self.request.value_slice();

        debug!("dispatch {:?} {:?} {:?} {:?}", self.token, opcode, key_str_slice, value_slice);

        let response_result = match opcode {
            OpCode::Get | OpCode::GetK | OpCode::GetQ | OpCode::GetKQ
            if value_slice.is_empty() && !key_str_slice.is_empty() => {
                self.get(opcode, key_str_slice)
            }
            OpCode::Set if !key_str_slice.is_empty() => {
                self.put(opcode, key_str_slice, value_slice)
            }
            OpCode::Delete if !key_str_slice.is_empty() && value_slice.is_empty() => {
                self.delete(opcode, key_str_slice)
            }
            OpCode::NoOp if key_str_slice.is_empty() && value_slice.is_empty() => {
                Ok(ResponseBuffer::new(opcode, Status::NoError))
            }
            _ => Err(Status::InvalidArguments)
        };

        let response = match response_result {
            Ok(response) => response,
            Err(status) => ResponseBuffer::new(opcode, status)
        };
        let composed_msg = (self.token, NotifyMessage::Response(response));
        // self.sender.send(composed_msg).unwrap();
    }

    fn new(stream: TcpStream, state: &'a ServerBackend) -> Connection<'a> {
        Connection {
            stream: stream,
            state: state,
            token: INVALID_CLIENT,
            request: RequestBuffer::new(),
            response: None,
            interest: Interest::all() - Interest::writable()
        }
    }

    fn readable(&mut self, server: &mut Server, event_loop: &mut EventLoop<ServerHandler>, hint: ReadHint) -> bool {
        if hint.is_hup() || hint.is_error() {
            debug!("received hint {:?} for token {:?}", hint,  self.token);
            event_loop.deregister(&self.stream).unwrap();
            return false
        }

        while let Ok(bytes_read) = self.stream.read(self.request.mut_bytes()) {
            if bytes_read == 0 {
                break
            }
            self.request.advance(bytes_read);
            trace!("filled request with {} bytes, remaining: {}", bytes_read, self.request.remaining());
            
            if self.request.is_complete() {
                self.interest = Interest::none();
                event_loop.reregister(&self.stream, self.token, self.interest, PollOpt::level()).unwrap();
                
                debug!("dispatching request {:?} for token {:?}", self.request, self.token);
                self.dispatch();
                break
            } else {
                // keep reading
            }
        }

        true
    }

    fn writable(&mut self, server: &mut Server, event_loop: &mut EventLoop<ServerHandler>) -> bool {
        let response = self.response.as_mut().expect("writable with None response");
        while let Ok(bytes_written) = self.stream.write(response.bytes()) {
            if bytes_written == 0 {
                break
            }
            response.advance(bytes_written);
            trace!("filled response with {} bytes, remaining: {}", bytes_written, response.remaining());

            if response.is_complete() {
                debug!("done sending response {:?} to token {:?}", response, self.token);
                self.interest = Interest::all() - Interest::writable();
                event_loop.reregister(&self.stream, self.token, self.interest, PollOpt::level()).unwrap();
                if response.opcode() == OpCode::Exit {
                    return false;
                }
                break
            } else {
                // keep writing
            }
        }
        true
    }

    fn notify(&mut self, server: &mut Server, event_loop: &mut EventLoop<ServerHandler>, msg: NotifyMessage) -> bool {
        match msg {
            NotifyMessage::Response(response) => {
                self.response = Some(response);
                self.interest = Interest::all() - Interest::readable();
                event_loop.reregister(&self.stream, self.token, self.interest, PollOpt::level()).unwrap();
            }
            _ => panic!("can't handle msg {:?}", msg)
        }
        true
    }
}

impl Server {
    pub fn new<'a>(config: ServerConfig) -> (ServerHandler<'a>, EventLoop<ServerHandler<'a>>) {
        let addr = SocketAddr::from_str("127.0.0.1:9797").unwrap();

        debug!("binding tcp socket to {:?}", addr);
        let listener = TcpListener::bind(&addr).unwrap();

        let num_cpus = get_num_cpus();
        let num_threads = num_cpus + 2;
        debug!("detected {} cpus, using {} threads", num_cpus, num_threads);

        let server = Server {
            listener: listener,
            backend: ServerBackend {
                config: config,
                queues: Default::default(),
            },
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

     fn accept<'a,'b,'d,'e>( &'a mut self, connections: &'b mut Slab<Connection<'a>>, event_loop: &'d mut EventLoop<ServerHandler<'e>>) -> bool {
        let (stream, connection_addr) = self.listener.accept().unwrap();
        debug!("incomming connection from {:?}", connection_addr);

        // Don't buffer output in TCP - kills latency sensitive benchmarks
        // TODO: use TCP_CORK
        stream.set_nodelay(false).unwrap();

        let connection = Connection::new(stream, &self.backend);

        let token = connections.insert(connection).ok().unwrap();

        connections[token].token = token;
        debug!("assigned token {:?} to client {:?}", token, connection_addr);

        event_loop.register_opt(
            &connections[token].stream, token,
            connections[token].interest,
            PollOpt::level()
        ).unwrap();
        true
    }
}

impl<'a> ServerHandler<'a> {
    #[inline]
    fn call_accept(&'a mut self, event_loop: &'a mut EventLoop<Self>) -> bool {
        self.server.accept(&mut self.connections, event_loop)
    }
}

impl<'a> Handler for ServerHandler<'a> {
    type Timeout = ();
    type Message = (Token, NotifyMessage);

    #[inline]
    fn readable(&mut self, event_loop: &mut EventLoop<Self>, token: Token, hint: ReadHint) {
        trace!("readable event for token {:?} hint {:?}", token, hint);
        let is_ok = match token {
            SERVER => self.call_accept(event_loop),
            token => if let Some(connection) = self.connections.get_mut(token) {
                connection.readable(&mut self.server, event_loop, hint)
            } else {
                trace!("token {:?} not found", token);
                false
            }
        };
        if !is_ok {
            trace!("deregistering token {:?}", token);
            self.connections.remove(token);
        }

        trace!("done readable event for token {:?}", token);
    }

    #[inline]
    fn writable(&mut self, event_loop: &mut EventLoop<Self>, token: Token) {
        trace!("writable event for token {:?}", token);
        let is_ok = match token {
            SERVER => unreachable!(),
            token => if let Some(connection) = self.connections.get_mut(token) {
                connection.writable(&mut self.server, event_loop)
            } else {
                trace!("token {:?} not found", token);
                false
            }
        };
        if !is_ok {
            trace!("deregistering token {:?}", token);
            self.connections.remove(token);
        }
        trace!("done writable event for token {:?}", token);
    }

    fn notify(&mut self, event_loop: &mut EventLoop<Self>, composed_msg: Self::Message) {
        let (token, message) = composed_msg;
        trace!("notify event for token {:?} with {:?}", token, message);
        let is_ok = match token {
            SERVER => unreachable!(),
            token => if let Some(connection) = self.connections.get_mut(token) {
                connection.notify(&mut self.server, event_loop, message)
            } else {
                trace!("token {:?} not found", token);
                false
            }
        };
        if !is_ok {
            trace!("deregistering token {:?}", token);
            self.connections.remove(token);
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
