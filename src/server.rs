use std::str::{self, FromStr};
use std::net::{SocketAddr, lookup_host, SocketAddrV4};
use std::io::{Read, Write, Result as IoResult, Error as IoError};
use std::os::unix::io::{AsRawFd, RawFd};
use std::mem;
use std::thread::{self, Thread, JoinHandle};
use std::sync::Arc;
use spin::{Mutex as SpinLock, RwLock as SpinRwLock};
use std::collections::HashSet;
use mio::tcp::{TcpStream, TcpListener};
use mio::util::Slab;
use mio::{Buf, MutBuf, Token, EventLoop, EventSet, PollOpt, Timeout, Handler, Sender};
use threadpool::ThreadPool;
use num_cpus::get as get_num_cpus;
use rustc_serialize::json;
use rand::{self, Rng, XorShiftRng};
use queue::*;
use queue_backend::Message;
use config::*;
use protocol::*;
use utils::*;

const SERVER: Token = Token(0);
const FIRST_CLIENT: Token = Token(1);

#[derive(Debug)]
pub enum NotifyMessage {
    Response{cookie: u64, response: ResponseBuffer},
    WouldBlock{cookie: u64, request: RequestBuffer, timeout: u64},
    Retry{cookie: u64, fail: bool}
}

pub type NotifyType = (Token, NotifyMessage);

#[derive(Debug)]
pub enum TimeoutMessage {}

pub type TimeoutType = (Token);

struct Connection {
    token: Token,
    stream: TcpStream,
    interest: EventSet,
    request: Option<RequestBuffer>,
    response: Option<ResponseBuffer>,
    timeout: Option<Timeout>,
    cookie: u64,
    processing: bool,
    waiting: bool,
    hup: bool,
}

struct Dispatch {
    config: Arc<ServerConfig>,
    token: Token,
    cookie: u64,
    request: RequestBuffer,
    channel: Sender<NotifyType>,
    queues: Arc<SpinRwLock<HashMap<String, Arc<ServerQueue>>>>,
}

#[derive(Debug)]
struct ServerQueue {
    queue: Queue,
    waiting_clients: SpinLock<Vec<(Token, u64)>>
}

pub struct Server {
    config: Arc<ServerConfig>,
    queues: Arc<SpinRwLock<HashMap<String, Arc<ServerQueue>>>>,
    listener: TcpListener,
    thread_pool: ThreadPool,
    rng: XorShiftRng,
}

pub struct ServerHandler {
    server: Server,
    connections: Slab<Connection>,
}

type DispatchResult = Result<Option<ResponseBuffer>, Status>;

impl Dispatch {
    #[inline]
    fn get_queue(&self, name: &str) -> Option<Arc<ServerQueue>> {
        self.queues.read().get(name).map(|sq| sq.clone())
    }

    fn delete_queue(&self, name: &str) -> Option<Arc<ServerQueue>> {
        let result = self.queues.write().remove(name);
        if let Some(ref sq) = result {
            sq.queue.as_mut().delete();
            let mut locked = sq.waiting_clients.lock();
            for (token, cookie) in locked.drain(..) {
                self.channel.send((token, NotifyMessage::Retry{cookie: cookie, fail: true})).unwrap();
            }
        }
        result
    }

    fn get_or_create_queue(&self, name: &str) -> Arc<ServerQueue> {
        if let Some(sq) = self.queues.read().get(name) {
            return sq.clone()
        }

        self.queues.write().entry(name.into()).or_insert_with(|| {
            info!("Creating queue {:?}", name);
            let inner_queue = Queue::new(self.config.new_queue_config(name), true);
            trace!("done creating queue {:?}", name);

            Arc::new(ServerQueue {
                queue: inner_queue,
                waiting_clients: Default::default(),
            })
        }).clone()
    }

    fn get(&self) -> DispatchResult {
        let queue_name = self.request.arg_str(0).unwrap(); 
        let channel_name = self.request.arg_str(1).unwrap(); 
        let sq = if let Some(sq) = self.get_queue(queue_name) {
            sq
        } else {
            debug!("queue {:?} not found", queue_name);
            return Err(Status::KeyNotFound)
        };

        if let Some(message) = sq.queue.as_mut().get(channel_name) {
            if message.len() <= 500 {
                Ok(Some(ResponseBuffer::new_get_response(&self.request, message.id(), message.body())))
            } else {
                let (fd, file_offset) = message.file_info();
                Ok(Some(ResponseBuffer::new_get_response_fd(
                    &self.request, message.id(), fd, file_offset as usize, message.len() as usize)))
            }
        } else {
            debug!("queue {:?} channel {:?} has no messages", queue_name, channel_name);
            sq.waiting_clients.lock().push((self.token, self.cookie));
            Ok(None)
        }
    }

    fn put(&self) -> DispatchResult {
        let queue_name = self.request.arg_str(0).unwrap(); 
        let channel_name_opt = self.request.arg_str(1); 
        let sq = self.get_or_create_queue(queue_name);

        if let Some(channel_name) = channel_name_opt {
            info!("creating queue {:?} channel {:?}", queue_name, channel_name);
            sq.queue.as_mut().create_channel(channel_name);
        } else {
            let value_slice = self.request.value_slice();
            debug!("inserting into {:?} {:?}", queue_name, value_slice);
            let id = sq.queue.as_mut().put(value_slice).unwrap();
            trace!("inserted message into {:?} with id {:?}", queue_name, id);

            let waiting_clients_opt = {
                let mut locked = sq.waiting_clients.lock();
                if locked.is_empty() {
                    None
                } else {
                    Some(mem::replace(&mut *locked, Default::default()))
                }
            };

            if let Some(waiting_clients) = waiting_clients_opt {
                for (token, cookie) in waiting_clients {
                    self.channel.send((token, NotifyMessage::Retry{cookie: cookie, fail: false})).unwrap();
                }
            }
        }

        Ok(Some(ResponseBuffer::new_set_response()))
    }

    fn delete(&self) -> DispatchResult {
        let queue_name = self.request.arg_str(0).unwrap();
        let channel_name_opt = self.request.arg_str(1);
        let sq = if let Some(sq) = self.get_queue(queue_name) {
            sq
        } else {
            debug!("queue {:?} not found", queue_name);
            return Err(Status::KeyNotFound)
        };

        if let (Some(channel_name), Some(id)) = (channel_name_opt, self.request.arg_uint(2)) {
            debug!("deleting message {:?} from {:?} {:?}", id, queue_name, channel_name);
            if sq.queue.as_mut().ack(channel_name, id).is_none() {
                return Err(Status::KeyNotFound)
            }
        }

        match (channel_name_opt, self.request.arg_str(2)) {
            (Some("_purge"), None) => {
                sq.queue.as_mut().purge();
            },
            (Some("_delete"), None) => {
                sq.queue.as_mut().delete();
                self.delete_queue(queue_name);
            },
            (Some(channel), Some("_delete")) => {
                debug!("deleting channel {:?}", channel);
                if ! sq.queue.as_mut().delete_channel(channel) {
                    return Err(Status::KeyNotFound)
                }
            },
            _ => {
                warn!("unknown delete command {:?}", self.request.key_str());
            }
        }

        Err(Status::NoError)
    }

    fn dispatch(&mut self) {
        let opcode = self.request.opcode();

        debug!("dispatch {:?} {:?} {:?} {:?}",
            self.token, opcode, self.request.key_str(), self.request.value_slice());

        let response_result = match opcode {
            OpCode::Get | OpCode::GetK | OpCode::GetQ | OpCode::GetKQ => {
                self.get()
            }
            OpCode::Set => {
                self.put()
            }
            OpCode::Delete => {
                self.delete()
            }
            OpCode::NoOp => {
                Err(Status::NoError)
            }
            _ => Err(Status::InvalidArguments)
        };

        let notification = match response_result {
            Ok(Some(response)) => NotifyMessage::Response{
                cookie: self.cookie,
                response: response
            },
            Ok(None) => NotifyMessage::WouldBlock{
                cookie: self.cookie,
                request: mem::replace(&mut self.request, RequestBuffer::new()),
                timeout: 10000
            },
            Err(status) => NotifyMessage::Response{
                cookie: self.cookie,
                response: ResponseBuffer::new(opcode, status)
            },
        };

        self.channel.send((self.token, notification)).unwrap();
    }
}

fn write_response(stream: &mut TcpStream, response: &mut ResponseBuffer) -> IoResult<usize> {
    let bytes = response.bytes();
    if bytes.is_empty() && response.remaining() != 0 {
        trace!("response.bytes().is_empty() && response.remaining() == {}", response.remaining());
        if let Some((fd, fd_offset)) = response.send_file_opt {
            let r = sendfile(stream.as_raw_fd(), fd, fd_offset, response.remaining());
            trace!("sendfile returned {}", r);
            if r == -1 {
                Err(IoError::last_os_error())
            } else {
                Ok(r as usize)
            }
        } else {
            unreachable!();
        }
    } else {
        stream.write(response.bytes())
    }
}

impl Connection {

    fn new(token: Token, stream: TcpStream, chann: Sender<NotifyType>) -> Connection {
        Connection {
            token: token,
            stream: stream,
            interest: EventSet::all() - EventSet::writable(),
            request: Some(RequestBuffer::new()),
            response: None,
            timeout: None,
            cookie: 0,
            processing: false,
            waiting: false,
            hup: false,
        }
    }

    fn ready(&mut self, server: &mut Server, event_loop: &mut EventLoop<ServerHandler>, events: EventSet) -> bool {
        if events.is_hup() || events.is_error() {
            debug!("received events {:?} for token {:?}", events,  self.token);
            event_loop.deregister(&self.stream).unwrap();
            self.hup = true;
            return self.processing
        }

        if events.is_readable() {
            let is_complete = {
                let mut request = self.request.as_mut().expect("readable with a None request");
                while let Ok(bytes_read) = self.stream.read(request.mut_bytes()) {
                    if bytes_read == 0 {
                        break
                    }
                    request.advance(bytes_read);
                    trace!("filled request with {} bytes, remaining: {}", bytes_read, request.remaining());
                    
                    if request.is_complete() {
                        debug!("done reading request {:?} from token {:?}", request, self.token);
                        break
                    }
                }
                request.is_complete()
            };
            if is_complete {
                assert!(!self.processing);
                assert!(!self.waiting);
                self.processing = true;
                self.interest = EventSet::all() - EventSet::readable() - EventSet::writable();
                event_loop.reregister(&self.stream, self.token, self.interest, PollOpt::level()).unwrap();
                let request = self.request.take().unwrap();
                self.dispatch(request, server, event_loop);
            }
        }

        if events.is_writable() {
            let is_complete = {
                let response = self.response.as_mut().expect("writable with None response");
                while let Ok(bytes_written) = write_response(&mut self.stream, response) {
                    response.advance(bytes_written);
                    trace!("filled response with {} bytes, remaining: {}", bytes_written, response.remaining());

                    if response.is_complete() {
                        debug!("done sending response {:?} to token {:?}", response, self.token);
                        break
                    }
                }
                response.is_complete()
            };
            if is_complete {
                self.interest = EventSet::all() - EventSet::writable();
                self.request = Some(RequestBuffer::new());
                self.response = None;
                event_loop.reregister(&self.stream, self.token, self.interest, PollOpt::level()).unwrap(); 
            }
        }
        true
    }

    fn notify(&mut self, server: &mut Server, event_loop: &mut EventLoop<ServerHandler>, msg: NotifyMessage) -> bool {
        if self.hup {
            return false;
        }

        match msg {
            NotifyMessage::WouldBlock{cookie, request, timeout} => {
                if cookie == self.cookie {
                    assert!(!self.waiting);
                    assert!(self.processing);
                    self.processing = false;
                    self.waiting = true;
                    self.request = Some(request);
                    event_loop.timeout_ms((self.token), timeout).unwrap();
                }
            },
            NotifyMessage::Response{cookie, response} => {
                if cookie == self.cookie {
                    assert!(!self.waiting);
                    assert!(self.processing);
                    self.processing = false;
                    self.response = Some(response);
                    self.interest = EventSet::all() - EventSet::readable();
                    event_loop.reregister(&self.stream, self.token, self.interest, PollOpt::level()).unwrap();
                }
            },
            NotifyMessage::Retry{cookie, fail} => {
                if cookie == self.cookie {
                    assert!(self.waiting);
                    assert!(!self.processing);
                    self.waiting = false;
                    if fail {
                        let opcode = self.request.as_ref().unwrap().opcode();
                        self.response = Some(ResponseBuffer::new(opcode, Status::InvalidArguments));
                        self.interest = EventSet::all() - EventSet::readable();
                        event_loop.reregister(&self.stream, self.token, self.interest, PollOpt::level()).unwrap();
                    } else {
                        self.processing = true;
                        let request = self.request.take().unwrap();
                        self.dispatch(request, server, event_loop);
                    }
                }
            },
            // _ => panic!("can't handle msg {:?}", msg)
        }

        true
    }

    fn dispatch(&mut self, request: RequestBuffer, server: &mut Server, event_loop: &mut EventLoop<ServerHandler>) {
        self.cookie = server.rng.gen();
        let mut dispatch = Dispatch {
            config: server.config.clone(),
            channel: event_loop.channel(),
            cookie: self.cookie,
            token: self.token,
            request: request,
            queues: server.queues.clone(),
        };
        server.thread_pool.execute(move || dispatch.dispatch());
    }
}

impl Server {

    pub fn new(config: ServerConfig) -> (ServerHandler, EventLoop<ServerHandler>) {
        let addr = SocketAddr::from_str(&config.bind_address).unwrap();

        debug!("binding tcp socket to {:?}", addr);
        let listener = TcpListener::bind(&addr).unwrap();

        let num_cpus = get_num_cpus();
        let num_threads = num_cpus + 2;
        debug!("detected {} cpus, using {} threads", num_cpus, num_threads);

        let server = Server {
            listener: listener,
            config: Arc::new(config),
            queues: Default::default(),
            thread_pool: ThreadPool::new(num_threads),
            rng: rand::weak_rng(),
        };

        let mut event_loop = EventLoop::new().unwrap();
        event_loop.register_opt(&server.listener, SERVER, EventSet::all() - EventSet::writable(), PollOpt::level()).unwrap();

        let server_handler = ServerHandler {
            server: server,
            connections: Slab::new_starting_at(FIRST_CLIENT, 1024)
        };

        (server_handler, event_loop)
    }

     fn ready(&mut self, connections: &mut Slab<Connection>, event_loop: &mut EventLoop<ServerHandler>, events: EventSet) -> bool {
        assert_eq!(events, EventSet::readable());

        if let Some(stream) = self.listener.accept().unwrap() {
            let connection_addr = stream.peer_addr();
            debug!("incomming connection from {:?}", connection_addr);

            // Don't buffer output in TCP - kills latency sensitive benchmarks
            // TODO: use TCP_CORK
            stream.set_nodelay(true).unwrap();

            let token = connections.insert_with(
                |token| Connection::new(token, stream, event_loop.channel())).unwrap();

            debug!("assigned token {:?} to client {:?}", token, connection_addr);

            event_loop.register_opt(
                &connections[token].stream,
                token,
                connections[token].interest,
                PollOpt::level()
            ).unwrap();
        }
        true
    }

    fn notify(&mut self, connections: &mut Slab<Connection>, event_loop: &mut EventLoop<ServerHandler>, msg: NotifyMessage) -> bool {
        match msg {
            // NotifyMessage::Awake(tokens) => {
            //     for token in tokens {
            //         let connection = &connections[token];
            //         assert!(connection.processing);
            //         assert!(connection.request.is_complete());
            //         assert!(connection.response.is_none());
            //         connection.dispatch_in_pool(self);
            //     }
            // }
            _ => panic!("can't handle msg {:?}", msg)
        }
        true
    }
}

impl Handler for ServerHandler {
    type Timeout = TimeoutType;
    type Message = NotifyType;

    #[inline]
    fn ready(&mut self, event_loop: &mut EventLoop<Self>, token: Token, events: EventSet) {
        trace!("events {:?} for token {:?}", events, token);
        let is_ok = match token {
            SERVER => self.server.ready(&mut self.connections, event_loop, events),
            token => if let Some(connection) = self.connections.get_mut(token) {
                connection.ready(&mut self.server, event_loop, events)
            } else {
                trace!("token {:?} not found", token);
                false
            }
        };
        if !is_ok {
            trace!("deregistering token {:?}", token);
            self.connections.remove(token);
        }

        trace!("done events {:?} for token {:?}", events, token);
    }

    #[inline]
    fn notify(&mut self, event_loop: &mut EventLoop<Self>, composed_msg: Self::Message) {
        let (token, message) = composed_msg;
        trace!("notify event for token {:?} with {:?}", token, message);
        let is_ok = match token {
            SERVER => self.server.notify(&mut self.connections, event_loop, message),
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
        // match timeout {
        //     TimeoutMessage::Awake => (),
        //     TimeoutMessage::Maintenance => ()
        // }
    }

    fn interrupted(&mut self, event_loop: &mut EventLoop<Self>) {
        panic!("interrupted");
    }
}

use libc::{c_void, size_t, off_t, ssize_t};
mod ffi {
    use std::os::unix::io::RawFd;
    use libc::{c_void, size_t, off_t, ssize_t};
    extern {
        pub fn sendfile(out_fd: RawFd, in_fd: RawFd, offset: *mut off_t, count: size_t) -> ssize_t;
    }
}

fn sendfile(out_fd: RawFd, in_fd: RawFd, offset: usize, count: usize) -> isize {
    unsafe {
        let mut offset = offset as off_t;
        ffi::sendfile(out_fd, in_fd, &mut offset, count as size_t) as isize
    }
}
