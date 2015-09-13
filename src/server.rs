use std::str::{self, FromStr};
use std::net::{SocketAddr, lookup_host, SocketAddrV4};
use std::io::{Read, Write, Result as IoResult, Error as IoError};
use std::os::unix::io::{AsRawFd, RawFd};
use std::mem;
use std::thread::{self, Thread, JoinHandle};
use std::sync::Arc;
use spin::{Mutex as SpinLock, RwLock as SpinRwLock};
use mio::tcp::{TcpStream, TcpListener};
use mio::util::Slab;
use mio::{Buf, MutBuf, Token, EventLoop, EventSet, PollOpt, Timeout, Handler, Sender};
use threadpool::ThreadPool;
use num_cpus::get as get_num_cpus;
use rustc_serialize::json;
use queue::*;
use queue_backend::Message;
use config::*;
use protocol::*;
use utils::*;

const SERVER: Token = Token(0);
const FIRST_CLIENT: Token = Token(1);

#[derive(Debug)]
pub enum NotifyMessage {
    Response{response: ResponseBuffer},
    PutResponse{response: ResponseBuffer, queue: Arc<Queue>, new_tail: u64},
    GetWouldBlock{request: RequestBuffer, queue: Arc<Queue>, required_tail: u64},
    DeleteQueue{response: ResponseBuffer, queue_name: String},
}

pub type NotifyType = (Cookie, NotifyMessage);

#[derive(Debug)]
pub enum TimeoutMessage {}

pub type TimeoutType = (Token);

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
struct Cookie(u64);

impl Cookie {
    fn new(token: Token, nonce: u64) -> Cookie {
        Cookie((token.0 << 48) as u64 | (nonce & 0xFFFFFFFFFFFF))
    }

    fn token(self) -> Token {
        Token((self.0 >> 48) as usize)
    }

    fn nonce(self) -> u64 {
        self.0 & 0xFFFFFFFFFFFF
    }

    fn next(self) -> Cookie {
        Self::new(self.token(), self.nonce().wrapping_add(1))
    }
}

struct Connection {
    token: Token,
    cookie: Cookie,
    stream: TcpStream,
    interest: EventSet,
    request: Option<RequestBuffer>,
    response: Option<ResponseBuffer>,
    timeout: Option<Timeout>,
    processing: bool,
    waiting: bool,
    hup: bool,
}

struct Dispatch {
    config: Arc<ServerConfig>,
    cookie: Cookie,
    request: RequestBuffer,
    channel: Sender<NotifyType>,
    queues: Arc<SpinRwLock<HashMap<String, Arc<Queue>>>>,
}

pub struct Server {
    config: Arc<ServerConfig>,
    queues: Arc<SpinRwLock<HashMap<String, Arc<Queue>>>>,
    waiting_clients: HashMap<String, (u64, Vec<Cookie>)>,
    awaking_clients: Vec<Cookie>,
    listener: TcpListener,
    thread_pool: ThreadPool,
    nonce: u64,
}

pub struct ServerHandler {
    server: Server,
    connections: Slab<Connection>,
}

impl NotifyMessage {
    fn from_status(request: &RequestBuffer, status: Status) -> NotifyMessage {
        NotifyMessage::Response{response: ResponseBuffer::new(request.opcode(), status)}
    }

    fn from_message(request: &RequestBuffer, message: Message) -> NotifyMessage {
        if message.len() <= 500 {
            NotifyMessage::Response{
                response: ResponseBuffer::new_get_response(request, message.id(), message.body())
            }
        } else {
            let (fd, file_offset) = message.file_info();
            NotifyMessage::Response{
                response: ResponseBuffer::new_get_response_fd(
                    request, message.id(), fd, file_offset as usize, message.len() as usize)
            }
        }
    }
}

impl Dispatch {
    #[inline]
    fn get_queue(&self, name: &str) -> Option<Arc<Queue>> {
        self.queues.read().get(name).map(|sq| sq.clone())
    }

    fn delete_queue(&self, name: &str) -> Option<Arc<Queue>> {
        let result = self.queues.write().remove(name);
        if let Some(ref q) = result {
            q.as_mut().delete();
        }
        result
    }

    fn get_or_create_queue(&self, name: &str) -> Arc<Queue> {
        if let Some(sq) = self.queues.read().get(name) {
            return sq.clone()
        }

        self.queues.write().entry(name.into()).or_insert_with(|| {
            info!("Creating queue {:?}", name);
            let inner_queue = Queue::new(self.config.new_queue_config(name), true);
            trace!("done creating queue {:?}", name);

            Arc::new(inner_queue)
        }).clone()
    }

    #[allow(mutable_transmutes)]
    fn get(&mut self) -> NotifyMessage {
        let queue_name = self.request.arg_str(0).unwrap(); 
        let channel_name = self.request.arg_str(1).unwrap(); 
        let q = if let Some(q) = self.get_queue(queue_name) {
            q
        } else {
            debug!("queue {:?} not found", queue_name);
            return NotifyMessage::from_status(&self.request, Status::KeyNotFound)
        };

        match q.as_mut().get(channel_name) {
            Some(Ok(message)) => {
                NotifyMessage::from_message(&self.request, message)
            },
            Some(Err(required_tail)) => {
                debug!("queue {:?} channel {:?} has no messages", queue_name, channel_name);
                NotifyMessage::GetWouldBlock{
                    request: mem::replace(unsafe{mem::transmute(&self.request)}, RequestBuffer::new()),
                    queue: q,
                    required_tail: required_tail,
                }
            },
            _ => NotifyMessage::from_status(&self.request, Status::KeyNotFound)
        }
    }

    fn put(&mut self) -> NotifyMessage {
        let queue_name = self.request.arg_str(0).unwrap(); 
        let channel_name_opt = self.request.arg_str(1); 
        let q = self.get_or_create_queue(queue_name);

        if let Some(channel_name) = channel_name_opt {
            info!("creating queue {:?} channel {:?}", queue_name, channel_name);
            q.as_mut().create_channel(channel_name);
            NotifyMessage::from_status(&self.request, Status::NoError)
        } else {
            let value_slice = self.request.value_slice();
            debug!("inserting into {:?} {:?}", queue_name, value_slice);
            let id = q.as_mut().put(value_slice).unwrap();
            trace!("inserted message into {:?} with id {:?}", queue_name, id);
            NotifyMessage::PutResponse{
                response: ResponseBuffer::new_set_response(),
                queue: q,
                new_tail: id
            }
        }
    }

    fn delete(&mut self) -> NotifyMessage {
        let queue_name = self.request.arg_str(0).unwrap();
        let channel_name_opt = self.request.arg_str(1);
        let q = if let Some(q) = self.get_queue(queue_name) {
            q
        } else {
            debug!("queue {:?} not found", queue_name);
            return NotifyMessage::from_status(&self.request, Status::KeyNotFound)
        };

        if let (Some(channel_name), Some(id)) = (channel_name_opt, self.request.arg_uint(2)) {
            debug!("deleting message {:?} from {:?} {:?}", id, queue_name, channel_name);
            if q.as_mut().ack(channel_name, id).is_some() {
                return NotifyMessage::from_status(&self.request, Status::NoError)
            }
            return NotifyMessage::from_status(&self.request, Status::KeyNotFound)
        }

        match (channel_name_opt, self.request.arg_str(2)) {
            (Some("_purge"), None) => {
                q.as_mut().purge();
            },
            (Some("_delete"), None) => {
                q.as_mut().delete();
                self.delete_queue(queue_name);
                return NotifyMessage::DeleteQueue{
                    response: ResponseBuffer::new(self.request.opcode(), Status::NoError),
                    queue_name: q.name().into()
                }
            },
            (Some(channel), Some("_delete")) => {
                debug!("deleting channel {:?}", channel);
                if ! q.as_mut().delete_channel(channel) {
                    return NotifyMessage::from_status(&self.request, Status::KeyNotFound)
                }
            },
            _ => {
                warn!("unknown delete command {:?}", self.request.key_str());
            }
        }

        return NotifyMessage::from_status(&self.request, Status::NoError)
    }

    fn dispatch(&mut self) {
        let opcode = self.request.opcode();

        debug!("dispatch {:?} {:?} {:?} {:?}",
            self.cookie.token(), opcode, self.request.key_str(), self.request.value_slice());

        let notification = match opcode {
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
                NotifyMessage::from_status(&self.request, Status::NoError)
            }
            _ => NotifyMessage::from_status(&self.request, Status::InvalidArguments)
        };

        self.channel.send((self.cookie, notification)).unwrap();
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

    fn new(token: Token, nonce: u64, stream: TcpStream) -> Connection {
        Connection {
            token: token,
            cookie: Cookie::new(token, nonce),
            stream: stream,
            interest: EventSet::all() - EventSet::writable(),
            request: Some(RequestBuffer::new()),
            response: None,
            timeout: None,
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

    fn notify(&mut self, server: &mut Server, event_loop: &mut EventLoop<ServerHandler>, notification: NotifyType) -> bool {
        if self.hup {
            return false;
        }

        let (cookie, msg) = notification;

        if cookie != self.cookie {
            return true
        }

        assert!(self.processing);
        assert!(!self.waiting);

        match msg {
            NotifyMessage::GetWouldBlock{request, queue, required_tail} => {
                assert!(self.timeout.is_none());
                self.processing = false;
                self.waiting = true;
                self.request = Some(request);
                self.timeout = Some(event_loop.timeout_ms((self.token), 5000).unwrap());
                server.wait_queue(queue, cookie, required_tail);
            },
            NotifyMessage::PutResponse{response, queue, new_tail} => {
                self.processing = false;
                self.response = Some(response);
                self.interest = EventSet::all() - EventSet::readable();
                server.notify_queue(queue, new_tail);
            },
            NotifyMessage::DeleteQueue{response, queue_name} => {
                self.processing = false;
                self.response = Some(response);
                self.interest = EventSet::all() - EventSet::readable();
                server.notify_queue_deleted(&queue_name);
            },
            NotifyMessage::Response{response} => {
                self.processing = false;
                self.response = Some(response);
                self.interest = EventSet::all() - EventSet::readable();
            },
        }

        event_loop.reregister(&self.stream, self.token, self.interest, PollOpt::level()).unwrap();    

        true
    }

    fn dispatch(&mut self, request: RequestBuffer, server: &mut Server, event_loop: &mut EventLoop<ServerHandler>) {
        server.nonce += 1;
        self.cookie = Cookie::new(self.token, server.nonce);
        let mut dispatch = Dispatch {
            cookie: self.cookie,
            config: server.config.clone(),
            channel: event_loop.channel(),
            request: request,
            queues: server.queues.clone(),
        };
        server.thread_pool.execute(move || dispatch.dispatch());
    }

    fn retry(&mut self, server: &mut Server, event_loop: &mut EventLoop<ServerHandler>) {
        assert!(!self.processing);
        assert!(self.waiting);
        self.processing = true;
        self.waiting = false;
        event_loop.clear_timeout(self.timeout.take().unwrap());
        let request = self.request.take().unwrap();
        self.dispatch(request, server, event_loop);
    }
}

impl Server {

    fn wait_queue(&mut self, queue: Arc<Queue>, cookie: Cookie, required_tail: u64) {
        if let Some(&mut (q_tail, ref mut w_list)) = self.waiting_clients.get_mut(queue.name()) {
            if q_tail < required_tail {
                w_list.push(cookie);
                return
            }
        }
        self.waiting_clients.insert(queue.name().into(), (0, vec![cookie]));
    }

    fn notify_queue(&mut self, queue: Arc<Queue>, new_tail: u64) {
        if let Some(&mut (q_tail, ref mut w_list)) = self.waiting_clients.get_mut(queue.name()) {
            if new_tail >= q_tail {
                self.awaking_clients.extend(w_list.drain(..));
            }
        }
    }

    fn notify_queue_deleted(&mut self, queue_name: &str) {
        if let Some((_, w_list)) = self.waiting_clients.remove(queue_name) {
            self.awaking_clients.extend(w_list);
        }
    }

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
            waiting_clients: Default::default(),
            awaking_clients: Default::default(),
            nonce: 0,
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
                |token| Connection::new(token, 0, stream)).unwrap();

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

    fn tick(&mut self, connections: &mut Slab<Connection>, event_loop: &mut EventLoop<ServerHandler>) {
        if !self.awaking_clients.is_empty() {
            let mut awaking_clients = mem::replace(&mut self.awaking_clients, Vec::new());
            for cookie in awaking_clients.drain(..) {
                connections[cookie.token()].retry(self, event_loop);
            }
            self.awaking_clients = awaking_clients;
        }
    }

    fn notify(&mut self, connections: &mut Slab<Connection>, event_loop: &mut EventLoop<ServerHandler>, notification: NotifyType) -> bool {
        match notification {
            // NotifyMessage::Awake(tokens) => {
            //     for token in tokens {
            //         let connection = &connections[token];
            //         assert!(connection.processing);
            //         assert!(connection.request.is_complete());
            //         assert!(connection.response.is_none());
            //         connection.dispatch_in_pool(self);
            //     }
            // }
            (_, msg) => panic!("can't handle msg {:?}", msg)
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
    fn notify(&mut self, event_loop: &mut EventLoop<Self>, notification: Self::Message) {
        let token = notification.0.token();
        trace!("notify event for token {:?} with {:?}", token, notification.1);
        let is_ok = match token {
            SERVER => self.server.notify(&mut self.connections, event_loop, notification),
            token => if let Some(connection) = self.connections.get_mut(token) {
                connection.notify(&mut self.server, event_loop, notification)
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

    fn tick(&mut self, event_loop: &mut EventLoop<Self>) {
        self.server.tick(&mut self.connections, event_loop);
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
