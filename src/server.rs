use std::str::FromStr;
use std::net::SocketAddr;
use std::io::{Read, Write, Result as IoResult, Error as IoError};
use std::os::unix::io::{AsRawFd, RawFd};
use std::mem;
use std::cmp;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use spin::RwLock as SpinRwLock;
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
use atom::Atom;
use cookie::Cookie;
use time;

const SERVER: Token = Token(0);
const FIRST_CLIENT: Token = Token(1);

#[derive(Debug)]
pub enum NotifyMessage {
    Response{response: ResponseBuffer},
    PutResponse{response: ResponseBuffer, queue: Atom, head: u64},
    GetWouldBlock{request: RequestBuffer, queue: Atom, channel: Atom, required_head: u64},
    ChannelCreate{queue: Atom, channel: Atom},
    ChannelDelete{queue: Atom, channel: Atom},
    QueueCreate{queue: Atom},
    QueueDelete{queue: Atom},
    MaintenanceDone,
}

pub type NotifyType = (Cookie, NotifyMessage);

#[derive(Debug)]
pub enum TimeoutMessage {
    Timeout{queue: Atom, channel: Atom},
    Maintenance,
    WallClock,
}

pub type TimeoutType = (Token, TimeoutMessage);

struct Connection {
    token: Token,
    cookie: Cookie,
    stream: TcpStream,
    interest: EventSet,
    request_clock_ms: u64,
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
    meta_lock: Arc<Mutex<()>>,
    queues: Arc<SpinRwLock<HashMap<Atom, Arc<Queue>>>>,
    clock: u32,
}

#[derive(Default)]
struct WaitingClients {
    head: u64,
    channels: HashMap<Atom, VecDeque<Cookie>>,
}

pub struct Server {
    config: Arc<ServerConfig>,
    meta_lock: Arc<Mutex<()>>,
    queues: Arc<SpinRwLock<HashMap<Atom, Arc<Queue>>>>,
    waiting_clients: HashMap<Atom, WaitingClients>,
    awaking_clients: Vec<Cookie>,
    listener: TcpListener,
    thread_pool: ThreadPool,
    maintenance_timeout: Timeout,
    nonce: u64,
    clock_ms: u64,
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
        NotifyMessage::Response {
            response: if message.len() >= 1024 {
                ResponseBuffer::new_get_response_fd(request, message)
            } else {
                ResponseBuffer::new_get_response(request, message)
            }
        }
    }
}

impl Dispatch {
    #[inline]
    fn get_queue(&self, name: &str) -> Option<Arc<Queue>> {
        self.queues.read().get(name).map(|sq| sq.clone())
    }

    fn delete_queue(&self, q: Arc<Queue>) {
        debug!("deleting queue {:?}", q.name());
        let meta_lock = self.meta_lock.lock().unwrap();
        let q = self.queues.write().remove(q.name()).unwrap();
        self.notify_server(NotifyMessage::QueueDelete{
            queue: q.name().into(),
        });
        q.as_mut().delete();
    }

    fn delete_channel(&self, q: &Queue, channel_name: &str) -> bool {
        debug!("deleting queue {:?} channel {:?}", q.name(), channel_name);
        let meta_lock = self.meta_lock.lock().unwrap();
        if q.as_mut().delete_channel(channel_name) {
            self.notify_server(NotifyMessage::ChannelDelete{
                queue: q.name().into(),
                channel: channel_name.into(),
            });
            true
        } else {
            false
        }
    }

    fn create_channel(&self, q: &Queue, channel_name: &str) -> bool {
        info!("creating queue {:?} channel {:?}", q.name(), channel_name);
        let meta_lock = self.meta_lock.lock().unwrap();
        if q.as_mut().create_channel(channel_name, self.clock) {
            self.notify_server(NotifyMessage::ChannelCreate{
                queue: q.name().into(),
                channel: channel_name.into(),
            });
            true
        } else {
            false
        }
    }

    fn get_or_create_queue(&self, name: &str) -> Arc<Queue> {
        if let Some(sq) = self.queues.read().get(name) {
            return sq.clone()
        }

        let meta_lock = self.meta_lock.lock().unwrap();
        self.queues.write().entry(name.into()).or_insert_with(|| {
            info!("Creating queue {:?}", name);
            let inner_queue = Queue::new(self.config.new_queue_config(name), false);
            debug!("Done creating queue {:?}", name);

            self.notify_server(NotifyMessage::QueueCreate{
                queue: inner_queue.name().into(),
            });

            Arc::new(inner_queue)
        }).clone()
    }

    fn notify_server(&self, notification: NotifyMessage) {
        self.channel.send((Cookie::new(SERVER, 0), notification)).unwrap();
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

        match q.as_mut().get(channel_name, self.clock) {
            Some(Ok(message)) => {
                NotifyMessage::from_message(&self.request, message)
            },
            Some(Err(required_head)) => {
                debug!("queue {:?} channel {:?} has no messages", queue_name, channel_name);
                NotifyMessage::GetWouldBlock{
                    request: mem::replace(unsafe{mem::transmute(&self.request)}, RequestBuffer::new()),
                    queue: q.name().into(),
                    channel: channel_name.into(),
                    required_head: required_head,
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
            if self.create_channel(&q, channel_name) {
                NotifyMessage::from_status(&self.request, Status::NoError)
            } else {
                NotifyMessage::from_status(&self.request, Status::KeyNotFound)
            }
        } else {
            let value_slice = self.request.value_slice();
            debug!("inserting into {:?} [{:?}]", queue_name, value_slice.len());
            let id = q.as_mut().push(value_slice, self.clock).unwrap();
            trace!("inserted message into {:?} with id {:?}", queue_name, id);
            NotifyMessage::PutResponse{
                response: ResponseBuffer::new_set_response(),
                queue: q.name().into(),
                head: id + 1
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
            if q.as_mut().ack(channel_name, id, self.clock).is_some() {
                return NotifyMessage::from_status(&self.request, Status::NoError)
            }
            return NotifyMessage::from_status(&self.request, Status::KeyNotFound)
        }

        match (channel_name_opt, self.request.arg_str(2)) {
            (Some("_purge"), None) => {
                q.as_mut().purge();
            },
            (Some("_delete"), None) => {
                self.delete_queue(q);
            },
            (Some(channel_name), Some("_delete")) => {
                if ! self.delete_channel(&q, channel_name) {
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

        debug!("dispatch {:?} {:?} {:?} [{:?}]",
            self.cookie.token(), opcode, self.request.key_str(), self.request.value_slice().len());

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
            OpCode::NoOp | OpCode::Quit => {
                NotifyMessage::from_status(&self.request, Status::NoError)
            }
            _ => NotifyMessage::from_status(&self.request, Status::InvalidArguments)
        };

        self.channel.send((self.cookie, notification)).unwrap();
    }
}

fn write_response(stream: &mut TcpStream, response: &ResponseBuffer) -> IoResult<usize> {
    let bytes = response.bytes();
    if bytes.is_empty() && response.remaining() != 0 {
        if let Some(ref message) = response.message {
            let r = sendfile(
                stream.as_raw_fd(),
                message.fd(),
                (message.fd_offset() + message.len()) as isize - response.remaining() as isize,
                response.remaining());
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
            request_clock_ms: 0,
            request: Some(RequestBuffer::new()),
            response: None,
            timeout: None,
            processing: false,
            waiting: false,
            hup: false,
        }
    }

    fn send_response(&mut self, response: ResponseBuffer, server: &mut Server, event_loop: &mut EventLoop<ServerHandler>) {
        self.response = Some(response);
        self.interest = EventSet::all() - EventSet::readable();
        event_loop.reregister(&self.stream, self.token, self.interest, PollOpt::level()).unwrap();
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
                        trace!("done reading request from token {:?}", self.token);
                        break
                    }
                }
                request.is_complete()
            };
            if is_complete {
                assert!(!self.processing);
                assert!(!self.waiting);
                assert!(self.timeout.is_none());
                self.request_clock_ms = server.clock_ms;
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
                        trace!("done sending response to token {:?}", self.token);
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
        assert!(self.processing);
        assert!(!self.waiting);
        assert!(self.timeout.is_none());
        self.processing = false;

        let (cookie, msg) = notification;

        if self.hup {
            return false
        }

        // FIXME: this is impossible in the current code
        if cookie != self.cookie {
            return true
        }

        match msg {
            NotifyMessage::GetWouldBlock{request, queue, channel, required_head} => {
                // TODO: this timeout value should come with the message
                let deadline = self.request_clock_ms + 5000;
                if server.clock_ms >= deadline {
                    self.send_response(
                        ResponseBuffer::new(request.opcode(), Status::KeyNotFound),
                        server,
                        event_loop
                    );
                } else {
                    self.waiting = true;
                    self.request = Some(request);
                    self.timeout = Some(event_loop.timeout_ms(
                        (self.token, TimeoutMessage::Timeout{queue: queue.clone(), channel: channel.clone()}),
                        (deadline - server.clock_ms) as u64
                    ).unwrap());
                    server.wait_queue(queue, channel, cookie, required_head);
                }
            },
            NotifyMessage::PutResponse{response, queue, head} => {
                self.send_response(response, server, event_loop);
                server.notify_queue(queue, head);
            },
            NotifyMessage::Response{response} => {
                self.send_response(response, server, event_loop);
            },
            msg => panic!("can't handle msg {:?}", msg)
        }

        true
    }

    fn timeout(&mut self, server: &mut Server, event_loop: &mut EventLoop<ServerHandler>, timeout: TimeoutMessage) -> bool {
        match timeout {
            TimeoutMessage::Timeout{queue, channel} => {
                assert!(self.waiting);
                assert!(!self.processing);
                assert!(self.timeout.is_some());
                self.waiting = false;
                self.timeout = None;
                let request = self.request.take().unwrap();
                server.notify_timeout(queue, channel, self.cookie);
                self.send_response(
                    ResponseBuffer::new(request.opcode(), Status::KeyNotFound),
                    server,
                    event_loop
                );
            },
            to => panic!("can't handle to {:?}", to)
        }
        true
    }

    fn dispatch(&mut self, request: RequestBuffer, server: &mut Server, event_loop: &mut EventLoop<ServerHandler>) {
        server.nonce = server.nonce.wrapping_add(1);
        self.cookie = Cookie::new(self.token, server.nonce);
        // FIXME: this not only allocates but clone 4 ARCs
        let mut dispatch = Dispatch {
            cookie: self.cookie,
            config: server.config.clone(),
            channel: event_loop.channel(),
            request: request,
            meta_lock: server.meta_lock.clone(),
            queues: server.queues.clone(),
            clock: (server.clock_ms / 1000) as u32
        };
        server.thread_pool.execute(move || dispatch.dispatch());
    }

    fn retry(&mut self, server: &mut Server, event_loop: &mut EventLoop<ServerHandler>) {
        assert!(!self.processing);
        assert!(self.waiting);
        self.processing = true;
        self.waiting = false;
        assert!(event_loop.clear_timeout(self.timeout.take().unwrap()));
        let request = self.request.take().unwrap();
        self.dispatch(request, server, event_loop);
    }
}

impl Server {

    pub fn new(config: ServerConfig) -> (ServerHandler, EventLoop<ServerHandler>) {
        let addr = SocketAddr::from_str(&config.bind_address).unwrap();

        debug!("binding tcp socket to {:?}", addr);
        let listener = TcpListener::bind(&addr).unwrap();

        let num_cpus = get_num_cpus();
        let num_threads = cmp::max(6, num_cpus * 2 + 1);
        debug!("detected {} cpus, using {} threads", num_cpus, num_threads);

        let mut event_loop = EventLoop::new().unwrap();
        event_loop.register_opt(&listener, SERVER,
            EventSet::all() - EventSet::writable(), PollOpt::level()).unwrap();

        let maintenance_timeout = event_loop.timeout_ms(
            (SERVER, TimeoutMessage::Maintenance), config.maintenance_timeout as u64).unwrap();
        event_loop.timeout_ms((SERVER, TimeoutMessage::WallClock), 1000).unwrap();

        let mut server = Server {
            listener: listener,
            config: Arc::new(config),
            meta_lock: Arc::new(Mutex::new(())),
            queues: Default::default(),
            thread_pool: ThreadPool::new(num_threads),
            waiting_clients: Default::default(),
            awaking_clients: Default::default(),
            maintenance_timeout: maintenance_timeout,
            nonce: 0,
            clock_ms: Self::get_clock_ms(),
        };

        info!("Opening queues...");
        for queue_config in ServerConfig::read_queue_configs(&server.config) {
            info!("Opening queue {:?}", queue_config.name);

            let queue = Queue::new(queue_config, true);
            // load state
            let info = queue.info();
            let mut waiting_clients = WaitingClients {
                head: info.head,
                channels: Default::default(),
            };
            for channel_name in info.channels.keys() {
                waiting_clients.channels.insert(channel_name.into(), Default::default());
            }
            server.waiting_clients.insert(queue.name().into(), waiting_clients);

            debug!("Done opening queue {:?}", queue.name());
            server.queues.write().insert(queue.name().into(), Arc::new(queue));
        }
        debug!("Opening complete!");

        let server_handler = ServerHandler {
            server: server,
            connections: Slab::new_starting_at(FIRST_CLIENT, 1024)
        };

        (server_handler, event_loop)
    }

    fn wait_queue(&mut self, queue: Atom, channel: Atom, cookie: Cookie, required_head: u64) {
        debug!("wait_queue {:?} {:?} {:?} {:?}", queue, channel, cookie, required_head);
        if let Some(w) = self.waiting_clients.get_mut(&queue) {
            if let Some(cookies)  = w.channels.get_mut(&channel) {
                if required_head > w.head {
                    cookies.push_back(cookie);
                } else {
                    debug!("Race condition, queue {:?} channel {:?} has new data {:?}", queue, channel, w.head);
                    self.awaking_clients.push(cookie);
                }
            } else {
                debug!("Race condition, queue {:?} channel {:?} is gone", queue, channel);
                self.awaking_clients.push(cookie);
            }
        } else {
            debug!("Race condition, queue {:?} is gone", queue);
            self.awaking_clients.push(cookie);
        }
    }

    fn notify_queue(&mut self, queue: Atom, new_head: u64) {
        debug!("notify_queue {:?} {:?}", queue, new_head);
        if let Some(w) = self.waiting_clients.get_mut(&queue) {
            if new_head > w.head {
                w.head = new_head;
                for (_, tokens) in w.channels.iter_mut() {
                    // FIXME: it should be possible to awake only the necessary amout of clients
                    self.awaking_clients.extend(tokens.drain(..));
                }
            } else {
                debug!("Race condition, queue {:?} tail already advanced", queue);
            }
        } else {
            debug!("Race condition, queue {:?} is gone", queue);
        }
    }

    fn notify_timeout(&mut self, queue: Atom, channel: Atom, cookie: Cookie) {
        debug!("notify_timeout {:?} {:?} {:?}", queue, channel, cookie);
        if let Some(w) = self.waiting_clients.get_mut(&queue) {
            if let Some(cookies)  = w.channels.get_mut(&channel) {
                // FIXME: only need to delete the first ocurrence
                cookies.retain(|&c| c != cookie);
            } else {
                unreachable!();
            }
        } else {
            unreachable!();
        }
    }

    fn notify_queue_deleted(&mut self, queue: Atom) {
        debug!("notify_queue_deleted {:?}", queue);
        if let Some(w) = self.waiting_clients.remove(&queue) {
            for (_, cookies) in w.channels {
                self.awaking_clients.extend(cookies);
            }
        } else {
            unreachable!();
        }
    }

    fn notify_queue_created(&mut self, queue: Atom) {
        debug!("notify_queue_created {:?}", queue);
        let prev = self.waiting_clients.insert(queue, Default::default());
        assert!(prev.is_none());
    }

    fn notify_channel_deleted(&mut self, queue: Atom, channel: Atom) {
        debug!("notify_channel_deleted {:?} {:?}", queue, channel);
        if let Some(w) = self.waiting_clients.get_mut(&queue) {
            if let Some(cookies)  = w.channels.remove(&channel) {
                self.awaking_clients.extend(cookies);
            } else {
                unreachable!();
            }
        } else {
            unreachable!();
        }
    }

    fn notify_channel_created(&mut self, queue: Atom, channel: Atom) {
        debug!("notify_channel_created {:?} {:?}", queue, channel);
        if let Some(w) = self.waiting_clients.get_mut(&queue) {
            let prev = w.channels.insert(channel, Default::default());
            debug_assert!(prev.is_none());
        } else {
            unreachable!();
        }
    }

    fn notify_connection_gone(&mut self, connection: &Connection,  event_loop: &mut EventLoop<ServerHandler>) {
        debug!("notify_connection_gone {:?} {:?}", connection.token, connection.cookie);
        if !connection.waiting {
            return
        }
        assert!(event_loop.clear_timeout(connection.timeout.unwrap()));
        // FIXME: possibly expensive
        for (_, w) in self.waiting_clients.iter_mut() {
            for (_, cookies) in w.channels.iter_mut() {
                cookies.retain(|&c| c != connection.cookie);
            }
        }
    }

    fn ready(&mut self, connections: &mut Slab<Connection>, event_loop: &mut EventLoop<ServerHandler>, events: EventSet) -> bool {
        assert_eq!(events, EventSet::readable());

        if let Some(stream) = self.listener.accept().unwrap() {
            let connection_addr = stream.peer_addr();
            info!("incomming connection from {:?}", connection_addr);

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
        match notification.1 {
            NotifyMessage::MaintenanceDone => {
                self.maintenance_timeout = event_loop.timeout_ms(
                    (SERVER, TimeoutMessage::Maintenance), self.config.maintenance_timeout as u64).unwrap();
            }
            NotifyMessage::QueueDelete{queue} => {
                self.notify_queue_deleted(queue);
            },
            NotifyMessage::QueueCreate{queue} => {
                self.notify_queue_created(queue);
            },
            NotifyMessage::ChannelDelete{queue, channel} => {
                self.notify_channel_deleted(queue, channel);
            },
            NotifyMessage::ChannelCreate{queue, channel} => {
                self.notify_channel_created(queue, channel);
            },
            msg => panic!("can't handle msg {:?}", msg)
        }

        true
    }

    fn timeout(&mut self, connections: &mut Slab<Connection>, event_loop: &mut EventLoop<ServerHandler>, timeout: TimeoutMessage) -> bool {
        match timeout {
            TimeoutMessage::Maintenance => {
                let queues = self.queues.clone();
                let channel = event_loop.channel();
                self.thread_pool.execute(move || {
                    let queue_names: Vec<_> = queues.read().keys().cloned().collect();
                    for queue_name in queue_names {
                        // get the shared lock for a brief moment
                        let q_opt = queues.read().get(&queue_name).cloned();
                        if let Some(q) = q_opt {
                            q.as_mut().maintenance();
                        }
                    }
                    channel.send((Cookie::new(SERVER, 0), NotifyMessage::MaintenanceDone)).unwrap();
                });
            },
            TimeoutMessage::WallClock => {
                self.clock_ms = Self::get_clock_ms();
                event_loop.timeout_ms((SERVER, TimeoutMessage::WallClock), 100).unwrap();
            }
            to => panic!("can't handle to {:?}", to)
        }
        true
    }

    fn get_clock_ms() -> u64 {
        let time::Timespec{sec, nsec} = time::get_time();
        sec as u64 + (nsec / 1000 / 1000) as u64
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
            self.connections.remove(token).map(|c| self.server.notify_connection_gone(&c, event_loop));
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
            self.connections.remove(token).map(|c| self.server.notify_connection_gone(&c, event_loop));
        }
        trace!("end notify event for token {:?}", token);
    }

    #[inline]
    fn tick(&mut self, event_loop: &mut EventLoop<Self>) {
        self.server.tick(&mut self.connections, event_loop);
    }

    #[inline]
    fn timeout(&mut self, event_loop: &mut EventLoop<Self>, timeout: Self::Timeout) {
        let token = timeout.0;
        trace!("timeout event for token {:?} with {:?}", token, timeout.1);
        let is_ok = match token {
            SERVER => self.server.timeout(&mut self.connections, event_loop, timeout.1),
            token => if let Some(connection) = self.connections.get_mut(token) {
                connection.timeout(&mut self.server, event_loop, timeout.1)
            } else {
                trace!("token {:?} not found", token);
                false
            }
        };
        if !is_ok {
            trace!("deregistering token {:?}", token);
            self.connections.remove(token).map(|c| self.server.notify_connection_gone(&c, event_loop));
        }
        trace!("end timeout event for token {:?}", token);
    }

    #[inline]
    fn interrupted(&mut self, event_loop: &mut EventLoop<Self>) {
        panic!("interrupted");
    }
}

mod ffi {
    use std::os::unix::io::RawFd;
    extern {
        pub fn sendfile(out_fd: RawFd, in_fd: RawFd, offset: *mut isize, count: usize) -> isize;
    }
}

fn sendfile(out_fd: RawFd, in_fd: RawFd, mut offset: isize, count: usize) -> isize {
    unsafe {
        ffi::sendfile(out_fd, in_fd, &mut offset, count)
    }
}
