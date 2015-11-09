use std::str::{self, FromStr};
use std::net::SocketAddr;
use std::io::{Read, Write};
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
use queue::*;
use queue_backend::Message;
use config::*;
use protocol::{Value, ProtocolError, RequestBuffer, ResponseBuffer};
use utils::*;
use atom::Atom;
use cookie::Cookie;
use time;

const SERVER: Token = Token(0);
const FIRST_CLIENT: Token = Token(1);

#[derive(Debug)]
pub enum NotifyMessage {
    Response{response: Value},
    PutResponse{response: Value, queue: Atom, head: u64},
    GetWouldBlock{request: Value, queue: Atom, channel: Atom, required_head: u64},
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
    request: RequestBuffer,
    response: ResponseBuffer,
    timeout: Option<Timeout>,
    processing: bool,
    waiting: bool,
    hup: bool,
}

struct Dispatch {
    config: Arc<ServerConfig>,
    cookie: Cookie,
    request: Value,
    channel: Sender<NotifyType>,
    meta_lock: Arc<Mutex<()>>,
    queues: Arc<SpinRwLock<HashMap<Atom, Arc<Queue>>>>,
    clock: u32,
}

#[derive(Default)]
struct WaitingClients {
    head: u64,
    channels: HashMap<Atom, VecDeque<(Cookie, Value)>>,
}

pub struct Server {
    config: Arc<ServerConfig>,
    meta_lock: Arc<Mutex<()>>,
    queues: Arc<SpinRwLock<HashMap<Atom, Arc<Queue>>>>,
    waiting_clients: HashMap<Atom, WaitingClients>,
    awaking_clients: Vec<(Cookie, Value)>,
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
    fn with_value(value: Value) -> NotifyMessage {
        NotifyMessage::Response{response: value}
    }

    fn with_ok() -> NotifyMessage {
        Self::with_value(Value::Status("OK".into()))
    }

    fn with_error(error: &str) -> NotifyMessage {
        Self::with_value(Value::Error(error.into()))
    }

    fn with_nil() -> NotifyMessage {
        Self::with_value(Value::Nil)
    }

    fn with_int(int: i64) -> NotifyMessage {
        Self::with_value(Value::Int(int))
    }
}

fn assume_str(possibly_str: &[u8]) -> &str {
    unsafe { str::from_utf8_unchecked(possibly_str) }
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

    fn notify_client(&self, notification: NotifyMessage) {
        self.channel.send((self.cookie, notification)).unwrap();
    }

    fn mget(&self, args: &[&[u8]]) -> NotifyMessage {
        if args.len() < 3 {
            return NotifyMessage::with_error("MPA Queue or Channel Missing")
        }
        let queue_name = assume_str(args[1]);
        let channel_name = assume_str(args[2]);
        let q = if let Some(q) = self.get_queue(queue_name) {
            q
        } else {
            debug!("queue {:?} not found", queue_name);
            return NotifyMessage::with_error("QNF")
        };

        match q.as_mut().get(channel_name, self.clock) {
            Some(Ok(message)) => {
                NotifyMessage::with_value(Value::Message(message))
            },
            Some(Err(required_head)) => {
                debug!("queue {:?} channel {:?} has no messages", queue_name, channel_name);
                NotifyMessage::GetWouldBlock{
                    request: self.request.clone(),
                    queue: q.name().into(),
                    channel: channel_name.into(),
                    required_head: required_head,
                }
            },
            _ => NotifyMessage::with_error("CNF")
        }
    }

    fn mset(&self, args: &[&[u8]]) -> NotifyMessage {
        if args.len() < 3 {
            return NotifyMessage::with_error("MPA Queue or Channel Missing")
        }
        let queue_name = assume_str(args[1]);
        let channel_name = assume_str(args[2]);
        let q = self.get_or_create_queue(queue_name);

        if self.create_channel(&q, channel_name) {
            NotifyMessage::with_ok()
        } else {
            NotifyMessage::with_error("CAE Channel Already Exists")
        }
    }

    fn lpush_(&self, args: &[&[u8]]) -> NotifyMessage {
        if args.len() < 3 {
            return NotifyMessage::with_error("MPA Queue or Value Missing")
        }
        let queue_name = assume_str(args[1]);
        let value = args[2];
        let q = self.get_or_create_queue(queue_name);

        debug!("inserting {} msgs into {:?} [{:?}]",
            args[2..].len(), queue_name, value.len());
        let last_id = q.as_mut().push_many(args[2..], self.clock).unwrap();
        trace!("inserted {} messages into {:?} with last id {:?}",
            args[2..].len(), queue_name, last_id);

        NotifyMessage::PutResponse{
            response: Value::Nil,
            queue: q.name().into(),
            head: last_id + 1
        }
    }

    fn hdel(&self, args: &[&[u8]]) -> NotifyMessage {
        if args.len() < 4 {
            return NotifyMessage::with_error("MPA Queue, Channel or Id Missing")
        }
        let queue_name = assume_str(args[1]);
        let channel_name = assume_str(args[2]);
        let q = if let Some(q) = self.get_queue(queue_name) {
            q
        } else {
            return NotifyMessage::with_error("QNF Queue Not Found");
        };

        let mut successfully = 0;
        for id_arg in &args[3..] {
            if let Ok(id) = assume_str(id_arg).parse::<u64>() {
                if q.as_mut().ack(channel_name, id, self.clock).is_some() {
                    successfully += 1
                }
            } else {
                return NotifyMessage::with_error("IID Invalid Id")
            }
        }

        NotifyMessage::with_int(successfully)
    }

    fn del(&self, args: &[&[u8]]) -> NotifyMessage {
        if args.len() < 2 {
            return NotifyMessage::with_error("MPA Queue Missing")
        }
        let queue_name = assume_str(args[1]);
        let channel_name_opt = args.get(2).map(|s| assume_str(s));
        let q = if let Some(q) = self.get_queue(queue_name) {
            q
        } else {
            return NotifyMessage::with_int(0);
        };

        match channel_name_opt {
            None => {
                q.as_mut().purge();
            },
            Some("_purge") => {
                self.delete_queue(q);
            },
            Some(channel_name) => {
                if ! self.delete_channel(&q, channel_name) {
                    return NotifyMessage::with_int(0);
                }
            },
        }

        NotifyMessage::with_int(1)
    }

    fn dispatch(&self) {
        debug!("dispatch {:?} {:?}", self.cookie.token(), self.request);
        let mut args: [&[u8]; 50] = [b""; 50];
        let mut argc: usize = 0;

        match self.request {
            Value::Bulk(ref b) if b.len() > 0 && b.len() <= 50 => {
                for v in b {
                    if let &Value::Data(ref d) = v {
                        args[argc] = d.as_ref();
                        argc += 1;
                    } else {
                        return self.notify_client(NotifyMessage::with_error("ICOM Invalid Type"))
                    };
                }
            }
            _ => return self.notify_client(NotifyMessage::with_error("ICOM Invalid Command"))
        }

        let notification = match assume_str(args[0]) {
            "LPUSH" | "LPUSHX" => self.lpush_(&args[..argc]),
            "MSET" => self.mset(&args[..argc]),
            "HDEL" => self.hdel(&args[..argc]),
            "DEL" => self.del(&args[..argc]),
            "MGET" => self.mget(&args[..argc]),
            _ => NotifyMessage::with_error("UCOM Unknown Command")
        };

        return self.notify_client(notification);
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
            request: RequestBuffer::new(),
            response: ResponseBuffer::new(),
            timeout: None,
            processing: false,
            waiting: false,
            hup: false,
        }
    }

    fn send_response(&mut self, value: Value, server: &mut Server, event_loop: &mut EventLoop<ServerHandler>) {
        self.response.push_value(value);
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
                while let Ok(bytes_read) = self.stream.read(self.request.mut_bytes()) {
                    if bytes_read == 0 {
                        break
                    }
                    self.request.advance(bytes_read);
                    trace!("filled request with {} bytes, remaining: {}", bytes_read, self.request.remaining());
                }
            };
            match self.request.pop_value() {
                Ok(value) => {
                    assert!(!self.processing);
                    assert!(!self.waiting);
                    assert!(self.timeout.is_none());
                    self.request_clock_ms = server.clock_ms;
                    self.processing = true;
                    self.interest = EventSet::all() - EventSet::readable() - EventSet::writable();
                    event_loop.reregister(&self.stream, self.token, self.interest, PollOpt::level()).unwrap();
                    self.dispatch(value, server, event_loop);
                },
                Err(ProtocolError::Incomplete) => (),
                Err(ProtocolError::Invalid(error)) => {
                    error!("{:?} protocol is invalid {:?}", self.token, error);
                    return false
                },
            }
        }

        if events.is_writable() {
            while let Ok(bytes_written) = self.stream.write(self.response.bytes()) {
                if bytes_written == 0 {
                    break
                }
                self.response.advance(bytes_written);
                trace!("filled response with {} bytes, remaining: {}", bytes_written, self.response.remaining());
            }
            if self.response.remaining() == 0 {
                // TODO: try to pop a value from the request_buffer
                self.interest = EventSet::all() - EventSet::writable();
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
                    self.send_response(Value::Nil, server, event_loop);
                } else {
                    self.waiting = true;
                    let timeout_m = TimeoutMessage::Timeout{
                        queue: queue.clone(),
                        channel: channel.clone()
                    };
                    self.timeout = Some(event_loop.timeout_ms(
                        (self.token, timeout_m),
                        (deadline - server.clock_ms) as u64
                    ).unwrap());
                    server.wait_queue(request, queue, channel, cookie, required_head);
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
                server.notify_timeout(queue, channel, self.cookie);
                self.send_response(Value::Nil, server, event_loop);
            },
            to => panic!("can't handle to {:?}", to)
        }
        true
    }

    fn dispatch(&mut self, request: Value, server: &mut Server, event_loop: &mut EventLoop<ServerHandler>) {
        server.nonce = server.nonce.wrapping_add(1);
        self.cookie = Cookie::new(self.token, server.nonce);
        // FIXME: this not only allocates but clone 4 ARCs
        let dispatch = Dispatch {
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

    fn retry(&mut self, request: Value, server: &mut Server, event_loop: &mut EventLoop<ServerHandler>) {
        assert!(!self.processing);
        assert!(self.waiting);
        self.processing = true;
        self.waiting = false;
        assert!(event_loop.clear_timeout(self.timeout.take().unwrap()));
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

    fn wait_queue(&mut self, request: Value, queue: Atom, channel: Atom, cookie: Cookie, required_head: u64) {
        debug!("wait_queue {:?} {:?} {:?} {:?}", queue, channel, cookie, required_head);
        if let Some(w) = self.waiting_clients.get_mut(&queue) {
            if let Some(cookies)  = w.channels.get_mut(&channel) {
                if required_head >= w.head {
                    cookies.push_back((cookie, request));
                } else {
                    debug!("Race condition, queue {:?} channel {:?} has new data {:?}", queue, channel, w.head);
                    self.awaking_clients.push((cookie, request));
                }
            } else {
                debug!("Race condition, queue {:?} channel {:?} is gone", queue, channel);
                self.awaking_clients.push((cookie, request));
            }
        } else {
            debug!("Race condition, queue {:?} is gone", queue);
            self.awaking_clients.push((cookie, request));
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
                cookies.retain(|&(c, _)| c != cookie);
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
                cookies.retain(|&(c, _)| c != connection.cookie);
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
            for (cookie, request) in awaking_clients.drain(..) {
                connections[cookie.token()].retry(request, self, event_loop);
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
