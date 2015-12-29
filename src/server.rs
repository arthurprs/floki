use std::{mem, cmp, thread};
use std::str::{self, FromStr};
use std::net::SocketAddr;
use std::io::{Read, Write};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use spin::RwLock as SpinRwLock;
use mio::tcp::{TcpStream, TcpListener};
use mio::util::Slab;
use mio::{self, Token, EventLoop, EventSet, PollOpt, Timeout, Handler};
use threadpool::ThreadPool;
use rustc_serialize::json;
use promising_future::{future_promise, Promise};
use num_cpus::get as get_num_cpus;
use queue::*;
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
    MessagesAvailable{queue: Atom, channel: Atom, available: usize},
    GetWouldBlock{request: Value, queue: Atom, channel: Atom, required_head: u64, timeout: u32},
    ChannelCreate{queue: Atom, channel: Atom},
    ChannelDelete{queue: Atom, channel: Atom},
    QueueCreate{queue: Atom},
    QueueDelete{queue: Atom},
    ServerInfo{promise: Promise<ServerInfo>},
}

pub type NotifyType = (Cookie, NotifyMessage);

impl From<QueueError> for NotifyMessage {
    fn from(from: QueueError) -> Self {
        match from {
            QueueError::ChannelNotFound =>
                NotifyMessage::with_error("CNF Channel Not Found"),
            QueueError::ChannelAlreadyExists =>
                NotifyMessage::with_error("CAE Channel Already Exists"),
            _ => NotifyMessage::with_error(&format!("Unexpected error {:?}", from))
        }
    }
}

#[derive(Debug)]
pub enum TimeoutMessage {
    Timeout{queue: Atom, channel: Atom},
}

pub type TimeoutType = (Token, TimeoutMessage);

struct Client {
    token: Token,
    stream: TcpStream,
    interest: EventSet,
    request_clock_ms: u64,
    request: RequestBuffer,
    response: ResponseBuffer,
    timeout: Option<(Timeout, Atom, Atom)>,
    processing: Option<Cookie>,
    hup: bool,
}

struct Dispatch {
    config: Arc<ServerConfig>,
    cookie: Cookie,
    request: Value,
    channel: mio::Sender<NotifyType>,
    meta_lock: Arc<Mutex<()>>,
    queues: Arc<SpinRwLock<HashMap<Atom, Arc<Queue>>>>,
    clock: u32,
}

#[derive(Default)]
struct WaitingClients {
    head: u64,
    channels: HashMap<Atom, VecDeque<(Token, Value)>>,
}

#[derive(Debug, RustcEncodable)]
pub struct ServerInfo {
    client_count: u32,
    blocked_client_count: u32,
    queue_count: u32,
    thread_count: u32,
    clock: u32,
}

pub struct Server {
    config: Arc<ServerConfig>,
    meta_lock: Arc<Mutex<()>>,
    queues: Arc<SpinRwLock<HashMap<Atom, Arc<Queue>>>>,
    waiting_clients: HashMap<Atom, WaitingClients>,
    awaking_clients: Vec<(Token, Value)>,
    listener: TcpListener,
    thread_pool: ThreadPool,
    thread_count: u32,
    nonce: u64,
    internal_clock_ms: u64,
}

pub struct ServerHandler {
    server: Server,
    clients: Slab<Client>,
}

macro_rules! try_or_error {
    ($try: expr) => (
        match $try {
            Ok(ok) => ok,
            Err(error) => return error.into(),
        }
    );
    ($try: expr, $error: expr) => (
        match $try {
            Ok(ok) => ok,
            _ => return NotifyMessage::with_error($error),
        }
    )
}

macro_rules! try_or_int {
    ($try: expr, $int: expr) => (
        match $try {
            Ok(ok) => ok,
            _ => return NotifyMessage::with_int($int),
        }
    )
}

impl NotifyMessage {
    fn with_value(value: Value) -> NotifyMessage {
        NotifyMessage::Response{response: value}
    }

    fn with_ok() -> NotifyMessage {
        Self::with_value(Value::Status("OK".into()))
    }

    fn with_error<T: AsRef<str>>(error: T) -> NotifyMessage {
        Self::with_value(Value::Error(error.as_ref().into()))
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

fn check_identifier_str(possibly_str: &[u8]) -> Result<&str, NotifyMessage> {
    if possibly_str.iter().all(|&b| (b as char).is_alphanumeric() || b == b'_') {
        Ok(assume_str(possibly_str))
    } else {
        // FIXME
        Err(NotifyMessage::with_error("IPA Invalid Identifier Name"))
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
        let q = self.queues.write().remove(&*q.name()).unwrap();
        let mut wait_count = 0;
        while Arc::strong_count(&q) > 1 {
            thread::sleep(Duration::from_millis(100));
            wait_count += 1;
            if wait_count % 10 == 0 {
                warn!("Still waiting for Queue {:?} to be freed {}", q.name(), wait_count);
            }
        }
        self.notify_server(NotifyMessage::QueueDelete{
            queue: q.name().into(),
        });
        drop(meta_lock);
        q.delete();
    }

    fn delete_channel(&self, q: &Queue, channel_name: &str) -> QueueResult<()> {
        debug!("deleting queue {:?} channel {:?}", q.name(), channel_name);
        let meta_lock = self.meta_lock.lock().unwrap();
        let result = q.delete_channel(channel_name);
        if result.is_ok() {
            self.notify_server(NotifyMessage::ChannelDelete{
                queue: q.name().into(),
                channel: channel_name.into(),
            });
        }
        drop(meta_lock);
        result
    }

    fn create_channel(&self, q: &Queue, channel_name: &str) -> QueueResult<()> {
        info!("creating queue {:?} channel {:?}", q.name(), channel_name);
        let meta_lock = self.meta_lock.lock().unwrap();
        let result = q.create_channel(channel_name, self.clock);
        if result.is_ok() {
            self.notify_server(NotifyMessage::ChannelCreate{
                queue: q.name().into(),
                channel: channel_name.into(),
            });
        }
        drop(meta_lock);
        result
    }

    fn get_or_create_queue(&self, name: &str) -> Arc<Queue> {
        if let Some(sq) = self.queues.read().get(name) {
            return sq.clone()
        }

        let meta_lock = self.meta_lock.lock().unwrap();
        let queue = self.queues.write().entry(name.into()).or_insert_with(|| {
            info!("Creating queue {:?}", name);
            let inner_queue = Queue::new(self.config.new_queue_config(name), false);
            debug!("Done creating queue {:?}", name);

            self.notify_server(NotifyMessage::QueueCreate{
                queue: inner_queue.name().into(),
            });

            Arc::new(inner_queue)
        }).clone();
        drop(meta_lock);
        queue
    }

    fn notify_server(&self, notification: NotifyMessage) {
        self.channel.send((Cookie::new(SERVER, 0), notification)).unwrap();
    }

    fn notify_client(&self, notification: NotifyMessage) {
        self.channel.send((self.cookie, notification)).unwrap();
    }

    fn hmset(&self, args: &[&[u8]]) -> NotifyMessage {
        if args.len() < 5 {
            return NotifyMessage::with_error("MPA Queue or Channel or TS|ID or Value Missing")
        }
        let queue_name = assume_str(args[1]);
        let channel_name = assume_str(args[2]);
        let seek_type = assume_str(args[3]);
        let seek_value_str = assume_str(args[4]);
        let q = try_or_error!(self.get_queue(queue_name).ok_or(()), "QNF Queue Not Found");

        match seek_type {
            "TS" => {
                let timestamp = try_or_error!(seek_value_str.parse::<u32>(), "IPA Invalid Timestamp");
                try_or_error!(q.seek_channel_to_timestamp(channel_name, timestamp, self.clock));
            },
            "ID" => {
                let id = try_or_error!(seek_value_str.parse::<u64>(), "IPA Invalid ID");
                try_or_error!(q.seek_channel_to_id(channel_name, id, self.clock));
            },
            _ => return NotifyMessage::with_error("IPA Invalid Seek Type Expected TS or ID")
        }
        NotifyMessage::with_int(2)
    }

    fn hmget(&self, args: &[&[u8]]) -> NotifyMessage {
        if args.len() < 4 {
            return NotifyMessage::with_error("MPA Queue or Channel or Count Missing")
        }
        let queue_name = assume_str(args[1]);
        let channel_name = assume_str(args[2]);
        let count = try_or_error!(assume_str(args[3]).parse::<usize>(), "IPA Invalid count value");
        let timeout = if let Some(arg) = args.get(4) {
            try_or_error!(assume_str(arg).parse::<u32>(), "IPA Invalid timeout value")
        } else {
            0
        };
        let q = try_or_error!(self.get_queue(queue_name).ok_or(()), "QNF Queue Not Found");

        // get one by one, this contributes for fairness avoiding starving consumers
        let mut results = Vec::with_capacity(count);
        for _ in 0..count {
            match q.get(channel_name, self.clock) {
                Ok(ticket_message) => {
                    results.push(Value::Message(ticket_message));
                },
                Err(QueueError::EndOfQueue(required_head)) => {
                    // block only if timeout is set and we have no results
                    if timeout != 0 && results.is_empty() {
                        return NotifyMessage::GetWouldBlock{
                            request: self.request.clone(),
                            queue: q.name().into(),
                            channel: channel_name.into(),
                            required_head: required_head,
                            timeout: timeout,
                        }
                    }
                    break
                },
                Err(error) => return error.into()
            }
        }
        NotifyMessage::with_value(Value::Array(results))
    }

    fn set(&self, args: &[&[u8]]) -> NotifyMessage {
        if args.len() < 3 {
            return NotifyMessage::with_error("MPA Queue or Channel Missing")
        }
        let queue_name = try_or_error!(check_identifier_str(args[1]));
        let channel_name = try_or_error!(check_identifier_str(args[2]));
        let q = self.get_or_create_queue(queue_name);

        try_or_error!(self.create_channel(&q, channel_name));
        NotifyMessage::with_ok()
    }

    fn rpush(&self, args: &[&[u8]]) -> NotifyMessage {
        if args.len() < 3 {
            return NotifyMessage::with_error("MPA Queue or Value Missing")
        }
        let queue_name = assume_str(args[1]);
        let messages = &args[2..];
        let q = try_or_error!(self.get_queue(queue_name).ok_or(()), "QNF Queue Not Found");

        debug!("inserting {} msgs into {:?}",
            messages.len(), queue_name);
        let last_id = try_or_error!(q.push_many(messages, self.clock));
        trace!("inserted {} messages into {:?} with last id {:?}",
            messages.len(), queue_name, last_id);

        NotifyMessage::PutResponse{
            response: Value::Int(messages.len() as i64),
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
        let q = try_or_error!(self.get_queue(queue_name).ok_or(()), "QNF Queue Not Found");

        let mut successfully = 0;
        for ticket_arg in &args[3..] {
            let ticket = try_or_error!(assume_str(ticket_arg).parse::<i64>(), "IPA Invalid Ticket");
            if q.ack(channel_name, ticket, self.clock).is_ok() {
                successfully += 1
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
        let q = try_or_int!(self.get_queue(queue_name).ok_or(()), 0);

        match channel_name_opt {
            None => {
                self.delete_queue(q);
            },
            Some(channel_name) => {
                try_or_int!(self.delete_channel(&q, channel_name), 0);
            },
        }

        NotifyMessage::with_int(1)
    }

    fn srem(&self, args: &[&[u8]]) -> NotifyMessage {
        if args.len() < 3 {
            return NotifyMessage::with_error("MPA Queue or Channel Missing")
        }
        let queue_name = assume_str(args[1]);
        let channel_name = assume_str(args[2]);
        let q = try_or_int!(self.get_queue(queue_name).ok_or(()), 0);

        match channel_name {
            "*" => {
                q.purge();
            },
            channel_name => {
                try_or_int!(q.purge_channel(channel_name, self.clock), 0);
            },
        }

        NotifyMessage::with_int(1)
    }

    fn config_queue(&self, queue_prefix: &str) -> StdHashMap<String, QueueConfig> {
        self.queues.read().values().filter_map(|queue| {
            if queue_prefix == "*" || queue.name().starts_with(queue_prefix) {
                Some((queue.name().into(), queue.config_cloned()))
            } else {
                None
            }
        }).collect()
    }

    fn config_get(&self, args: &[&[u8]]) -> NotifyMessage {
        let config_args = assume_str(args[2]).splitn(2, ".").collect::<Vec<_>>();
        let json_bytes: Vec<u8> = match &config_args[..] {
            ["server"] =>
                json::encode(&self.config).unwrap(),
            ["queues"] =>
                json::encode(&self.config_queue("*")).unwrap(),
            ["queues", queue_prefix] =>
                json::encode(&self.config_queue(queue_prefix)).unwrap(),
            _ =>
                return NotifyMessage::with_error("IPA Invalid CONFIG parameter")
        }.into();

        let value = Value::Array(vec![Value::Data((&json_bytes[..]).into())]);
        NotifyMessage::with_value(value)
    }

    fn config(&self, args: &[&[u8]]) -> NotifyMessage {
        if args.len() < 3 || (assume_str(args[1]) == "SET" && args.len() < 4) {
            return NotifyMessage::with_error("MPA Missing CONFIG Parameter")
        }
        match assume_str(args[1]) {
            "GET" => self.config_get(args),
            "SET" => NotifyMessage::with_error("IPA CONFIG SET Not Implemented"),
            _ => NotifyMessage::with_error("IPA Invalid CONFIG Parameter")
        }
    }

    fn info_server(&self)-> ServerInfo {
        let (fut, prom) = future_promise();
        self.notify_server(NotifyMessage::ServerInfo{promise: prom});
        fut.value().unwrap()
    }

    fn info_queue(&self, queue_prefix: &str) -> StdHashMap<String, QueueInfo> {
        self.queues.read().values().filter_map(|queue| {
            if queue_prefix == "*" || queue.name().starts_with(queue_prefix) {
                Some((queue.name().into(), queue.info(self.clock)))
            } else {
                None
            }
        }).collect()
    }

    fn info(&self, args: &[&[u8]]) -> NotifyMessage {
        if args.len() < 2 {
            return NotifyMessage::with_error("MPA Missing INFO Parameter")
        }

        let info_args = assume_str(args[1]).splitn(2, ".").collect::<Vec<_>>();
        let json_bytes: Vec<u8> = match &info_args[..] {
            ["server"] =>
                json::encode(&self.info_server()).unwrap(),
            ["queues"] =>
                json::encode(&self.info_queue("*")).unwrap(),
            ["queues", queue_prefix] =>
                json::encode(&self.info_queue(queue_prefix)).unwrap(),
            _ =>
                return NotifyMessage::with_error("IPA Invalid INFO parameter")
        }.into();

        let value = Value::Array(vec![Value::Data((&json_bytes[..]).into())]);
        NotifyMessage::with_value(value)
    }

    fn dispatch(&self) {
        debug!("dispatch {:?} {:?}", self.cookie.token(), self.request);
        let mut args: [&[u8]; 50] = [b""; 50];
        let mut argc: usize = 0;

        match self.request {
            Value::Array(ref a) if a.len() > 0 && a.len() <= 50 => {
                for v in a {
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

        let args_slice = &args[..argc];
        let notification = match assume_str(args[0]) {
            "RPUSH" => self.rpush(args_slice), // push one or more messages
            "HMGET" => self.hmget(args_slice), // get one or more messages
            "HDEL" => self.hdel(args_slice), // ack messages
            "INFO" => self.info(args_slice), // info
            "HMSET" => self.hmset(args_slice), // seek channel to the specified ts or id
            "SET" => self.set(args_slice), // create queue/channel
            "DEL" => self.del(args_slice), // delete queue/channel
            "SREM" => self.srem(args_slice), // purge queue/channel
            "CONFIG" => self.config(args_slice), // config
            _ => NotifyMessage::with_error("UCOM Unknown Command")
        };

        return self.notify_client(notification);
    }
}

impl Client {

    fn new(token: Token, stream: TcpStream) -> Client {
        Client {
            token: token,
            stream: stream,
            interest: EventSet::all() - EventSet::writable(),
            request_clock_ms: 0,
            request: RequestBuffer::new(),
            response: ResponseBuffer::new(),
            timeout: None,
            processing: None,
            hup: false,
        }
    }

    fn send_response(&mut self, value: Value, event_loop: &mut EventLoop<ServerHandler>) {
        self.response.push_value(value);
        self.interest = EventSet::all() - EventSet::readable();
        event_loop.reregister(&self.stream, self.token, self.interest, PollOpt::level()).unwrap();
    }

    fn try_parse_request(&mut self, server: &mut Server, event_loop: &mut EventLoop<ServerHandler>) -> Result<bool, ()> {
        match self.request.pop_value() {
            Ok(value) => {
                self.request_clock_ms = server.clock_ms();
                self.interest = EventSet::all() - EventSet::readable() - EventSet::writable();
                event_loop.reregister(&self.stream, self.token, self.interest, PollOpt::level()).unwrap();
                self.dispatch(value, server, event_loop);
                Ok(true)
            },
            Err(ProtocolError::Incomplete) => Ok(false),
            Err(ProtocolError::Invalid(error)) => {
                error!("{:?} protocol is invalid {:?}", self.token, error);
                Err(())
            },
        }
    }

    fn ready(&mut self, server: &mut Server, event_loop: &mut EventLoop<ServerHandler>, events: EventSet) -> bool {
        if events.is_hup() || events.is_error() {
            debug!("received events {:?} for token {:?}", events,  self.token);
            event_loop.deregister(&self.stream).unwrap();
            self.hup = true;
            return self.processing.is_some()
        }

        if events.is_readable() {
            while let Ok(bytes_read) = self.stream.read(self.request.mut_bytes()) {
                if bytes_read == 0 {
                    break
                }
                self.request.advance(bytes_read);
                trace!("filled request with {} bytes, remaining: {}", bytes_read, self.request.remaining());
            }
            if self.try_parse_request(server, event_loop).is_err() {
                return false
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
                // if we were to support pipelining we'd have to try to pop a request value here
                self.interest = EventSet::all() - EventSet::writable();
                event_loop.reregister(&self.stream, self.token, self.interest, PollOpt::level()).unwrap();
            }
        }
        true
    }

    fn notify(&mut self, server: &mut Server, event_loop: &mut EventLoop<ServerHandler>, notification: NotifyType) -> bool {
        assert!(self.timeout.is_none());
        let (cookie, msg) = notification;
        if Some(cookie) != self.processing {
            return true
        }

        self.processing = None;
        if self.hup {
            return false
        }

        match msg {
            NotifyMessage::GetWouldBlock{request, queue, channel, required_head, timeout} => {
                let deadline = self.request_clock_ms + (timeout as u64 * 1000);
                let clock = server.clock_ms();
                if clock >= deadline {
                    self.send_response(Value::Nil, event_loop);
                } else {
                    let timeout_msg = TimeoutMessage::Timeout{
                        queue: queue.clone(),
                        channel: channel.clone()
                    };
                    let timeout = event_loop.timeout_ms(
                        (self.token, timeout_msg), (deadline - clock) as u64).unwrap();
                    self.timeout = Some((timeout, queue.clone(), channel.clone()));
                    server.wait_queue(request, queue, channel, self.token, required_head);
                }
            },
            NotifyMessage::PutResponse{response, queue, head} => {
                self.send_response(response, event_loop);
                server.notify_queue(queue, head);
            },
            NotifyMessage::Response{response} => {
                self.send_response(response, event_loop);
            },
            msg => panic!("can't handle msg {:?}", msg)
        }

        true
    }

    fn timeout(&mut self, server: &mut Server, event_loop: &mut EventLoop<ServerHandler>, timeout: TimeoutMessage) -> bool {
        match timeout {
            TimeoutMessage::Timeout{queue, channel} => {
                assert!(self.processing.is_none());
                assert!(self.timeout.take().is_some());
                server.notify_timeout(queue, channel, self.token);
                self.send_response(Value::Nil, event_loop);
            },
            // _ => panic!("can't handle timeout {:?}", timeout)
        }
        true
    }

    fn dispatch(&mut self, request: Value, server: &mut Server, event_loop: &mut EventLoop<ServerHandler>) {
        assert!(self.processing.is_none());
        let cookie = server.make_cookie(self.token);
        self.processing = Some(cookie);
        // FIXME: this not only allocates but clone 4 ARCs
        let dispatch = Dispatch {
            cookie: cookie,
            config: server.config.clone(),
            channel: event_loop.channel(),
            request: request,
            meta_lock: server.meta_lock.clone(),
            queues: server.queues.clone(),
            clock: server.clock_s(),
        };
        server.thread_pool.execute(move || dispatch.dispatch());
    }

    fn retry(&mut self, request: Value, server: &mut Server, event_loop: &mut EventLoop<ServerHandler>) {
        assert!(event_loop.clear_timeout(self.timeout.take().unwrap().0));
        self.dispatch(request, server, event_loop);
    }
}

impl Server {

    pub fn new(config: ServerConfig) -> (ServerHandler, EventLoop<ServerHandler>) {
        let addr = SocketAddr::from_str(&config.bind_address).unwrap();

        debug!("binding tcp socket to {:?}", addr);
        let listener = TcpListener::bind(&addr).unwrap();

        let num_cpus = get_num_cpus();
        let thread_count = cmp::max(6, num_cpus * 2) + 2;
        debug!("detected {} cpus, using {} threads", num_cpus, thread_count);

        let mut event_loop = EventLoop::new().unwrap();
        event_loop.register(&listener, SERVER,
            EventSet::all() - EventSet::writable(), PollOpt::level()).unwrap();

        let mut server = Server {
            listener: listener,
            config: Arc::new(config),
            meta_lock: Arc::new(Mutex::new(())),
            queues: Default::default(),
            thread_pool: ThreadPool::new(thread_count),
            thread_count: thread_count as u32,
            waiting_clients: Default::default(),
            awaking_clients: Default::default(),
            nonce: 0,
            internal_clock_ms: 0,
        };

        info!("Opening queues...");
        let queue_configs = ServerConfig::read_queue_configs(&server.config).
            expect("Error reading queue configurations");
        for queue_config in queue_configs {
            info!("Opening queue {:?}", queue_config.name);

            let q = Queue::new(queue_config, true);
            // load state
            let info = q.info(server.clock_s());
            let mut waiting_clients = WaitingClients {
                head: info.head,
                channels: Default::default(),
            };
            for channel_name in info.channels.keys() {
                waiting_clients.channels.insert(channel_name.into(), Default::default());
            }
            server.waiting_clients.insert(q.name().into(), waiting_clients);

            debug!("Done opening queue {:?}", q.name());
            server.queues.write().insert(q.name().into(), Arc::new(q));
        }
        debug!("Opening complete!");

        server.start_maintenance(&mut event_loop);
        server.start_monitor(&mut event_loop);

        let clients = Slab::new_starting_at(FIRST_CLIENT, server.config.max_connections);

        let server_handler = ServerHandler {
            server: server,
            clients: clients,
        };

        (server_handler, event_loop)
    }

    fn make_cookie(&mut self, token: Token) -> Cookie {
        self.nonce = self.nonce.wrapping_add(1);
        Cookie::new(token, self.nonce)
    }

    fn wait_queue(&mut self, request: Value, queue: Atom, channel: Atom, token: Token, required_tail: u64) {
        debug!("wait_queue {:?} {:?} {:?} {:?}", queue, channel, token, required_tail);
        if let Some(w) = self.waiting_clients.get_mut(&queue) {
            if let Some(tokens)  = w.channels.get_mut(&channel) {
                if required_tail >= w.head {
                    tokens.push_back((token, request));
                    return;
                } else {
                    debug!("Race condition, queue {:?} channel {:?} has new data {:?}", queue, channel, w.head);
                }
            } else {
                debug!("Race condition, queue {:?} channel {:?} is gone", queue, channel);
            }
        } else {
            debug!("Race condition, queue {:?} is gone", queue);
        }
        self.awaking_clients.push((token, request));
    }

    fn notify_queue(&mut self, queue: Atom, new_head: u64) {
        debug!("notify_queue {:?} {:?}", queue, new_head);
        if let Some(w) = self.waiting_clients.get_mut(&queue) {
            if new_head > w.head {
                let available = (new_head - w.head) as usize;
                w.head = new_head;
                for (_, tokens) in w.channels.iter_mut() {
                    let drain_len = cmp::min(available, tokens.len());
                    self.awaking_clients.extend(tokens.drain(..drain_len));
                }
            } else {
                debug!("Race condition, queue {:?} tail already advanced", queue);
            }
        } else {
            debug!("Race condition, queue {:?} is gone", queue);
        }
    }

    fn notify_channel(&mut self, queue: Atom, channel: Atom, available: usize) {
        debug!("notify_channel {:?} {:?} {:?}", queue, channel, available);
        if let Some(w) = self.waiting_clients.get_mut(&queue) {
            if let Some(tokens) = w.channels.get_mut(&channel) {
                let drain_len = cmp::min(available, tokens.len());
                self.awaking_clients.extend(tokens.drain(..drain_len));
            } else {
                debug!("Race condition, queue {:?} channel {:?} is gone", queue, channel);
            }
        } else {
            debug!("Race condition, queue {:?} is gone", queue);
        }
    }

    fn notify_timeout(&mut self, queue: Atom, channel: Atom, token: Token) {
        debug!("notify_timeout {:?} {:?} {:?}", queue, channel, token);
        if let Some(w) = self.waiting_clients.get_mut(&queue) {
            if let Some(tokens)  = w.channels.get_mut(&channel) {
                // FIXME: only need to delete the first ocurrence
                tokens.retain(|&(t, _)| t != token);
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

    fn notify_client_gone(&mut self, client: &mut Client,  event_loop: &mut EventLoop<ServerHandler>) {
        debug!("notify_client_gone {:?}", client.token);
        if client.timeout.is_none() {
            return
        }
        let (timeout, queue, channel) = client.timeout.take().unwrap();
        assert!(event_loop.clear_timeout(timeout));

        if let Some(w) = self.waiting_clients.get_mut(&queue) {
            if let Some(cookies)  = w.channels.get_mut(&channel) {
                // FIXME: only need to delete the first ocurrence
                cookies.retain(|&(c, _)| c != client.token);
            } else {
                unreachable!();
            }
        } else {
            unreachable!();
        }
    }

    fn remove_client(&mut self, token: Token, clients: &mut Slab<Client>, event_loop: &mut EventLoop<ServerHandler>) {
        info!("closing token {:?} client", token);
        let mut client = clients.remove(token).unwrap();
        self.notify_client_gone(&mut client, event_loop);
    }

    fn ready(&mut self, clients: &mut Slab<Client>, event_loop: &mut EventLoop<ServerHandler>, events: EventSet) -> bool {
        assert_eq!(events, EventSet::readable());

        if let Some((stream, client_addr)) = self.listener.accept().unwrap() {
            trace!("incomming client from {:?}", client_addr);

            let token_opt = clients.insert_with(|token| Client::new(token, stream));

            if let Some(token) = token_opt {
                info!("assigned token {:?} to client {:?}", token, client_addr);

                event_loop.register(
                    &clients[token].stream,
                    token,
                    clients[token].interest,
                    PollOpt::level()
                ).unwrap();
            } else {
                warn!("dropping incomming client from {:?}, max_clients reached", client_addr);
            }
        }
        true
    }

    fn tick(&mut self, clients: &mut Slab<Client>, event_loop: &mut EventLoop<ServerHandler>) {
        if !self.awaking_clients.is_empty() {
            let mut awaking_clients = mem::replace(&mut self.awaking_clients, Vec::new());
            for (token, request) in awaking_clients.drain(..) {
                clients[token].retry(request, self, event_loop);
            }
            self.awaking_clients = awaking_clients;
        }
        // invalidate clock cache
        self.internal_clock_ms = 0;
    }

    fn notify(&mut self, clients: &mut Slab<Client>, _: &mut EventLoop<ServerHandler>, notification: NotifyType) -> bool {
        match notification.1 {
            NotifyMessage::MessagesAvailable{queue, channel, available} => {
                self.notify_channel(queue, channel, available);
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
            NotifyMessage::ServerInfo{promise} => {
                promise.set(self.info(clients));
            }
            msg => panic!("can't handle msg {:?}", msg)
        }

        true
    }

    fn info(&self, clients: &mut Slab<Client>) -> ServerInfo {
        ServerInfo {
            client_count: clients.count() as u32,
            blocked_client_count:
                self.waiting_clients.values().map(|w|
                    w.channels.values().map(|wc| wc.len() as u32).sum::<u32>()
                ).sum(),
            thread_count: self.thread_count,
            queue_count: self.queues.read().len() as u32,
            clock: Self::get_clock_s(),
        }
    }

    fn start_maintenance(&mut self, _: &mut EventLoop<ServerHandler>) {
        let config = self.config.clone();
        let queues = self.queues.clone();
        self.thread_pool.execute(move || {
            let mut queue_names = Vec::new();
            loop {
                // maintenance incurs heavy IO, so avoid holding the queues lock
                queue_names.clear();
                queue_names.extend(queues.read().keys().cloned());
                for queue_name in &queue_names {
                    // get the shared lock for a brief moment
                    let q_opt = queues.read().get(queue_name).cloned();
                    if let Some(q) = q_opt {
                        q.maintenance(Self::get_clock_s());
                    }
                }
                thread::sleep(Duration::from_millis(config.maintenance_interval));
            }
        });
    }

    fn start_monitor(&mut self, event_loop: &mut EventLoop<ServerHandler>) {
        let config = self.config.clone();
        let queues = self.queues.clone();
        let evloop_channel = event_loop.channel();
        self.thread_pool.execute(move || {
            loop {
                let clock = Self::get_clock_s();
                for (queue_name, queue) in queues.read().iter() {
                    queue.iter_channels(clock, |channel_name, channel| {
                        let available = channel.messages_available();
                        if available > 0 {
                            evloop_channel.send((
                                Cookie::new(SERVER, 0),
                                NotifyMessage::MessagesAvailable{
                                    queue: queue_name.clone(),
                                    channel: channel_name.clone(),
                                    available: available as usize,
                                }
                            )).unwrap();
                        }
                    });
                }
                thread::sleep(Duration::from_millis(config.monitor_interval));
            }
        });
    }

    fn timeout(&mut self, _: &mut EventLoop<ServerHandler>, timeout: TimeoutMessage) -> bool {
        match timeout {
            _ => panic!("can't handle timeout {:?}", timeout)
        }
        true
    }

    #[inline]
    fn clock_ms(&mut self) -> u64 {
        if self.internal_clock_ms == 0 {
            self.internal_clock_ms = Self::get_clock_ms();
        }
        self.internal_clock_ms
    }

    #[inline]
    fn clock_s(&mut self) -> u32 {
        (self.clock_ms() / 1000) as u32
    }

    fn get_clock_ms() -> u64 {
        let time::Timespec{sec, nsec} = time::get_time();
        (sec as u64) * 1000 + (nsec / 1_000_000) as u64
    }

    fn get_clock_s() -> u32 {
        time::get_time().sec as u32
    }
}

impl Handler for ServerHandler {
    type Timeout = TimeoutType;
    type Message = NotifyType;

    #[inline]
    fn ready(&mut self, event_loop: &mut EventLoop<Self>, token: Token, events: EventSet) {
        trace!("events {:?} for token {:?}", events, token);
        let is_ok = match token {
            SERVER => self.server.ready(&mut self.clients, event_loop, events),
            token => if let Some(client) = self.clients.get_mut(token) {
                client.ready(&mut self.server, event_loop, events)
            } else {
                warn!("ready token {:?} not found", token);
                false
            }
        };
        if !is_ok {
            self.server.remove_client(token, &mut self.clients, event_loop);
        }
        trace!("done events {:?} for token {:?}", events, token);
    }

    #[inline]
    fn notify(&mut self, event_loop: &mut EventLoop<Self>, notification: Self::Message) {
        let token = notification.0.token();
        trace!("notify event for token {:?} with {:?}", token, notification.1);
        let is_ok = match token {
            SERVER => self.server.notify(&mut self.clients, event_loop, notification),
            token => if let Some(client) = self.clients.get_mut(token) {
                client.notify(&mut self.server, event_loop, notification)
            } else {
                warn!("notify token {:?} not found", token);
                false
            }
        };
        if !is_ok {
            self.server.remove_client(token, &mut self.clients, event_loop);
        }
        trace!("end notify event for token {:?}", token);
    }

    #[inline]
    fn tick(&mut self, event_loop: &mut EventLoop<Self>) {
        self.server.tick(&mut self.clients, event_loop);
    }

    #[inline]
    fn timeout(&mut self, event_loop: &mut EventLoop<Self>, timeout: Self::Timeout) {
        let token = timeout.0;
        trace!("timeout event for token {:?} with {:?}", token, timeout.1);
        let is_ok = match token {
            SERVER => self.server.timeout(event_loop, timeout.1),
            token => if let Some(client) = self.clients.get_mut(token) {
                client.timeout(&mut self.server, event_loop, timeout.1)
            } else {
                warn!("timeout token {:?} not found", token);
                false
            }
        };
        if !is_ok {
            self.server.remove_client(token, &mut self.clients, event_loop);
        }
        trace!("end timeout event for token {:?}", token);
    }

    #[inline]
    fn interrupted(&mut self, _: &mut EventLoop<Self>) {
        panic!("interrupted");
    }
}
