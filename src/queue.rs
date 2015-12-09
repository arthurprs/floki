use std::sync::Mutex;
use std::collections::{BTreeMap, BinaryHeap};
use std::collections::hash_map::Entry;
use std::io::{Read, Write};
use std::fs::{self, File};
use std::mem;
use rustc_serialize::json;

use config::*;
use queue_backend::*;
use utils::*;
use tristate_lock::*;
use rev::Rev;
use rand;

pub type QueueResult<T> = Result<T, QueueError>;

#[derive(Debug)]
pub enum QueueError {
    ChannelAlreadyExists,
    ChannelNotFound,
    TicketNotFound,
    EndOfQueue(u64),
    Backend(QueueBackendError),
}

impl From<QueueBackendError> for QueueError {
    fn from(from: QueueBackendError) -> QueueError {
        QueueError::Backend(from)
    }
}

#[derive(Eq, PartialEq, Debug, Copy, Clone, RustcDecodable, RustcEncodable)]
pub enum QueueState {
    Ready,
    Deleting
}

#[derive(Debug, PartialEq, RustcDecodable, RustcEncodable)]
pub struct ChannelInfo {
    pub tail: u64,
    pub last_touched: u32,
    pub in_flight_count: u32,
}

#[derive(Debug, RustcDecodable, RustcEncodable)]
pub struct QueueInfo {
    pub head: u64,
    pub tail: u64,
    pub channels: BTreeMap<String, ChannelInfo>,
    pub segments_count: u32,
}

#[derive(Debug, Eq, PartialEq, RustcDecodable, RustcEncodable)]
struct ChannelCheckpoint {
    tail: u64,
    last_touched: u32,
}

#[derive(Debug, RustcDecodable, RustcEncodable)]
struct QueueCheckpoint {
    state: QueueState,
    channels: BTreeMap<String, ChannelCheckpoint>,
}

const EXPIRED: u32 = 0;

#[derive(Debug)]
struct InFlightState {
    id: u64,
    expiration: u32,
}

#[derive(Debug)]
struct Channel {
    last_touched: u32,
    expired_count: u32,
    tail: u64,
    // keeps track of the messages in flight (possibly expired) by their ticket in LRU order
    in_flight_map: LinkedHashMap<u64, InFlightState>,
    // keeps track of the smallest in flight ids and their tickets (possibly expired)
    in_flight_heap: BinaryHeap<(Rev<u64>, u64)>,
}

#[derive(Debug)]
struct InnerQueue {
    config: QueueConfig,
    backend: QueueBackend,
    channels: HashMap<String, Mutex<Channel>>,
    state: QueueState,
}

pub struct Queue {
    name: String,
    inner: TristateLock<InnerQueue>,
    maintenance_mutex: Mutex<()>,
}

unsafe impl Send for Queue {}
unsafe impl Sync for Queue {}

impl Queue {
    pub fn new(config: QueueConfig, recover: bool) -> Queue {
        Queue{
            name: config.name.clone(),
            inner: TristateLock::new(InnerQueue::new(config, recover)),
            maintenance_mutex: Mutex::new(()),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn create_channel(&self, channel_name: &str, clock: u32) -> QueueResult<()> {
        self.inner.lock().create_channel(channel_name, clock)
    }

    pub fn delete_channel(&self, channel_name: &str) -> QueueResult<()> {
        self.inner.lock().delete_channel(channel_name)
    }

    pub fn purge_channel(&self, channel_name: &str) -> QueueResult<()> {
        self.inner.read().purge_channel(channel_name)
    }

    /// get access is suposed to be thread-safe, even while writing
    pub fn get(&self, channel_name: &str, clock: u32) -> QueueResult<(u64, Message)> {
        self.inner.read().get(channel_name, clock)
    }

    /// all calls are serialized internally
    pub fn push(&self, message: &[u8], clock: u32) -> QueueResult<u64> {
        self.inner.write().push(message, clock)
    }

    /// all calls are serialized internally
    pub fn push_many(&self, messages: &[&[u8]], clock: u32) -> QueueResult<u64> {
        self.inner.write().push_many(messages, clock)
    }

    /// ack access is suposed to be thread-safe, even while writing
    pub fn ack(&self, channel_name: &str, ticket: u64, clock: u32) -> QueueResult<()> {
        self.inner.read().ack(channel_name, ticket, clock)
    }

    pub fn purge(&self) {
        self.inner.lock().purge()
    }

    pub fn info(&self, clock: u32) -> QueueInfo {
        self.inner.read().info(clock)
    }

    pub fn delete(&self) {
        self.inner.lock().delete()
    }

    #[allow(mutable_transmutes)]
    pub fn checkpoint(&self, full: bool) {
        let maintenance_lock = self.maintenance_mutex.lock().unwrap();
        let inner: &mut InnerQueue = unsafe { mem::transmute(&*self.inner.read()) };
        inner.checkpoint(full);
        drop(maintenance_lock)
    }

    #[allow(mutable_transmutes)]
    pub fn maintenance(&self, clock: u32) {
        let maintenance_lock = self.maintenance_mutex.lock().unwrap();
        let inner: &mut InnerQueue = unsafe { mem::transmute(&*self.inner.read()) };
        inner.maintenance(clock);
        drop(maintenance_lock);
    }
}

impl Channel {
    fn real_tail(&self) -> u64 {
        if let Some(&(Rev(tail), _)) = self.in_flight_heap.peek() {
            debug_assert!(tail < self.tail);
            tail
        } else {
            self.tail
        }
    }

    fn in_flight_count(&mut self, clock: u32) -> u32 {
        // first, adjust expired_count accordingly
        // FIXME: may be expensive
        for (_, state) in self.in_flight_map.iter_mut().skip(self.expired_count as usize) {
            if clock >= state.expiration {
                self.expired_count += 1;
                state.expiration = EXPIRED;
            } else {
                break
            }
        }

        self.in_flight_map.len() as u32 - self.expired_count
    }

    fn purge(&mut self, new_tail: u64) {
        self.in_flight_heap.clear();
        self.in_flight_map.clear();
        self.expired_count = 0;
        self.tail = new_tail;
    }
}

impl InnerQueue {
    pub fn new(config: QueueConfig, recover: bool) -> InnerQueue {
        if ! recover {
            remove_dir_if_exist(&config.data_directory).unwrap();
        }
        create_dir_if_not_exist(&config.data_directory).unwrap();

        let mut queue = InnerQueue {
            config: config.clone(),
            backend: QueueBackend::new(config, recover),
            channels: Default::default(),
            state: QueueState::Ready,
        };
        if recover {
           queue.recover();
        } else {
           queue.checkpoint(false);
        }
        queue
    }

    fn set_state(&mut self, new_state: QueueState) {
        if self.state == new_state {
            return
        }
        match self.state {
            QueueState::Deleting => panic!("Deleting can't be reverted"),
            QueueState::Ready => (),
        }
        self.state = new_state;
    }

    pub fn create_channel(&mut self, channel_name: &str, clock: u32) -> QueueResult<()> {
        if let Entry::Vacant(vacant_entry) = self.channels.entry(channel_name.into()) {
            let channel = Channel {
                last_touched: clock,
                expired_count: 0,
                tail: self.backend.head(),
                in_flight_map: Default::default(),
                in_flight_heap: Default::default(),
            };
            debug!("[{}] creating channel {:?}", self.config.name, channel);
            vacant_entry.insert(Mutex::new(channel));
            Ok(())
        } else {
            Err(QueueError::ChannelAlreadyExists)
        }
    }

    pub fn delete_channel(&mut self, channel_name: &str) -> QueueResult<()> {
        if self.channels.remove(channel_name).is_some() {
            Ok(())
        } else {
            Err(QueueError::ChannelNotFound)
        }
    }

    pub fn purge_channel(&self, channel_name: &str) -> QueueResult<()> {
        if let Some(channel) = self.channels.get(channel_name) {
            channel.lock().unwrap().purge(self.backend.head());
            Ok(())
        } else {
            Err(QueueError::ChannelNotFound)
        }
    }

    /// get access is suposed to be thread-safe, even while writing
    pub fn get(&self, channel_name: &str, clock: u32) -> QueueResult<(u64, Message)> {
        if let Some(channel) = self.channels.get(channel_name) {
            let mut locked_channel = channel.lock().unwrap();

            locked_channel.last_touched = clock;

            if let Some((&ticket, &InFlightState{expiration, ..})) = locked_channel.in_flight_map.front() {
                // check in flight queue for timeouts
                if clock >= expiration {
                    if expiration == EXPIRED {
                        locked_channel.expired_count -= 1;
                    }
                    let mut state = locked_channel.in_flight_map.remove(&ticket).unwrap();
                    let id = state.id;
                    let ticket = rand::random::<u64>();
                    state.expiration = clock + self.config.message_timeout;
                    locked_channel.in_flight_map.insert(ticket, state);
                    locked_channel.in_flight_heap.push((Rev(id), ticket));
                    debug!("[{}:{}] msg {} expired and will be sent again as ticket {}",
                        self.config.name, channel_name, id, ticket);
                    return Ok((ticket, self.backend.get(id).unwrap()))
                }
            }

            // fetch from the backend
            return if let Some(message) = self.backend.get(locked_channel.tail) {
                let ticket = rand::random::<u64>();
                let id = message.id();
                let state = InFlightState {
                    id: id,
                    expiration: clock + self.config.message_timeout,
                };
                locked_channel.in_flight_map.insert(ticket, state);
                locked_channel.in_flight_heap.push((Rev(id), ticket));
                locked_channel.tail = id + 1;
                debug!("[{}:{}] fetched msg {} from backend as ticket {}",
                    self.config.name, channel_name, message.id(), ticket);
                trace!("[{}:{}] advancing tail to {}",
                    self.config.name, channel_name, locked_channel.tail);
                Ok((ticket, message))
            } else {
                debug!("[{}:{}] no more messages", self.config.name, channel_name);
                Err(QueueError::EndOfQueue(locked_channel.tail))
            }
        }
        Err(QueueError::ChannelNotFound)
    }

    /// all calls are serialized internally
    pub fn push(&mut self, message: &[u8], clock: u32) -> QueueResult<u64> {
        trace!("[{}] putting message w/ clock {}", self.config.name, clock);
        Ok(try!(self.backend.push(message, clock)))
    }

    /// all calls are serialized internally
    pub fn push_many(&mut self, messages: &[&[u8]], clock: u32) -> QueueResult<u64> {
        trace!("[{}] putting {} messages w/ clock {}", self.config.name, messages.len(), clock);
        assert!(messages.len() > 0);
        for message in &messages[..messages.len() - 1] {
            try!(self.backend.push(message, clock));
        }
        Ok(try!(self.backend.push(messages[messages.len() - 1], clock)))
    }

    /// ack access is suposed to be thread-safe, even while writing
    pub fn ack(&self, channel_name: &str, ticket: u64, clock: u32) -> QueueResult<()> {
        if let Some(channel) = self.channels.get(channel_name) {
            let mut locked_channel = channel.lock().unwrap();
            locked_channel.last_touched = clock;

            // try to remove the ticket if not expired
            // TODO: success rate should be much higher, so remove and re-add if needed
            match locked_channel.in_flight_map.get(&ticket) {
                Some(state) if clock < state.expiration => (),
                _ => return Err(QueueError::TicketNotFound),
            };

            let state = locked_channel.in_flight_map.remove(&ticket).unwrap();
            trace!("[{}:{}] message {} ticket {} deleted from channel",
                self.config.name, channel_name, state.id, ticket);
            // advance channel real tail
            while locked_channel.in_flight_heap
                    .peek()
                    .map_or(false, |&(_, ticket)| !locked_channel.in_flight_map.contains_key(&ticket)) {
                locked_channel.in_flight_heap.pop();
            }
            Ok(())
        } else {
            Err(QueueError::ChannelNotFound)
        }
    }

    pub fn purge(&mut self) {
        info!("[{}] purging", self.config.name);
        self.backend.purge();
        for (_, channel) in &mut self.channels {
            let mut locked_channel = channel.lock().unwrap();
            locked_channel.tail = self.backend.tail();
            locked_channel.in_flight_map.clear();
            locked_channel.expired_count = 0;
        }
        self.as_mut().checkpoint(false);
    }

    pub fn info(&self, clock: u32) -> QueueInfo {
        let mut q_info = QueueInfo {
            tail: self.backend.tail(),
            head: self.backend.head(),
            channels: Default::default(),
            segments_count: self.backend.segments_count() as u32,
        };
        for (channel_name, channel) in &self.channels {
            let mut locked_channel = channel.lock().unwrap();
            q_info.channels.insert(channel_name.clone(), ChannelInfo{
                last_touched: locked_channel.last_touched,
                in_flight_count: locked_channel.in_flight_count(clock),
                tail: locked_channel.tail,
            });
        }

        q_info
    }

    pub fn delete(&mut self) {
        info!("[{}] deleting", self.config.name);
        self.as_mut().set_state(QueueState::Deleting);
        self.as_mut().checkpoint(false);
        self.backend.delete();
        remove_dir_if_exist(&self.config.data_directory).unwrap();
    }

    fn recover(&mut self) {
        let path = self.config.data_directory.join(QUEUE_CHECKPOINT_FILE);
        let queue_checkpoint: QueueCheckpoint = match File::open(path) {
            Ok(mut file) => {
                let mut contents = String::new();
                let _ = file.read_to_string(&mut contents);
                let checkpoint_result = json::decode(&contents);
                match checkpoint_result {
                    Ok(state) => state,
                    Err(error) => {
                        error!("[{}] error parsing checkpoint information: {}",
                            self.config.name, error);
                        return;
                    }
                }
            }
            Err(error) => {
                warn!("[{}] error reading checkpoint information: {}",
                    self.config.name, error);
                return;
            }
        };

        info!("[{}] checkpoint loaded: {:?}", self.config.name, queue_checkpoint.state);

        self.state = queue_checkpoint.state;

        match self.state {
            QueueState::Ready => {
                for (channel_name, channel_checkpoint) in queue_checkpoint.channels {
                    self.channels.insert(
                        channel_name,
                        Mutex::new(Channel {
                            last_touched: channel_checkpoint.last_touched,
                            expired_count: 0,
                            tail: channel_checkpoint.tail,
                            in_flight_map: Default::default(),
                            in_flight_heap: Default::default()
                        })
                    );
                }
            }
            QueueState::Deleting => {
                // TODO: return some sort of error
            }
        }
    }

    fn checkpoint(&mut self, full: bool) {
        let mut checkpoint = QueueCheckpoint {
            state: self.state,
            channels: Default::default()
        };

        if self.state == QueueState::Ready {
            self.backend.checkpoint(full);
            for (channel_name, channel) in &self.channels {
                let locked_channel = channel.lock().unwrap();
                checkpoint.channels.insert(
                    channel_name.clone(),
                    ChannelCheckpoint {
                        last_touched: locked_channel.last_touched,
                        tail: locked_channel.real_tail(),
                    }
                );
            }
        }

        let tmp_path = self.config.data_directory.join(TMP_QUEUE_CHECKPOINT_FILE);
        let result = File::create(&tmp_path)
            .and_then(|mut file| {
                write!(file, "{}", json::as_pretty_json(&checkpoint)).unwrap();
                file.sync_data()
            })
            .and_then(|_| {
                let final_path = tmp_path.with_file_name(QUEUE_CHECKPOINT_FILE);
                fs::rename(tmp_path, final_path)
            });

        match result {
            Ok(_) => info!("[{}] checkpointed: {:?}", self.config.name, checkpoint.state),
            Err(error) =>
                warn!("[{}] error writing checkpoint information: {}",
                    self.config.name, error)
        }
    }

    pub fn maintenance(&mut self, clock: u32) {
        let smallest_tail = {
            self.channels.values()
                .map(|c| c.lock().unwrap().real_tail())
                .min()
                .unwrap_or(0)
        };

        debug!("[{}] smallest_tail is {}", self.config.name, smallest_tail);

        self.backend.gc(smallest_tail, clock);
        self.as_mut().checkpoint(false);
    }

    #[allow(mutable_transmutes)]
    pub fn as_mut(&self) -> &mut Self {
        unsafe { mem::transmute(self) }
    }
}

impl Drop for InnerQueue {
    fn drop(&mut self) {
        if self.state != QueueState::Deleting {
            self.checkpoint(true)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use config::*;
    use std::thread;
    use test;

    fn get_queue_opt(name: &str, recover: bool) -> Queue {
        let mut server_config = ServerConfig::read();
        server_config.data_directory = "./test_data".into();
        server_config.default_queue_config.segment_size = 4 * 1024 * 1024;
        server_config.default_queue_config.retention_period = 1;
        server_config.default_queue_config.hard_retention_period = 2;
        let mut queue_config = server_config.new_queue_config(name);
        queue_config.message_timeout = 1;
        Queue::new(queue_config, recover)
    }

    fn get_queue() -> Queue {
        get_queue_opt(thread::current().name().unwrap(), false)
    }

    fn get_queue_recover() -> Queue {
        get_queue_opt(thread::current().name().unwrap(), true)
    }

    fn gen_message() -> &'static [u8] {
        return b"333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333";
    }

    #[test]
    fn test_fill() {
        let q = get_queue();
        for _ in 0..100_000 {
            q.push(gen_message(), 0).unwrap();
        }
    }

    #[test]
    fn test_put_get() {
        let q = get_queue();
        let message = gen_message();
        q.create_channel("test", 0).unwrap();
        for _ in 0..100_000 {
            q.push(&message, 0).unwrap();
            let r = q.get("test", 0);
            assert!(r.unwrap().1.body() == message);
        }
    }

    #[test]
    fn test_create_channel() {
        let q = get_queue();
        q.get("test", 0).unwrap_err();
        q.push(gen_message(), 0).unwrap();
        q.create_channel("test", 0).unwrap();
        assert_eq_repr!(q.create_channel("test", 0).unwrap_err(), QueueError::ChannelAlreadyExists);
        q.get("test", 0).unwrap_err();
        q.push(gen_message(), 0).unwrap();
        q.get("test", 0).unwrap();
    }

    #[test]
    fn test_in_flight() {
        let q = get_queue();
        q.create_channel("test", 0).unwrap();
        q.push(gen_message(), 1).unwrap();
        q.get("test", 1).unwrap();
        q.get("test", 1).unwrap_err();
        assert_eq!(q.info(1).channels["test"].in_flight_count, 1);
        assert_eq!(q.info(2).channels["test"].in_flight_count, 0);
    }

    #[test]
    fn test_in_flight_timeout() {
        let q = get_queue();
        q.create_channel("test", 0).unwrap();
        q.push(gen_message(), 0).unwrap();
        q.get("test", 0).unwrap();
        q.get("test", 0).unwrap_err();
        q.get("test", 1).unwrap();
    }

    #[test]
    fn test_backend_recover() {
        let mut q = get_queue();
        q.create_channel("test", 0).unwrap();
        let mut put_msg_count = 0;
        while q.info(0).segments_count < 3 {
            q.push(gen_message(), 0).unwrap();
            put_msg_count += 1;
        }
        q.checkpoint(true);

        q = get_queue_recover();
        assert_eq_repr!(q.create_channel("test", 1).unwrap_err(), QueueError::ChannelAlreadyExists);
        assert_eq!(q.info(0).segments_count, 3);
        let mut get_msg_count = 0;
        while let Ok(_) = q.get("test", 0) {
            get_msg_count += 1;
        }
        assert_eq!(get_msg_count, put_msg_count);
    }

    #[test]
    fn test_queue_recover() {
        let mut q = get_queue();
        q.create_channel("test", 0).unwrap();
        q.push(gen_message(), 0).unwrap();
        q.push(gen_message(), 0).unwrap();
        q.get("test", 0).unwrap();
        q.get("test", 0).unwrap();
        q.get("test", 0).unwrap_err();
        q.checkpoint(true);

        q = get_queue_recover();
        assert_eq_repr!(q.create_channel("test", 0).unwrap_err(), QueueError::ChannelAlreadyExists);
        q.get("test", 0).unwrap();
        q.get("test", 0).unwrap();
        q.get("test", 0).unwrap_err();
    }

    #[test]
    fn test_maintenance() {
        let q = get_queue();
        q.create_channel("test", 1).unwrap();

        while q.info(1).segments_count < 3 {
            q.push(gen_message(), 1).unwrap();
            let (ticket, _) = q.get("test", 1).unwrap();
            q.ack("test", ticket, 1).unwrap();
        }

        // retention period will keep segments from beeing deleted
        q.maintenance(2);
        assert_eq!(q.info(2).segments_count, 3);
        // retention period expired, gc should get rid of the first two segment
        q.maintenance(3);
        assert_eq!(q.info(3).segments_count, 1);
    }

    #[bench]
    fn put_like_crazy(b: &mut test::Bencher) {
        let q = get_queue();
        let m = gen_message();
        let n = 10000;
        b.bytes = (m.len() * n) as u64;
        b.iter(|| {
            for _ in 0..n {
                q.push(m, 0).unwrap();
            }
        });
    }

    #[bench]
    fn put_get_like_crazy(b: &mut test::Bencher) {
        let q = get_queue();
        let m = &gen_message();
        let n = 10000;
        q.create_channel("test", 0).unwrap();
        b.bytes = (m.len() * n) as u64;
        b.iter(|| {
            for _ in 0..n {
                let p = q.push(m, 0).unwrap();
                let (ticket, gm) = q.get("test", 0).unwrap();
                q.ack("test", ticket, 0).unwrap();
                assert_eq!(p, gm.id());
            }
        });
    }
}
