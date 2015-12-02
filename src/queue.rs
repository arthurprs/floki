use std::sync::{Mutex, RwLock};
use std::collections::{BTreeMap, BinaryHeap};
use std::collections::hash_map::Entry;
use std::io::{Read, Write};
use std::fs::{self, File};
use std::mem;
use std::rc::Rc;
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
    pub channels: BTreeMap<String, ChannelInfo>
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
pub struct Channel {
    last_touched: u32,
    expired_count: u32,
    tail: u64,
    // keeps track of the messages in flight (possibly expired) by their ticket in LRU order
    in_flight_map: LinkedHashMap<u64, InFlightState>,
    // keeps track of the smallest in flight ids and their tickets (possibly expired)
    in_flight_heap: BinaryHeap<(Rev<u64>, u64)>,
}

#[derive(Debug)]
pub struct Queue {
    config: Rc<QueueConfig>,
    lock: TristateLock<()>,
    backend: QueueBackend,
    channels: RwLock<HashMap<String, Mutex<Channel>>>,
    state: QueueState,
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

unsafe impl Sync for Queue {}
unsafe impl Send for Queue {}

impl Queue {
    pub fn new(config: QueueConfig, recover: bool) -> Queue {
        if ! recover {
            remove_dir_if_exist(&config.data_directory).unwrap();
        }
        create_dir_if_not_exist(&config.data_directory).unwrap();

        let rc_config = Rc::new(config);
        let mut queue = Queue {
            config: rc_config.clone(),
            lock: TristateLock::new(()),
            backend: QueueBackend::new(rc_config.clone(), recover),
            channels: RwLock::new(Default::default()),
            state: QueueState::Ready,
        };
        if recover {
           queue.recover();
        } else {
           queue.checkpoint(false);
        }
        queue
    }

    pub fn name(&self) -> &str {
        &self.config.name
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
        let r_lock = self.lock.read();
        let mut locked_channels = self.channels.write().unwrap();
        if let Entry::Vacant(vacant_entry) = locked_channels.entry(channel_name.into()) {
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
        let mut locked_channels = self.channels.write().unwrap();
        if locked_channels.remove(channel_name).is_some() {
            Ok(())
        } else {
            Err(QueueError::ChannelNotFound)
        }
    }

    pub fn purge_channel(&mut self, channel_name: &str) -> QueueResult<()> {
        let locked_channels = self.channels.write().unwrap();
        if let Some(channel) = locked_channels.get(channel_name) {
            let r_lock = self.lock.read();
            channel.lock().unwrap().purge(self.backend.head());
            Ok(())
        } else {
            Err(QueueError::ChannelNotFound)
        }
    }

    /// get access is suposed to be thread-safe, even while writing
    pub fn get(&mut self, channel_name: &str, clock: u32) -> QueueResult<(u64, Message)> {
        let r_lock = self.lock.read();
        let locked_channels = self.channels.read().unwrap();
        if let Some(channel) = locked_channels.get(channel_name) {
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
        let w_lock = self.lock.write();
        trace!("[{}] putting message w/ clock {}", self.config.name, clock);
        Ok(try!(self.backend.push(message, clock)))
    }

    /// all calls are serialized internally
    pub fn push_many(&mut self, messages: &[&[u8]], clock: u32) -> QueueResult<u64> {
        let w_lock = self.lock.write();
        trace!("[{}] putting {} messages w/ clock {}", self.config.name, messages.len(), clock);
        assert!(messages.len() > 0);
        for message in &messages[..messages.len() - 1] {
            try!(self.backend.push(message, clock));
        }
        Ok(try!(self.backend.push(messages[messages.len() - 1], clock)))
    }

    /// ack access is suposed to be thread-safe, even while writing
    pub fn ack(&mut self, channel_name: &str, ticket: u64, clock: u32) -> QueueResult<()> {
        let locked_channels = self.channels.read().unwrap();
        if let Some(channel) = locked_channels.get(channel_name) {
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
        let x_lock = self.lock.lock();
        self.backend.purge();
        for (_, channel) in self.channels.write().unwrap().iter_mut() {
            let mut locked_channel = channel.lock().unwrap();
            locked_channel.tail = self.backend.tail();
            locked_channel.in_flight_map.clear();
            locked_channel.expired_count = 0;
        }
        self.as_mut().checkpoint(false);
    }

    pub fn info(&self, clock: u32) -> QueueInfo {
        let r_lock = self.lock.read();
        let mut q_info = QueueInfo {
            tail: self.backend.tail(),
            head: self.backend.head(),
            channels: Default::default()
        };
        for (channel_name, channel) in self.channels.write().unwrap().iter() {
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
        let x_lock = self.lock.lock();
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
                let mut locked_channels = self.channels.write().unwrap();
                for (channel_name, channel_checkpoint) in queue_checkpoint.channels {
                    locked_channels.insert(
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
            let locked_channels = self.channels.read().unwrap();
            for (channel_name, channel) in locked_channels.iter() {
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
            self.channels.read().unwrap().values()
                .map(|c| c.lock().unwrap().real_tail())
                .min()
                .unwrap_or(0)
        };

        debug!("[{}] smallest_tail is {}", self.config.name, smallest_tail);

        let r_lock = self.lock.read();
        self.backend.gc(smallest_tail, clock);
        self.as_mut().checkpoint(false);
    }

    #[allow(mutable_transmutes)]
    pub fn as_mut(&self) -> &mut Self {
        unsafe { mem::transmute(self) }
    }
}

impl Drop for Queue {
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
        server_config.segment_size = 4 * 1024 * 1024;
        server_config.retention_period = 1;
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
        let mut q = get_queue();
        let message = gen_message();
        for i in 0..100_000 {
            assert!(q.push(&message, 0).is_ok());
        }
    }

    #[test]
    fn test_put_get() {
        let mut q = get_queue();
        let message = gen_message();
        assert!(q.create_channel("test", 0).is_ok());
        for i in 0..100_000 {
            assert!(q.push(&message, 0).is_ok());
            let r = q.get("test", 0);
            assert!(r.unwrap().1.body() == message);
        }
    }

    #[test]
    fn test_create_channel() {
        let mut q = get_queue();
        let message = gen_message();
        assert!(q.get("test", 0).is_err());
        assert!(q.push(&message, 0).is_ok());
        assert!(q.create_channel("test", 0).is_ok());
        assert_eq_repr!(q.create_channel("test", 0).unwrap_err(), QueueError::ChannelAlreadyExists);
        assert!(q.get("test", 0).is_err());
        assert!(q.push(&message, 0).is_ok());
        assert!(q.get("test", 0).is_ok());
    }

    #[test]
    fn test_in_flight() {
        let mut q = get_queue();
        assert!(q.create_channel("test", 0).is_ok());
        let message = gen_message();
        assert!(q.push(&message, 1).is_ok());
        assert!(q.get("test", 1).is_ok());
        assert!(q.get("test", 1).is_err());
        assert_eq!(q.info(1).channels["test"].in_flight_count, 1);
        assert_eq!(q.info(2).channels["test"].in_flight_count, 0);
    }

    #[test]
    fn test_in_flight_timeout() {
        let mut q = get_queue();
        let message = gen_message();
        assert!(q.create_channel("test", 0).is_ok());
        assert!(q.push(&message, 0).is_ok());
        assert!(q.get("test", 0).is_ok());
        assert!(q.get("test", 0).is_err());
        assert!(q.get("test", 1).is_ok());
    }

    #[test]
    fn test_backend_recover() {
        let mut q = get_queue();
        assert!(q.create_channel("test", 0).is_ok());
        let message = gen_message();
        let mut put_msg_count = 0;
        while q.backend.segments_count() < 3 {
            assert!(q.push(&message, 0).is_ok());
            put_msg_count += 1;
        }
        q.checkpoint(true);

        q = get_queue_recover();
        assert_eq_repr!(q.create_channel("test", 1).unwrap_err(), QueueError::ChannelAlreadyExists);
        assert_eq!(q.backend.segments_count(), 3);
        let mut get_msg_count = 0;
        while let Ok(_) = q.get("test", 0) {
            get_msg_count += 1;
        }
        assert_eq!(get_msg_count, put_msg_count);
    }

    #[test]
    fn test_queue_recover() {
        let mut q = get_queue();
        let message = gen_message();
        assert!(q.create_channel("test", 0).is_ok());
        assert!(q.push(&message, 0).is_ok());
        assert!(q.push(&message, 0).is_ok());
        assert!(q.get("test", 0).is_ok());
        assert!(q.get("test", 0).is_ok());
        assert!(q.get("test", 0).is_err());
        q.checkpoint(true);

        q = get_queue_recover();
        assert_eq_repr!(q.create_channel("test", 0).unwrap_err(), QueueError::ChannelAlreadyExists);
        assert!(q.get("test", 0).is_ok());
        assert!(q.get("test", 0).is_ok());
        assert!(q.get("test", 0).is_err());
    }

    #[test]
    fn test_gc() {
        let message = gen_message();
        let mut q = get_queue();
        assert!(q.create_channel("test", 2).is_ok());

        while q.backend.segments_count() < 3 {
            assert!(q.push(&message, 2).is_ok());
            let r = q.get("test", 2);
            assert!(r.is_ok());
            assert!(q.ack("test", r.unwrap().0, 2).is_ok());
        }

        // retention period will keep segments from beeing deleted
        q.maintenance(2);
        assert_eq!(q.backend.segments_count(), 3);
        // retention period expired, gc should get rid of the first two segments
        q.maintenance(3);
        assert_eq!(q.backend.segments_count(), 1);
    }

    #[bench]
    fn put_like_crazy(b: &mut test::Bencher) {
        let mut q = get_queue();
        let m = &gen_message();
        let n = 10000;
        b.bytes = (m.len() * n) as u64;
        b.iter(|| {
            for _ in 0..n {
                assert!(q.push(m, 0).is_ok());
            }
        });
    }

    #[bench]
    fn put_get_like_crazy(b: &mut test::Bencher) {
        let mut q = get_queue();
        let m = &gen_message();
        let n = 10000;
        q.create_channel("test", 0).unwrap();
        b.bytes = (m.len() * n) as u64;
        b.iter(|| {
            for _ in 0..n {
                let p = q.push(m, 0).unwrap();
                let (ticket, gm) = q.get("test", 0).unwrap();
                assert!(q.ack("test", ticket, 0).is_ok());
                assert_eq!(p, gm.id());
            }
        });
    }
}
