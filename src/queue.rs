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
use rev::Rev;

#[derive(Eq, PartialEq, Debug, Copy, Clone, RustcDecodable, RustcEncodable)]
pub enum QueueState {
    Ready,
    Purging,
    Deleting
}

impl Default for QueueState {
    fn default() -> Self {
        QueueState::Ready
    }
}

pub struct ChannelInfo {
    pub tail: u64,
}

pub struct QueueInfo {
    pub head: u64,
    pub channels: BTreeMap<String, ChannelInfo>
}

#[derive(Debug, Eq, PartialEq, RustcDecodable, RustcEncodable)]
struct ChannelCheckpoint {
    last_touched: u32,
    tail: u64,
}

#[derive(Debug, Default, RustcDecodable, RustcEncodable)]
struct QueueCheckpoint {
    state: QueueState,
    channels: BTreeMap<String, ChannelCheckpoint>,
}

#[derive(Debug, Default)]
struct InFlightState {
    expiration: u32,
    retry: u32,
}

#[derive(Debug)]
pub struct Channel {
    last_touched: u32,
    tail: u64,
    // keeps track of the IDs in flight by id and FIFO order
    in_flight: LinkedHashMap<u64, InFlightState>,
    // keeps track of the smallest id in flight
    in_flight_heap: BinaryHeap<Rev<u64>>,
}

#[derive(Debug)]
pub struct Queue {
    config: Rc<QueueConfig>,
    // backend writes don't block readers
    w_lock: Mutex<()>, // write lock
    r_lock: RwLock<()>, // read lock
    backend: QueueBackend,
    channels: RwLock<HashMap<String, Mutex<Channel>>>,
    state: QueueState,
}

impl Channel {
    fn real_tail(&self) -> u64 {
        if let Some(&Rev(tail)) = self.in_flight_heap.peek() {
            debug_assert!(tail < self.tail);
            tail
        } else {
            self.tail
        }
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
            w_lock: Mutex::new(()),
            r_lock: RwLock::new(()),
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
            QueueState::Purging => match new_state {
                QueueState::Ready => (),
                other => panic!("Can't go from {:?} to {:?}", self.state, new_state),
            },
            QueueState::Ready => (),
        }
        self.state = new_state;
    }

    pub fn create_channel<S>(&mut self, channel_name: S, clock: u32) -> bool
            where String: From<S> {
        let channel_name: String = channel_name.into();
        let r_lock = self.r_lock.read().unwrap();
        let mut locked_channel = self.channels.write().unwrap();
        if let Entry::Vacant(vacant_entry) = locked_channel.entry(channel_name) {
            let channel = Channel {
                last_touched: clock,
                tail: self.backend.tail(), // should probably be the head instead
                in_flight: Default::default(),
                in_flight_heap: Default::default(),
            };
            debug!("creating channel {:?}", channel);
            vacant_entry.insert(Mutex::new(channel));
            true
        } else {
            false
        }
    }

    pub fn delete_channel(&mut self, channel_name: &str) -> bool {
        let mut locked_channel = self.channels.write().unwrap();
        locked_channel.remove(channel_name).is_some()
    }

    /// get access is suposed to be thread-safe, even while writing
    pub fn get(&mut self, channel_name: &str, clock: u32) -> Option<Result<Message, u64>> {
        let r_lock = self.r_lock.read().unwrap();
        let locked_channels = self.channels.read().unwrap();
        if let Some(channel) = locked_channels.get(channel_name) {
            let mut locked_channel = channel.lock().unwrap();

            locked_channel.last_touched = clock;

            // check in flight queue for timeouts
            if let Some((&id, &InFlightState { expiration, ..} )) = locked_channel.in_flight.front() {
                if clock >= expiration {
                    // FIXME: double get bellow, not ideal
                    let state = locked_channel.in_flight.get_refresh(&id).unwrap();
                    state.expiration = clock + self.config.time_to_live;
                    state.retry += 1;
                    debug!("[{}:{}] msg {} expired and will be sent again",
                        self.config.name, channel_name, id);
                    return Some(Ok(self.backend.get(id).unwrap()))
                }
            }

            // fetch from the backend
            if let Some(message) = self.backend.get(locked_channel.tail) {
                debug!("[{}:{}] fetched msg {} from backend", self.config.name, channel_name, message.id());
                let state = InFlightState {
                    expiration: clock + self.config.time_to_live,
                    retry: 0
                };
                locked_channel.in_flight.insert(message.id(), state);
                locked_channel.in_flight_heap.push(Rev(message.id()));
                locked_channel.tail = message.id() + 1;
                trace!("[{}:{}] advancing tail to {}", self.config.name, channel_name, locked_channel.tail);
                return Some(Ok(message))
            }
            return Some(Err(locked_channel.tail))
        }
        None
    }

    /// all calls are serialized internally
    pub fn push(&mut self, message: &[u8], clock: u32) -> Option<u64> {
        let w_lock = self.w_lock.lock().unwrap();
        trace!("[{}] putting message", self.config.name);
        self.backend.push(message, clock)
    }

    /// ack access is suposed to be thread-safe, even while writing
    pub fn ack(&mut self, channel_name: &str, id: u64, clock: u32) -> Option<bool> {
        let locked_channels = self.channels.read().unwrap();
        if let Some(channel) = locked_channels.get(channel_name) {
            let mut locked_channel = channel.lock().unwrap();
            locked_channel.last_touched = clock;
            // try to remove the id
            let removed_opt = locked_channel.in_flight.remove(&id);
            trace!("[{}:{}] message {} deleted from channel: {}",
                self.config.name, channel_name, id, removed_opt.is_some());
            // advance channel real tail
            while locked_channel.in_flight_heap
                    .peek()
                    .map_or(false, |&Rev(id)| !locked_channel.in_flight.contains_key(&id)) {
                locked_channel.in_flight_heap.pop();
            }
            return Some(removed_opt.is_some())
        }
        None
    }

    // TODO: NACK
    // pub fn nack(arg: Type) -> RetType {
    //     // add code here
    // }

    pub fn purge(&mut self) {
        // TODO:
        // we should probably just advance the tails and let GC take care of any cleaning
        // it's much faster and won't block incoming operations for long
        info!("[{}] purging", self.config.name);
        let r_lock = self.r_lock.write().unwrap();
        let w_lock = self.w_lock.lock().unwrap();
        self.as_mut().set_state(QueueState::Purging);
        self.as_mut().checkpoint(false);
        self.backend.purge();
        for (_, channel) in &mut*self.channels.write().unwrap() {
            let mut locked_channel = channel.lock().unwrap();
            locked_channel.tail = 1;
            locked_channel.in_flight.clear();
        }
        self.as_mut().set_state(QueueState::Ready);
        self.as_mut().checkpoint(false);
    }

    pub fn info(&self) -> QueueInfo {
        let r_lock = self.r_lock.write().unwrap();
        let mut q_info = QueueInfo {
            head: 1,
            channels: Default::default()
        };
        for (channel_name, channel) in &*self.channels.write().unwrap() {
            let locked_channel = channel.lock().unwrap();
            q_info.channels.insert(channel_name.clone(), ChannelInfo{
                tail: locked_channel.tail
            });
        }

        q_info
    }

    pub fn delete(&mut self) {
        info!("[{}] deleting", self.config.name);
        let r_lock = self.r_lock.write().unwrap();
        let w_lock = self.w_lock.lock().unwrap();
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
                            tail: channel_checkpoint.tail,
                            in_flight: Default::default(),
                            in_flight_heap: Default::default()
                        })
                    );
                }
            }
            QueueState::Purging => {
                // TODO: resume purging
            }
            QueueState::Deleting => {
                // TODO: return some sort of error
            }
        }
    }

    fn checkpoint(&mut self, full: bool) {
        let mut checkpoint = QueueCheckpoint {
            state: self.state,
            .. Default::default()
        };

        if self.state == QueueState::Ready {
            self.backend.checkpoint(full);
            let locked_channels = self.channels.read().unwrap();
            for (channel_name, channel) in &*locked_channels {
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

    pub fn maintenance(&mut self) {
        let smallest_tail = {
            self.channels.read().unwrap().values()
                .map(|c| c.lock().unwrap().real_tail())
                .min()
                .unwrap_or(0)
        };

        debug!("[{}] smallest_tail is {}", self.config.name, smallest_tail);

        let r_lock = self.r_lock.read();
        self.backend.gc(smallest_tail);
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
        let mut queue_config = server_config.new_queue_config(name);
        queue_config.time_to_live = 1;
        Queue::new(queue_config, recover)
    }

    fn get_queue() -> Queue {
        let thread = thread::current();
        let name = thread.name().unwrap().split("::").last().unwrap();
        get_queue_opt(name, false)
    }

    fn gen_message(id: u64) -> &'static [u8] {
        return b"333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333";
    }

    #[test]
    fn test_fill() {
        let mut q = get_queue();
        let message = gen_message(0);
        for i in (0..100_000) {
            let r = q.push(&message, 0);
            assert!(r.is_some());
        }
    }

    #[test]
    fn test_put_get() {
        let mut q = get_queue();
        let message = gen_message(0);
        assert!(q.create_channel("test", 0));
        for i in (0..100_000) {
            assert!(q.push(&message, 0).is_some());
            let m = q.get("test", 0);
            assert!(m.is_some());
        }
    }

    #[test]
    fn test_create_channel() {
        let mut q = get_queue();
        let message = gen_message(0);
        assert!(q.get("test", 0).is_none());
        assert!(q.push(&message, 0).is_some());
        assert!(q.create_channel("test", 0) == true);
        assert!(q.create_channel("test", 0) == false);
        assert!(q.get("test", 0).is_some());
    }

    #[test]
    fn test_in_flight() {
        let mut q = get_queue();
        let message = gen_message(0);
        assert!(q.push(&message, 0).is_some());
        assert!(q.get("test", 0).is_none());
        assert!(q.create_channel("test", 0) == true);
        assert!(q.create_channel("test", 0) == false);
        assert!(q.get("test", 0).unwrap().is_ok());
        assert!(q.get("test", 0).unwrap().is_err());
        // TODO: check in flight count
    }

    #[test]
    fn test_in_flight_timeout() {
        let mut q = get_queue();
        let message = gen_message(0);
        assert!(q.create_channel("test", 0) == true);
        assert!(q.push(&message, 0).is_some());
        assert!(q.get("test", 0).unwrap().is_ok());
        assert!(q.get("test", 0).unwrap().is_err());
        assert!(q.get("test", 1).unwrap().is_ok());
    }

    #[test]
    fn test_backend_recover() {
        let mut q = get_queue_opt("test_backend_recover", false);
        let message = gen_message(0);
        let mut put_msg_count = 0;
        while q.backend.segments_count() < 3 {
            assert!(q.push(&message, 0).is_some());
            put_msg_count += 1;
        }
        q.backend.checkpoint(true);

        q = get_queue_opt("test_backend_recover", true);
        assert_eq!(q.backend.segments_count(), 3);
        let mut get_msg_count = 0;
        assert!(q.create_channel("test", 0) == true);
        while let Some(Ok(_)) = q.get("test", 0) {
            get_msg_count += 1;
        }
        assert_eq!(get_msg_count, put_msg_count);
    }

    #[test]
    fn test_queue_recover() {
        let mut q = get_queue_opt("test_queue_recover", false);
        let message = gen_message(0);
        assert!(q.create_channel("test", 0) == true);
        assert!(q.push(&message, 0).is_some());
        assert!(q.push(&message, 0).is_some());
        assert!(q.get("test", 0).unwrap().is_ok());
        assert!(q.get("test", 0).unwrap().is_ok());
        assert!(q.get("test", 0).unwrap().is_err());
        q.checkpoint(true);

        println!("{:#?}", &q);

        q = get_queue_opt("test_queue_recover", true);

        println!("{:#?}", &q);
        assert!(q.create_channel("test", 0) == false);
        assert!(q.get("test", 0).unwrap().is_ok());
        assert!(q.get("test", 0).unwrap().is_ok());
        assert!(q.get("test", 0).unwrap().is_err());
    }

    #[test]
    fn test_gc() {
        let message = gen_message(0);
        let mut q = get_queue_opt("test_gc", false);
        assert!(q.create_channel("test", 0) == true);

        while q.backend.segments_count() < 3 {
            assert!(q.push(&message, 0).is_some());
            let get_result = q.get("test", 0);
            assert!(get_result.as_ref().unwrap().is_ok());
            assert!(q.ack("test", get_result.unwrap().unwrap().id(), 0).unwrap());
        }
        q.maintenance();

        // gc should get rid of the first two segments
        assert_eq!(q.backend.segments_count(), 1);
    }

    #[bench]
    fn put_like_crazy(b: &mut test::Bencher) {
        let mut q = get_queue();
        let m = &gen_message(0);
        let n = 10000;
        b.bytes = (m.len() * n) as u64;
        b.iter(|| {
            for _ in (0..n) {
                let r = q.push(m, 0);
                assert!(r.is_some());
            }
        });
    }

    #[bench]
    fn put_get_like_crazy(b: &mut test::Bencher) {
        let mut q = get_queue();
        let m = &gen_message(0);
        let n = 10000;
        q.create_channel("test", 0);
        b.bytes = (m.len() * n) as u64;
        b.iter(|| {
            for _ in (0..n) {
                let p = q.push(m, 0).unwrap();
                let r = q.get("test", 0).unwrap().unwrap().id();
                q.ack("test", r, 0);
                assert_eq!(p, r);
            }
        });
    }
}
