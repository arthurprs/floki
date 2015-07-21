use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex, RwLock};
use std::collections::{HashMap, BTreeMap};
use std::collections::hash_map::Entry;
use std::collections::hash_state::DefaultState;
use fnv::FnvHasher;
use std::io::{self, Read, Write};
use std::fs::{self, File};
use std::mem;
use std::cmp;
use std::rc::Rc;
use linked_hash_map::LinkedHashMap;
use time::precise_time_s;
use rustc_serialize::json;

use config::*;
use queue_backend::*;

#[derive(Eq, PartialEq, Debug, Copy, Clone, RustcDecodable, RustcEncodable)]
pub enum QueueState {
    Ready,
    Deleting
}

impl Default for QueueState {
    fn default() -> Self {
        QueueState::Ready
    }
}

#[derive(Debug, Eq, PartialEq, RustcDecodable, RustcEncodable)]
struct ChannelCheckpoint {
    tail: u64,
    in_flight: Vec<u64>
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
    tail: u64,
    in_flight: LinkedHashMap<u64, InFlightState, DefaultState<FnvHasher>>
}

//#[derive(Debug)]
pub struct Queue {
    config: Rc<QueueConfig>,
    // backend writes don't block readers
    // FIXME: queue_backend should handle it's concurrency access internally,
    // both for simplicity and performance
    backend_wlock: Mutex<()>,
    backend_rlock: RwLock<()>,
    backend: QueueBackend,
    channels: RwLock<HashMap<String, Mutex<Channel>, DefaultState<FnvHasher>>>,
    clock: u32, // local copy of the internal clock
    state: QueueState,
}

unsafe impl Sync for Queue {}
unsafe impl Send for Queue {}

impl Queue {

    pub fn new(config: QueueConfig, recover: bool) -> Queue {
        if ! recover {
            let _ = fs::remove_dir_all(&config.data_directory);
        }
        match fs::create_dir_all(&config.data_directory) {
            Err(ref err) if err.kind() != io::ErrorKind::AlreadyExists => {
                panic!("failed to open queue directory: {}", err);
            }
            _ => ()
        }

        let rc_config = Rc::new(config);
        let mut queue = Queue {
            config: rc_config.clone(),
            backend_wlock: Mutex::new(()),
            backend_rlock: RwLock::new(()),
            backend: QueueBackend::new(rc_config.clone(), recover),
            channels: RwLock::new(Default::default()),
            clock: 0,
            state: QueueState::Ready,
        };
        if recover {
            queue.recover();
        }
        queue.tick();
        queue
    }

    pub fn set_state(&mut self, new_state: QueueState) {
        self.state = new_state;
        self.checkpoint()
    }

    pub fn create_channel<S>(&mut self, channel_name: S) -> bool
            where String: From<S> {
        let channel_name: String = channel_name.into();
        let mut locked_channel = self.channels.write().unwrap();
        if let Entry::Vacant(vacant_entry) = locked_channel.entry(channel_name) {
            vacant_entry.insert(
                Mutex::new(
                    Channel {
                        tail: 0,
                        in_flight: Default::default()
                    }
                )
            );
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
    pub fn get(&mut self, channel_name: &str) -> Option<Message> {
        let _ = self.backend_rlock.read().unwrap();
        let locked_channels = self.channels.read().unwrap();
        if let Some(channel) = locked_channels.get(channel_name) {
            let mut locked_channel = channel.lock().unwrap();

            // check in flight queue for timeouts
            if let Some((&id, &InFlightState { expiration, ..} )) = locked_channel.in_flight.front() {
                if self.clock >= expiration {
                    // FIXME: double get bellow, not ideal
                    let state = locked_channel.in_flight.get_refresh(&id).unwrap();
                    state.expiration = self.clock + self.config.time_to_live;
                    state.retry += 1;
                    trace!("[{}] msg {} expired and will be sent again", self.config.name, id);
                    return Some(self.backend.get(id).unwrap().1)
                }
            }

            // fetch from the backend
            if let Some((new_tail, message)) = self.backend.get(locked_channel.tail) {
                locked_channel.tail = new_tail;
                let state = InFlightState {
                    expiration: self.clock + self.config.time_to_live,
                    retry: 0
                };
                locked_channel.in_flight.insert(message.id, state);
                trace!("[{}] fetched msg {} from backend", self.config.name, message.id);
                return Some(message)
            }
        }
        None
    }

    /// all calls are serialized internally
    pub fn put(&mut self, message: &Message) -> Option<u64> {
        let _ = self.backend_wlock.lock().unwrap();
        trace!("[{}] putting message", self.config.name);
        self.backend.put(message)
    }

    /// delete access is suposed to be thread-safe, even while writing
    pub fn delete(&mut self, channel_name: &str, id: u64) -> Option<bool> {
        let locked_channels = self.channels.read().unwrap();
        if let Some(channel) = locked_channels.get(channel_name) {
            let removed_opt = channel.lock().unwrap().in_flight.remove(&id);
            trace!("[{}] message {} deleted from channel: {}",
                self.config.name, id, removed_opt.is_some());
            return Some(removed_opt.is_some())
        }
        None
    }

    pub fn purge(&mut self) {
        info!("[{}] purging", self.config.name);
        let _ = self.backend_rlock.write().unwrap();
        let _ = self.backend_wlock.lock().unwrap();
        self.backend.purge();
        self.channels.write().unwrap().clear();
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

        info!("[{}] checkpoint loaded: {:?}", self.config.name, queue_checkpoint);

        self.state = queue_checkpoint.state;

        let mut locked_channels = self.channels.write().unwrap();
        for (channel_name, channel_checkpoint) in queue_checkpoint.channels {
            let mut in_flight: LinkedHashMap<_, _, _> = Default::default();
            for id in channel_checkpoint.in_flight {
                in_flight.insert(id, Default::default());
            }

            locked_channels.insert(
                channel_name,
                Mutex::new(Channel {
                    tail: channel_checkpoint.tail,
                    in_flight: in_flight,
                })
            );
        }
    }

    fn checkpoint(&mut self) {
        let mut checkpoint = QueueCheckpoint {
            state: self.state,
            .. Default::default()
        };

        if self.state == QueueState::Ready {
            {
                let _ = self.backend_rlock.read();
                self.backend.checkpoint();
            }
            let locked_channels = self.channels.read().unwrap();
            for (channel_name, channel) in &*locked_channels {
                let locked_channel = channel.lock().unwrap();
                checkpoint.channels.insert(
                    channel_name.clone(),
                    ChannelCheckpoint {
                        tail: locked_channel.tail,
                        in_flight: locked_channel.in_flight
                            .keys().map(|&id| id).collect()
                    }
                );
            }
        }

        let tmp_path = self.config.data_directory.join(TMP_QUEUE_CHECKPOINT_FILE);
        let result = File::create(&tmp_path)
            .and_then(|mut file| {
                write!(file, "{}", json::as_pretty_json(&checkpoint)).unwrap();
                file.sync_data()
            }).and_then(|_| {
                let final_path = tmp_path.with_file_name(QUEUE_CHECKPOINT_FILE);
                fs::rename(tmp_path, final_path)
            });

        match result {
            Ok(_) => info!("[{}] checkpointed: {:?}", self.config.name, checkpoint),
            Err(error) =>
                warn!("[{}] error writing checkpoint information: {}",
                    self.config.name, error)
        }
    }

    pub fn maintenance(&mut self) {
        let smallest_tail = {
            let locked_channels = self.channels.read().unwrap();
            locked_channels.values().fold(0, |st, ch| {
                cmp::min(st, ch.lock().unwrap().tail)
            })
        };

        let _ = self.backend_rlock.read();
        self.checkpoint();
        self.backend.gc(smallest_tail);
    }

    fn tick(&mut self) {
        self.tick_to(precise_time_s() as u32);
    }

    pub fn tick_to(&mut self, clock: u32) {
        // FIXME: must ensure self.clock is within a single cache line
        self.clock = clock;
        debug!("[{}] tick to {}", self.config.name, self.clock);
    }

    #[allow(mutable_transmutes)]
    pub fn as_mut(&self) -> &mut Self {
        unsafe { mem::transmute(self) }
    }
}

impl Drop for Queue {
    fn drop(&mut self) {
        if self.state == QueueState::Deleting {
            self.purge()
        } else {
            self.checkpoint()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use config::*;
    use queue_backend::Message;
    use std::thread;
    use test;

    fn get_queue_opt(name: &str, recover: bool) -> Queue {
        let server_config = ServerConfig::read();
        let mut queue_config = server_config.new_queue_config(name);
        queue_config.time_to_live = 1;
        Queue::new(queue_config, recover)
    }

    fn get_queue() -> Queue {
        let thread = thread::current();
        let name = thread.name().unwrap().split("::").last().unwrap();
        get_queue_opt(name, false)
    }

    fn gen_message(id: u64) -> Message<'static> {
        Message {
            id: id,
            body: b"333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333"
        }
    }

    #[test]
    fn test_fill() {
        let mut q = get_queue();
        let message = gen_message(0);
        for i in (0..100_000) {
            let r = q.put(&message);
            assert!(r.is_some());
        }
    }

    #[test]
    fn test_put_get() {
        let mut q = get_queue();
        let message = gen_message(0);
        assert!(q.create_channel("test"));
        for i in (0..100_000) {
            assert!(q.put(&message).is_some());
            let m = q.get("test");
            assert!(m.is_some());
        }
    }

    #[test]
    fn test_create_channel() {
        let mut q = get_queue();
        let message = gen_message(0);
        assert!(q.get("test").is_none());
        assert!(q.put(&message).is_some());
        assert!(q.create_channel("test") == true);
        assert!(q.create_channel("test") == false);
        assert!(q.get("test").is_some());
    }

    #[test]
    fn test_in_flight() {
        let mut q = get_queue();
        let message = gen_message(0);
        assert!(q.get("test").is_none());
        assert!(q.put(&message).is_some());
        assert!(q.create_channel("test") == true);
        assert!(q.create_channel("test") == false);
        assert!(q.get("test").is_some());
        assert!(q.get("test").is_none());
        // TODO: check in flight count
    }

    #[test]
    fn test_in_flight_timeout() {
        let mut q = get_queue();
        let message = gen_message(0);
        assert!(q.create_channel("test") == true);
        assert!(q.put(&message).is_some());
        assert!(q.get("test").is_some());
        assert!(q.get("test").is_none());
        thread::sleep_ms(1001);
        q.tick();
        assert!(q.get("test").is_some());
    }

    #[test]
    fn test_backend_recover() {
        let mut q = get_queue_opt("test_backend_recover", false);
        let message = gen_message(0);
        let mut put_msg_count = 0;
        while q.backend.files_count() < 3 {
            assert!(q.put(&message).is_some());
            put_msg_count += 1;
        }
        q.backend.checkpoint();

        q = get_queue_opt("test_backend_recover", true);
        assert_eq!(q.backend.files_count(), 3);
        let mut get_msg_count = 0;
        assert!(q.create_channel("test") == true);
        while let Some(_) = q.get("test") {
            get_msg_count += 1;
        }
        assert_eq!(get_msg_count, put_msg_count);
    }

    #[test]
    fn test_queue_recover() {
        let mut q = get_queue_opt("test_queue_recover", false);
        let message = gen_message(0);
        assert!(q.create_channel("test") == true);
        assert!(q.put(&message).is_some());
        assert!(q.put(&message).is_some());
        assert!(q.get("test").is_some());
        q.checkpoint();

        q = get_queue_opt("test_queue_recover", true);
        assert!(q.create_channel("test") == false);
        assert!(q.get("test").is_some());
        assert!(q.get("test").is_some());
        assert!(q.get("test").is_none());
    }

    #[test]
    fn test_gc() {
        let message = gen_message(0);
        let mut q = get_queue_opt("test_reopen", false);
        assert!(q.create_channel("test") == true);

        while q.backend.files_count() < 3 {
            assert!(q.put(&message).is_some());
            assert!(q.get("test").is_some());
        }
        q.maintenance();

        // gc should get rid of the first two files
        assert_eq!(q.backend.files_count(), 1);
    }

    #[bench]
    fn put_like_crazy(b: &mut test::Bencher) {
        let mut q = get_queue();
        let m = &gen_message(0);
        let n = 10000;
        b.bytes = (m.body.len() * n) as u64;
        b.iter(|| {
            for _ in (0..n) {
                let r = q.put(m);
                assert!(r.is_some());
            }
        });
    }

    #[bench]
    fn put_get_like_crazy(b: &mut test::Bencher) {
        let mut q = get_queue();
        let m = &gen_message(0);
        let n = 10000;
        q.create_channel("test");
        b.bytes = (m.body.len() * n) as u64;
        b.iter(|| {
            for _ in (0..n) {
                let p = q.put(m);
                let r = q.get("test");
                assert_eq!(p.unwrap(), r.unwrap().id);
            }
        });
    }
}
