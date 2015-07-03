use std::ops::Deref;
use std::sync::{Arc, Mutex, RwLock};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::collections::hash_state::DefaultState;

use std::mem;
use std::rc::Rc;
use linked_hash_map::LinkedHashMap;
use time::precise_time_s;
use fnv::FnvHasher;

use config::*;
use queue_backend::*;

#[derive(Debug)]
pub struct Channel {
    tail: u64,
    message_count: u64,
    in_flight: LinkedHashMap<u64, u32, DefaultState<FnvHasher>>
}

#[derive(Debug)]
pub struct Queue {
    config: Rc<QueueConfig>,
    // backend writes don't block readers
    // FIXME: queue_backend should handle it's concurrency access internally,
    // both for simplicity and performance
    backend_wlock: Mutex<()>,
    backend_rlock: RwLock<()>,
    backend: QueueBackend,
    channels: RwLock<HashMap<String, Mutex<Channel>, DefaultState<FnvHasher>>>,
    clock: u32
}

#[derive(Clone)]
pub struct ArcQueue(Arc<Queue>);

unsafe impl Sync for ArcQueue {}
unsafe impl Send for ArcQueue {}

impl Deref for ArcQueue {
    type Target = Queue;
    fn deref(&self) -> &Self::Target {
        let &ArcQueue(ref queue) = self;
        queue
    }
}

impl ArcQueue {
    pub fn new(queue: Queue) -> ArcQueue {
        ArcQueue(Arc::new(queue))
    }
}

impl Queue {

    pub fn new(config: QueueConfig, recover: bool) -> Queue {
        let rc_config = Rc::new(config);
        let mut queue = Queue {
            config: rc_config.clone(),
            backend_wlock: Mutex::new(()),
            backend_rlock: RwLock::new(()),
            backend: QueueBackend::new(rc_config.clone(), recover),
            channels: RwLock::new(Default::default()),
            clock: 0,
        };
        queue.tick();
        queue
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
                        message_count: 0,
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
            if let Some((&id, &expires)) = locked_channel.in_flight.front() {
                if self.clock >= expires {
                    // FIXME: double get bellow, not ideal
                    let new_expires = locked_channel.in_flight.get_refresh(&id).unwrap();
                    *new_expires = self.clock + self.config.time_to_live;
                    trace!("[{}] msg {} expired and will be sent again", self.config.name, id);
                    return Some(self.backend.get(id).unwrap().1)
                }
            }

            // fetch from the backend
            if let Some((new_tail, message)) = self.backend.get(locked_channel.tail) {
                locked_channel.tail = new_tail;
                locked_channel.in_flight.insert(message.id, self.clock + self.config.time_to_live);
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
        let _ = self.backend_rlock.read().unwrap();
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

    pub fn maintenance(&mut self) {
        // add gc code here
    }

    pub fn tick(&mut self) {
        self.clock = precise_time_s() as u32;
        debug!("[{}] tick to {}", self.config.name, self.clock);
    }

    #[allow(mutable_transmutes)]
    pub fn as_mut(&self) -> &mut Self {
        unsafe { mem::transmute(self) }
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
        let mut queue = Queue::new(queue_config, recover);
        queue
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

    fn init_logger() {
        use env_logger;
        env_logger::init();
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
        init_logger();
        let mut q = get_queue_opt("test_reopen", false);
        let message = gen_message(0);
        let mut put_msg_count = 0;
        while q.backend.files_count() < 3 {
            assert!(q.put(&message).is_some());
            put_msg_count += 1;
        }
        drop(q);

        let mut q = get_queue_opt("test_reopen", true);
        assert!(q.create_channel("test") == true);
        assert_eq!(q.backend.files_count(), 3);
        let mut get_msg_count = 0;
        while let Some(_) = q.get("test") {
            get_msg_count += 1;
        }
        assert_eq!(get_msg_count, put_msg_count);
    }

    #[test]
    fn test_queue_recover() {
        // Add code here
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
