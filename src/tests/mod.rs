use std::thread;
use std::time::Duration;
use std::sync::{Once, ONCE_INIT};
use redis::{self, RedisResult, Value as RedisValue, Commands};
use super::server::Server;
use super::config::ServerConfig;

static START: Once = ONCE_INIT;

fn start_test_server() {
    let mut server_config = ServerConfig::read();
    server_config.data_directory = "./test_data".into();
    server_config.default_queue_config.segment_size = 4 * 1024 * 1024;
    server_config.default_queue_config.retention_period = 1;
    server_config.default_queue_config.hard_retention_period = 2;
    server_config.default_queue_config.retention_size = 0;
    server_config.default_queue_config.hard_retention_size = 0;
    let (mut server_handler, mut ev_loop) = Server::new(server_config);
    thread::spawn(move || {
        ev_loop.run(&mut server_handler).unwrap();
    });
    thread::sleep(Duration::from_millis(1000));
}

#[test]
fn test_roundtrip() {
    START.call_once(|| start_test_server());
    let queue_name = "tests_test_roundtrip";
    let channel_name = "channel_1";
    let client = redis::Client::open("redis://127.0.0.1:9797/").unwrap();
    let connection = client.get_connection().expect("Couldn't open connection");

    // create the queue
    let _: RedisResult<bool> = connection.set(queue_name, channel_name);
    // purge
    let _: RedisResult<u32> = connection.del(&[queue_name, "*"]);

    let test_bodies = [
        "Message1 abc",
        "Message2 def",
    ];

    let result = connection.rpush(queue_name, &test_bodies);
    assert_eq!(result, Ok(2));

    let msgs: Vec<Vec<RedisValue>> =
        redis::cmd("HMGET")
        .arg(queue_name).arg(channel_name).arg(test_bodies.len())
        .query(&connection).unwrap();

    let msg_bodies: Vec<String> = msgs.iter().map(|msg| {
        let body: String = redis::from_redis_value(&msg[2]).unwrap();
        body
    }).collect();
    assert_eq!(msg_bodies, test_bodies);
}
