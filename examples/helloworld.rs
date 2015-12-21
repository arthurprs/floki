extern crate redis;

use std::thread;
use redis::{RedisResult, Value as RedisValue, Commands};

fn main() {
    // connect to local host, default port
    let queue_name = "my_queue";
    let get_timeout = 5;
    let get_count = 2;
    let client = redis::Client::open("redis://127.0.0.1:9797/").unwrap();
    let mut thread_handles = Vec::new();
    let connection = client.get_connection().expect("Couldn't open connection");

    // create the queue and two channels
    for &channel_name in &["channel_1", "channel_2"] {
        let _: RedisResult<bool> = connection.set(queue_name, channel_name);
        // assert_eq!(result, Ok(true));
    }

    // start two producer threads
    for consumer_num in 0..2 {
        let connection = client.get_connection().expect("Couldn't open connection"); 
        let thread_handle = thread::spawn(move || {
            for _ in 0..10 {
                let msgs = [
                    format!("message 1 from producer {}", consumer_num),
                    format!("message 2 from producer {}", consumer_num),
                ];
                let result = connection.rpush(queue_name, &msgs);
                assert_eq!(result, Ok(2));
            }
        });
        thread_handles.push(thread_handle);
    }

    // start two consumer threads for channel_1 and one for channel_2
    for &channel_name in &["channel_1", "channel_1", "channel_2"] {
        let connection = client.get_connection().expect("Couldn't open connection");
        let thread_handle = thread::spawn(move || {
            loop {
                let maybe_result: RedisResult<Vec<Vec<RedisValue>>> =
                    redis::cmd("HMGET")
                    .arg(queue_name).arg(channel_name).arg(get_count).arg(get_timeout)
                    .query(&connection);

                let result = match maybe_result {
                    Err(_) => maybe_result.unwrap(), 
                    Ok(ref result) if result.is_empty() => {
                        println!("{} received no messages after {} seconds, will exit",
                            channel_name, get_timeout);
                        break
                    }
                    Ok(result) => result
                };

                let mut tickets = Vec::new();
                for msg in result {
                    let id: u64 = redis::from_redis_value(&msg[0]).unwrap();
                    let ticket: i64 = redis::from_redis_value(&msg[1]).unwrap();
                    let body: String = redis::from_redis_value(&msg[2]).unwrap();
                    tickets.push(ticket);
                    println!("{} received msg: id {} ticket {} body {:?}",
                        channel_name, id, ticket, body);
                }

                // ack the messages
                let _: usize = redis::cmd("HDEL")
                    .arg(queue_name).arg(channel_name).arg(&tickets[..])
                    .query(&connection).unwrap();
            }
        });
        thread_handles.push(thread_handle);
    }

    for thread_handle in thread_handles {
        thread_handle.join().unwrap();
    }

    // print some info
    {
        let result: RedisResult<Vec<String>> = redis::cmd("INFO").arg("queues").query(&connection);
        println!("queue info\n{}", result.unwrap()[0]);
        let result: RedisResult<Vec<String>> = redis::cmd("INFO").arg("server").query(&connection);
        println!("server info\n{}", result.unwrap()[0]);
    }
}
