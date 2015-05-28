#![allow(unused_imports)]
#![allow(dead_code)] 
#![allow(unused_variables)]
#![allow(raw_pointer_derive)]
#![feature(test)]
#![feature(path_ext)]

#[cfg(test)] extern crate test;
extern crate env_logger;
#[macro_use] extern crate log;
extern crate nix;
extern crate threadpool;
extern crate time;
extern crate toml;
extern crate mio;
extern crate num_cpus;

mod config;
mod queue;
mod server;
mod protocol;

use config::*;
use queue::*;
use server::*;


fn gen_message(id: u64) -> Message<'static> {
	Message {
		id: id,
		body: b"333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333"
	}
}

fn main() {
	env_logger::init().unwrap();
    info!("starting up");
    let server_config = ServerConfig::read();
    let mut queue_configs = server_config.read_queue_configs();
    let mut q = Queue::new(
    	queue_configs.pop().unwrap_or_else(|| server_config.new_queue_config("test"))
	);
	let message = gen_message(0);
	for _ in (0..100_000) {
		let r = q.put(&message);
		assert!(r.is_some());
		{
			let m = q.get();
			assert!(m.is_some());
		}
		let m = q.get();
		assert!(m.is_none());
	}
}
