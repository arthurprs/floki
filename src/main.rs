#![allow(unused_imports)]
#![allow(dead_code)] 
#![allow(unused_variables)]
#![allow(raw_pointer_derive)]
#![feature(test)]
#![feature(path_ext)]
#![feature(hashmap_hasher)]

#![feature(custom_derive, plugin)]
#![plugin(serde_macros)]

#[cfg(test)] extern crate test;
extern crate env_logger;
#[macro_use] extern crate log;
extern crate fern;
extern crate nix;
extern crate mio;
extern crate num_cpus;
extern crate threadpool;
extern crate time;
extern crate toml;
extern crate serde;
extern crate linked_hash_map;
extern crate fnv;

mod config;
mod queue;
mod server;
mod protocol;

use config::*;
use queue::*;
use server::*;


fn configure_log() {
	let logger_config = fern::DispatchConfig {
	    format: Box::new(|msg: &str, level: &log::LogLevel, location: &log::LogLocation| {
	        format!("[{}]{}:{}: {}",
	        	time::now().strftime("%H:%M:%S.%f").unwrap(),
	        	level,
	        	location.module_path(),
	        	msg)
	    }),
	    output: vec![fern::OutputConfig::stderr()],
	    level: log::LogLevelFilter::Trace,
	};

	fern::init_global_logger(logger_config, log::LogLevelFilter::Debug).unwrap();
}


fn gen_message(id: u64) -> Message<'static> {
	Message {
		id: id,
		body: b"333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333"
	}
}

#[cfg(not(test))]
fn main() {
	env_logger::init().unwrap();
	// configure_log();
    info!("starting up");
    let server_config = ServerConfig::read();
    // let mut queue_configs = server_config.read_queue_configs();
    let (mut server_handler, mut ev_loop) = Server::new(server_config);
    info!("starting event loop");
    ev_loop.run(&mut server_handler).unwrap();
}
