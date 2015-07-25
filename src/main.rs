#![allow(unused_imports)]
#![allow(dead_code)] 
#![allow(unused_variables)]
#![allow(raw_pointer_derive)]
#![feature(path_ext)]
#![feature(hashmap_hasher)]
#![cfg_attr(test, feature(test))]

#[cfg(test)] extern crate test;
extern crate env_logger;
#[macro_use] extern crate log;
extern crate nix;
extern crate mio;
extern crate num_cpus;
extern crate threadpool;
extern crate clock_ticks;
extern crate rustc_serialize;
extern crate toml;
extern crate linked_hash_map;
extern crate vec_map;
extern crate fnv;
extern crate spin;

mod config;
mod queue;
mod server;
mod protocol;
mod queue_backend;
mod utils;

use config::*;
use queue::*;
use queue_backend::Message;
use server::*;


// fn configure_log() {
//  let logger_config = fern::DispatchConfig {
//      format: Box::new(|msg: &str, level: &log::LogLevel, location: &log::LogLocation| {
//          format!("[{}]{}:{}: {}",
//              time::now().strftime("%H:%M:%S.%f").unwrap(),
//              level,
//              location.module_path(),
//              msg)
//      }),
//      output: vec![fern::OutputConfig::stderr()],
//      level: log::LogLevelFilter::Trace,
//  };

//  fern::init_global_logger(logger_config, log::LogLevelFilter::Debug).unwrap();
// }

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
