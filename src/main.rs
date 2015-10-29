#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(raw_pointer_derive)]
#![feature(path_ext)]
#![feature(libc)]
#![feature(hashmap_hasher)]
#![feature(drain)]
#![feature(arc_counts)]
#![cfg_attr(test, feature(test))]
#![feature(str_match_indices)]

#[cfg(test)] extern crate test;
extern crate env_logger;
#[macro_use] extern crate log;
extern crate nix;
extern crate mio;
extern crate num_cpus;
extern crate threadpool;
extern crate time;
extern crate rustc_serialize;
extern crate toml;
extern crate linked_hash_map;
extern crate fnv;
extern crate spin;
extern crate libc;
extern crate rand;

mod config;
mod queue;
mod server;
mod protocol;
mod queue_backend;
mod utils;
mod rev;
mod atom;
mod cookie;
mod offset_index;

use config::*;
use server::*;

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
