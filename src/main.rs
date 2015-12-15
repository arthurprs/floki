#![allow(dead_code)]
#![feature(iter_arith)]
#![feature(hashmap_hasher)]
#![feature(drain)]
#![feature(arc_counts)]
#![cfg_attr(test, feature(test))]

#[cfg(test)] extern crate test;
extern crate env_logger;
#[macro_use] extern crate log;
extern crate libc;
extern crate nix;
extern crate mio;
extern crate num_cpus;
extern crate threadpool;
extern crate time;
extern crate rustc_serialize;
extern crate toml;
extern crate linked_hash_map;
extern crate fnv;
extern crate twox_hash;
extern crate spin;
extern crate rand;
extern crate tendril;
extern crate fs2;

mod config;
#[macro_use] mod utils;
mod queue;
#[cfg(not(test))] mod server;
mod protocol;
mod queue_backend;
mod rev;
mod atom;
mod cookie;
mod offset_index;
mod tristate_lock;

#[cfg(not(test))]
fn main() {
    use config::ServerConfig;
    use server::Server;

    env_logger::init().unwrap();
    info!("starting up");
    let server_config = ServerConfig::read();
    let (mut server_handler, mut ev_loop) = Server::new(server_config);
    info!("starting event loop");
    ev_loop.run(&mut server_handler).unwrap();
}
