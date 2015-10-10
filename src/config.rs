use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::io::prelude::*;
use toml::Parser as TomlParser;

pub const TMP_QUEUE_CHECKPOINT_FILE: &'static str = "queue.checkpoint.tmp";
pub const QUEUE_CHECKPOINT_FILE: &'static str = "queue.checkpoint";
pub const TMP_BACKEND_CHECKPOINT_FILE: &'static str = "backend.checkpoint.tmp";
pub const BACKEND_CHECKPOINT_FILE: &'static str = "backend.checkpoint";
pub const DATA_EXTENSION: &'static str = "data";
pub const INDEX_EXTENSION: &'static str = "index";

#[derive(Debug)]
pub struct ServerConfig {
    pub data_directory: PathBuf,
    pub bind_address: String,
    pub segment_size: u64,
    pub maintenance_timeout: u32,
}

#[derive(Debug)]
pub struct QueueConfig {
    pub name: String,
    pub data_directory: PathBuf,
    pub segment_size: u64,
    pub time_to_live: u32,
}

impl ServerConfig {
    pub fn read() -> ServerConfig {
        debug!("reading config");

        let config = {
            let mut s = String::new();
            File::open("floki.toml").unwrap().read_to_string(&mut s).unwrap();
            TomlParser::new(&s).parse().unwrap()
        };
        info!("done reading config: {:?}", config);

        let bind_address = config.get("bind_address").unwrap().as_str().unwrap();
        let data_directory = config.get("data_directory").unwrap().as_str().unwrap();
        let segment_size = config.get("segment_size").unwrap().as_integer().unwrap() as u64 * 1024 * 1024;
        let maintenance_timeout = config.get("maintenance_timeout").unwrap().as_integer().unwrap();

        assert!(segment_size <= 2 << 31, "segment_size must be <= 2GB");

        ServerConfig {
            data_directory: data_directory.into(),
            bind_address: bind_address.into(),
            segment_size: segment_size,
            maintenance_timeout: maintenance_timeout as u32,
        }
    }

    pub fn read_queue_configs(self: &ServerConfig) -> Vec<QueueConfig> {
        let read_dir = match fs::read_dir(&self.data_directory) {
            Ok(read_dir) => read_dir,
            Err(error) => {
                warn!("Can't read server data_directory {}", error);
                return Vec::new()
            }
        };

        read_dir.filter_map(|entry_opt| {
            let entry_path = entry_opt.unwrap().path();
            if ! entry_path.is_dir() {
                return None
            }
            Some(QueueConfig::read(self, entry_path))
        }).collect()
    }

    pub fn new_queue_config<S: Into<String>>(&self, queue_name: S) -> QueueConfig {
        QueueConfig::new(self, queue_name.into())
    }
}

impl QueueConfig {
    fn new(server_config: &ServerConfig, name: String) -> QueueConfig {
        let data_directory = server_config.data_directory.join(&name);
        QueueConfig {
            name: name,
            data_directory: data_directory,
            segment_size: server_config.segment_size,
            time_to_live: 30
        }
    }

    fn read(server_config: &ServerConfig, data_directory: PathBuf) -> QueueConfig {
        let name = data_directory.file_name().unwrap().to_string_lossy().into_owned();
        Self::new(server_config, name)
        // TODO: load from a file instead
    }
}

