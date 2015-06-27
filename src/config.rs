use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::io::prelude::*;
use toml::Parser as TomlParser;

#[derive(Debug)]
pub struct ServerConfig {
    pub data_directory: PathBuf,
    pub bind_address: String,
    pub port: u16,
    pub segment_size: u64,
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

        let mut s = String::new();
        File::open("floki.toml").unwrap().read_to_string(&mut s).unwrap();

        let config = TomlParser::new(&s).parse().unwrap();
        let port = config.get("port").unwrap().as_integer().unwrap() as u16;
        let bind_address = config.get("bind_address").unwrap().as_str().unwrap();
        let data_directory = config.get("data_directory").unwrap().as_str().unwrap();
        let segment_size_mb = config.get("segment_size_mb").unwrap().as_integer().unwrap();

        info!("done reading config: {:?}", config);

        ServerConfig {
            data_directory: data_directory.into(),
            port: port,
            bind_address: bind_address.into(),
            segment_size: segment_size_mb as u64 * 1024 * 1024,
        }
    }

    pub fn read_queue_configs(self: &ServerConfig) -> Vec<QueueConfig> {
        let read_dir = match fs::read_dir(&self.data_directory) {
            Ok(read_dir) => read_dir,
            _ =>  {
                info!("Can't read server data_directory {:?}", self.data_directory);
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
        if ! Path::new(&data_directory).is_dir() {
            fs::create_dir_all(&data_directory).unwrap();
        }
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

