use std::fs::{self, File};
use std::path::PathBuf;
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
    pub max_connections: usize,
    pub segment_size: u64,
    pub maintenance_interval: u64,
    pub message_timeout: u32,
    pub retention_period: u32,
}

#[derive(Debug)]
pub struct QueueConfig {
    pub name: String,
    pub data_directory: PathBuf,
    pub segment_size: u64,
    pub message_timeout: u32,
    pub retention_period: u32,
}


fn split_number_suffix(s: &str) -> Result<(u64, &str), ()> {
    let digits_end = s.chars().position(|c| !c.is_digit(10)).unwrap_or(0);
    let (digits, suffix) = (&s[0..digits_end], &s[digits_end..]);
    if let Ok(number) = digits.parse::<u64>() {
        Ok((number, suffix))
    } else {
        Err(())
    }
}

pub fn parse_duration(duration_text: &str) -> Result<u64, ()> {
    let (number, suffix) = try!(split_number_suffix(duration_text));
    let scale = match suffix.to_lowercase().as_ref() {
        "ms" => 1,
        "s" => 1000,
        "m" => 1000 * 60,
        "h" => 1000 * 60 * 60,
        "d" => 1000 * 60 * 60 * 24,
        _ => return Err(())
    };
    number.checked_mul(scale).ok_or(())
}

pub fn parse_size(size_text: &str) -> Result<u64, ()> {
    let (number, suffix) = try!(split_number_suffix(size_text));
    let scale = match suffix.to_lowercase().as_ref() {
        "b" => 1,
        "m" | "mb" => 1024,
        "g" | "gb" => 1024 * 1024,
        _ => return Err(())
    };
    number.checked_mul(scale).ok_or(())
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
        let max_connections = config.get("max_connections").unwrap().as_integer().unwrap();
        let segment_size = parse_size(
            config.get("segment_size").unwrap().as_str().unwrap()).unwrap();
        let maintenance_interval = parse_duration(
            config.get("maintenance_interval").unwrap().as_str().unwrap()).unwrap();
        let message_timeout = parse_duration(
            config.get("message_timeout").unwrap().as_str().unwrap()).unwrap();
        let retention_period = parse_duration(
            config.get("retention_period").unwrap().as_str().unwrap()).unwrap();

        assert!(segment_size <= 1 << 31, "segment_size must be <= 2GB");

        ServerConfig {
            data_directory: data_directory.into(),
            bind_address: bind_address.into(),
            max_connections: max_connections as usize,
            segment_size: segment_size,
            maintenance_interval: maintenance_interval,
            message_timeout: (message_timeout / 1000) as u32,
            retention_period: (retention_period / 1000) as u32,
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
            message_timeout: server_config.message_timeout,
            retention_period: server_config.retention_period,
        }
    }

    fn read(server_config: &ServerConfig, data_directory: PathBuf) -> QueueConfig {
        let name = data_directory.file_name().unwrap().to_string_lossy().into_owned();
        Self::new(server_config, name)
        // TODO: load from a file instead
    }
}

