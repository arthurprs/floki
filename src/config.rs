use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::io;
use std::io::prelude::*;
use toml::Parser as TomlParser;
use utils::*;

pub const TMP_QUEUE_CHECKPOINT_FILE: &'static str = "queue.checkpoint.tmp";
pub const QUEUE_CHECKPOINT_FILE: &'static str = "queue.checkpoint";
pub const TMP_BACKEND_CHECKPOINT_FILE: &'static str = "backend.checkpoint.tmp";
pub const BACKEND_CHECKPOINT_FILE: &'static str = "backend.checkpoint";
pub const DATA_EXTENSION: &'static str = "data";
pub const INDEX_EXTENSION: &'static str = "index";

#[derive(Debug, Clone, RustcEncodable)]
pub struct ServerConfig {
    pub data_directory: PathBuf,
    pub bind_address: String,
    pub max_connections: usize,
    pub maintenance_interval: u64,
    pub default_queue_config: QueueConfig,
}

#[derive(Debug, Clone, RustcEncodable)]
pub struct QueueConfig {
    pub name: String,
    pub data_directory: PathBuf,
    pub segment_size: u64,
    pub message_timeout: u32,
    pub retention_period: u32,
    pub hard_retention_period: u32,
    pub retention_size: u64,
    pub hard_retention_size: u64,
}


fn split_number_suffix(s: &str) -> Result<(u64, &str), GenericError> {
    let digits_end = s.chars().position(|c| !c.is_digit(10)).unwrap_or(s.len());
    let (digits, suffix) = (&s[0..digits_end], &s[digits_end..]);
    Ok((try!(digits.parse::<u64>()), suffix))
}

pub fn parse_duration(duration_text: &str) -> Result<u64, GenericError> {
    let (number, suffix) = try!(split_number_suffix(duration_text));
    let scale = match suffix.to_lowercase().as_ref() {
        "ms" => 1,
        "" | "s" => 1000,
        "m" => 1000 * 60,
        "h" => 1000 * 60 * 60,
        "d" => 1000 * 60 * 60 * 24,
        _ => return Err(format!("Unknown suffix `{}`", suffix).into())
    };
    number.checked_mul(scale).ok_or("Overflow error".into())
}

pub fn parse_size(size_text: &str) -> Result<u64, GenericError> {
    let (number, suffix) = try!(split_number_suffix(size_text));
    let scale = match suffix.to_lowercase().as_ref() {
        "" | "b" => 1,
        "k" | "kb" => 1024,
        "m" | "mb" => 1024 * 1024,
        "g" | "gb" => 1024 * 1024 * 1024,
        _ => return Err(format!("Unknown suffix `{}`", suffix).into())
    };
    number.checked_mul(scale).ok_or("Overflow error".into())
}

macro_rules! read_config {
    ($config: expr, $name: expr, $function: ident, $ty: expr) => {
        $config.get($name).expect(concat!("Config ", $name, " not found"))
            .$function().expect(concat!("Config ", $name, " expected to be of ", $ty, " type"))
    };
    ($config: expr, $name: expr => str) => {
        read_config!($config, $name, as_str, "string")
    };
    ($config: expr, $name: expr => int) => {
        read_config!($config, $name, as_integer, "integer")
    };
    ($config: expr, $name: expr => size) => {
        parse_size(read_config!($config, $name, as_str, "size")).
            expect(concat!("Config ", $name, " can't be parsed as size"))
    };
    ($config: expr, $name: expr => duration) => {
        parse_duration(read_config!($config, $name, as_str, "duration")).
            expect(concat!("Config ", $name, " can't be parsed as duration"))
    }
}

impl ServerConfig {
    pub fn read() -> ServerConfig {
        debug!("reading config file");
        let config = {
            let mut s = String::new();
            File::open("floki.toml").expect("Error opening config file").
                read_to_string(&mut s).expect("Error reading config file");
            TomlParser::new(&s).parse().expect("Error parsing config file")
        };
        debug!("done reading config file: {:?}", config);

        let bind_address = read_config!(config, "bind_address" => str);
        let data_directory = read_config!(config, "data_directory" => str);
        let max_connections = read_config!(config, "max_connections" => int);
        let segment_size = read_config!(config, "segment_size" => size);
        let maintenance_interval = read_config!(config, "maintenance_interval" => duration);
        let message_timeout = read_config!(config, "message_timeout" => duration);
        let retention_period = read_config!(config, "retention_period" => duration);
        let hard_retention_period = read_config!(config, "hard_retention_period" => duration);
        let retention_size = read_config!(config, "retention_size" => size);
        let hard_retention_size = read_config!(config, "hard_retention_size" => size);

        assert!(segment_size >= 16 * 1024 * 1024 && segment_size <= 1 << 31,
            "segment_size must be between 16MB and 2GB");
        create_dir_if_not_exist(data_directory).expect("Data directory not acessible");

        ServerConfig {
            data_directory: data_directory.into(),
            bind_address: bind_address.into(),
            max_connections: max_connections as usize,
            maintenance_interval: maintenance_interval,
            default_queue_config: QueueConfig {
                name: "".into(),
                data_directory: "".into(),
                segment_size: segment_size,
                message_timeout: (message_timeout / 1000) as u32,
                retention_period: (retention_period / 1000) as u32,
                hard_retention_period : (hard_retention_period / 1000) as u32,
                retention_size : retention_size,
                hard_retention_size: hard_retention_size,
            }
        }
    }

    pub fn read_queue_configs(self: &ServerConfig) -> Result<Vec<QueueConfig>, GenericError> {
        debug!("reading queue configurations");
        let mut queue_configs = Vec::new();
        for maybe_entry in try!(fs::read_dir(&self.data_directory)) {
            queue_configs.push(try!(QueueConfig::read(self, &try!(maybe_entry).path())))
        }
        Ok(queue_configs)
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
            .. server_config.default_queue_config
        }
    }

    fn read(server_config: &ServerConfig, data_directory: &Path) -> Result<QueueConfig, GenericError> {
        let name = data_directory.file_name().unwrap().to_string_lossy().into_owned();
        Ok(Self::new(server_config, name))
        // TODO: load from a file instead
    }

    fn write(&self) -> io::Result<()> {
        Ok(())
    }
}
