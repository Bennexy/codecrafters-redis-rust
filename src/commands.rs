use log::error;
use std::fmt::Display;

use crate::{consts::GLOBAL_MAP, parser::messages::RedisMessageType};

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Command {
    ECHO,
    PING,
    GET,
    SET,
    Unknown(String),
}

impl Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::ECHO => "ECHO".into(),
            Self::PING => "PING".into(),
            Self::GET => "GET".into(),
            Self::SET => "SET".into(),
            Self::Unknown(value) => format!("Unkown::{value}"),
        };

        return write!(f, "{}", name);
    }
}

impl From<&str> for Command {
    fn from(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "ECHO" => Self::ECHO,
            "PING" => Self::PING,
            "GET" => Self::GET,
            "SET" => Self::SET,
            other => Self::Unknown(s.to_uppercase().clone()),
        }
    }
}

impl From<&RedisMessageType> for Command {
    fn from(value: &RedisMessageType) -> Self {
        match value {
            RedisMessageType::BulkString(val) | RedisMessageType::SimpleString(val) => {
                Command::from(val.as_str())
            }
            _ => Command::Unknown(value.to_string()),
        }
    }
}

impl Command {
    pub fn execute(&self, args: &[RedisMessageType]) -> RedisMessageType {
        match self {
            Self::Unknown(val) => {
                return RedisMessageType::Error(format!("Unknown Redis command: {}", val.clone()))
            }
            Self::PING => return RedisMessageType::SimpleString("PONG".into()),
            Self::ECHO => return echo(args.get(0)),
            Self::GET => return get(args.get(0)),
            Self::SET => return set(args)
        }
    }
}

fn echo(arg: Option<&RedisMessageType>) -> RedisMessageType {
    let value = match arg {
        Some(val) => val,
        None => return RedisMessageType::Error("Echo expects an argument!".to_string()),
    };

    let key = match value.as_string() {
        Some(val) => val,
        None => return RedisMessageType::Error("Echo expects a Stringable argument!".to_string()),
    };

    let value = match value {
        RedisMessageType::BulkString(val) | RedisMessageType::SimpleString(val) => val.to_string(),
        RedisMessageType::Integer(val) => val.to_string(),
        other => panic!("Redis cannot echo value of type: {}", other.to_string()),
    };

    return RedisMessageType::BulkString(value);
}

fn get(arg: Option<&RedisMessageType>) -> RedisMessageType {
    let value = match arg {
        Some(val) => val,
        None => return RedisMessageType::Error("GET expects an argument!".to_string()),
    };

    let key = match value.as_string() {
        Some(val) => val,
        None => {
            return RedisMessageType::Error("GET expects a Stringable key argument!".to_string())
        }
    };

    match GLOBAL_MAP.try_read() {
        Ok(map) => match map.get(&key) {
            Some(val) => return RedisMessageType::BulkString(val.clone()),
            None => return RedisMessageType::NullBulkString,
        },
        Err(err) => {
            let msg = "Unable to read key due to lock!";
            error!("{msg}");
            return RedisMessageType::Error(msg.into());
        }
    }
}

fn set(args: &[RedisMessageType]) -> RedisMessageType {
    let key_value = match args.get(0) {
        Some(val) => val,
        None => return RedisMessageType::Error("SET expects an key argument!".to_string()),
    };

    let value_value = match args.get(1) {
        Some(val) => val,
        None => return RedisMessageType::Error("SET expects an value argument!".to_string()),
    };

    let key = match key_value.as_string() {
        Some(val) => val,
        None => {
            return RedisMessageType::Error("SET expects a Stringable key argument!".to_string())
        }
    };

    // todo: save not only string values
    let value = match value_value.as_string() {
        Some(val) => val,
        None => {
            return RedisMessageType::Error("SET expects a Stringable value argument!".to_string())
        }
    };


    match GLOBAL_MAP.try_write() {
        Ok(mut val) => {
            val.insert(key, value);
            return RedisMessageType::SimpleString("OK".into());
        }
        Err(err) => {
            let msg = "Unable to set key due to lock!";
            error!("{msg}");
            return RedisMessageType::Error(msg.into());
        }
    }
}
