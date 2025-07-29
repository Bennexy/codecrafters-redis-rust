use log::error;
use std::fmt::Display;

use crate::{parser::messages::RedisMessageType, db::data_store::{DB, DataUnit}};

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

#[derive(Debug, PartialEq, Eq, Clone)]
struct SetCommand {
    key: String,
    value: String,
    expiry: Option<u128>
}

impl SetCommand {
    fn new(value: &[RedisMessageType]) -> Result<SetCommand, RedisMessageType> {

        let key_value = match value.get(0) {
            Some(val) => val,
            None => return Err(RedisMessageType::Error("SET expects an key argument!".to_string())),
        };
    
        let value_value = match value.get(1) {
            Some(val) => val,
            None => return Err(RedisMessageType::Error("SET expects an value argument!".to_string()),)
        };

        let key = match key_value.as_string() {
            Some(val) => val,
            None => {
                return Err(RedisMessageType::Error("SET expects a Stringable key argument!".to_string()))
            }
        };
    
        // todo: save not only string values
        let save_value = match value_value.as_string() {
            Some(val) => val,
            None => {
                return Err(RedisMessageType::Error("SET expects a Stringable value argument!".to_string()))
            }
        };

        let expiry: Option<u128> = match value.get(2) {
            Some(val) => match val.as_string() {
                Some(val) => if val.to_uppercase() == "PX" {
                    match value.get(3) {
                        Some(val) => match val {
                            RedisMessageType::Integer(val) => Some(*val.max(&0) as u128),
                            _ => None
                        },
                        None => None
                        
                    }
                } else {
                    None
                },
                None => None
            },
            None => None
        };


        return Ok(SetCommand {
            key, value: save_value, expiry
        });
        
    }

    pub fn get_data_unit(&self) -> DataUnit {
        return DataUnit::new(self.value.clone(), self.expiry)
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

    match DB.get(&key) {
        Some(val) => return RedisMessageType::BulkString(val.clone()),
        None => return RedisMessageType::NullBulkString,
    }
}

fn set(args: &[RedisMessageType]) -> RedisMessageType {

    let set_command = match SetCommand::new(args) {
        Ok(val) => val,
        Err(message) => return message
    };

    DB.set(set_command.key.clone(), set_command.get_data_unit());
    return RedisMessageType::SimpleString("OK".into());
}
