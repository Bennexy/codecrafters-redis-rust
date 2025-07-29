use log::{debug, error, info};
use std::fmt::Display;

use crate::{
    db::data_store::{DataUnit, DB},
    parser::messages::RedisMessageType,
};

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Command {
    ECHO,
    PING,
    GET,
    SET,
    CONFIG,
    Unknown(String),
}

impl Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::ECHO => "ECHO".into(),
            Self::PING => "PING".into(),
            Self::GET => "GET".into(),
            Self::SET => "SET".into(),
            Self::CONFIG => "CONFIG".into(),
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
            "CONFIG" => Self::CONFIG,
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
            Self::SET => return set(args),
            Self::CONFIG => return config(args),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
struct SetCommand {
    key: String,
    value: String,
    expiry: Option<u128>,
}

impl SetCommand {
    fn new(value: &[RedisMessageType]) -> Result<SetCommand, RedisMessageType> {
        let key_value = match value.get(0) {
            Some(val) => val,
            None => {
                return Err(RedisMessageType::Error(
                    "SET expects an key argument!".to_string(),
                ))
            }
        };

        let value_value = match value.get(1) {
            Some(val) => val,
            None => {
                return Err(RedisMessageType::Error(
                    "SET expects an value argument!".to_string(),
                ))
            }
        };

        let key = match key_value.as_string() {
            Some(val) => val,
            None => {
                return Err(RedisMessageType::Error(
                    "SET expects a Stringable key argument!".to_string(),
                ))
            }
        };

        // todo: save not only string values
        let save_value = match value_value.as_string() {
            Some(val) => val,
            None => {
                return Err(RedisMessageType::Error(
                    "SET expects a Stringable value argument!".to_string(),
                ))
            }
        };

        let expiry: Option<u128> = match value.get(2) {
            Some(val) => match val.as_string() {
                Some(val) => {
                    info!("found px arg");
                    if val.to_uppercase() == "PX" {
                        match value.get(3) {
                            Some(val) => match val {
                                RedisMessageType::BulkString(val) => {
                                    match i64::from_str_radix(val, 10) {
                                        Ok(val) => Some(val.max(0) as u128),
                                        Err(_) => None,
                                    }
                                }
                                _ => None,
                            },
                            None => None,
                        }
                    } else {
                        None
                    }
                }
                None => None,
            },
            None => None,
        };

        return Ok(SetCommand {
            key,
            value: save_value,
            expiry,
        });
    }

    pub fn get_data_unit(&self) -> DataUnit {
        return DataUnit::new(self.value.clone(), self.expiry);
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
enum ConfigAction {
    Get,
    Unknown(String),
}

impl From<String> for ConfigAction {
    fn from(value: String) -> Self {
        let uppercase = value.to_uppercase();
        if uppercase == "GET" {
            return ConfigAction::Get;
        } else {
            return ConfigAction::Unknown(uppercase);
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
enum ConfigValue {
    DbDir,
    DbFilename,
    Unknown(String),
}

impl From<String> for ConfigValue {
    fn from(value: String) -> Self {
        let uppercase = value.to_uppercase();
        if uppercase == "DIR" {
            return ConfigValue::DbDir;
        } else if uppercase == "DBFILENAME" {
            return ConfigValue::DbFilename;
        } else {
            return ConfigValue::Unknown(uppercase);
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
struct ConfigCommand {
    action: ConfigAction,
    value: ConfigValue,
}

impl ConfigCommand {
    fn new(value: &[RedisMessageType]) -> Result<ConfigCommand, RedisMessageType> {
        let action_value = match value.get(0) {
            Some(val) => val,
            None => {
                return Err(RedisMessageType::Error(
                    "SET expects an key argument!".to_string(),
                ))
            }
        };

        let value_value = match value.get(1) {
            Some(val) => val,
            None => {
                return Err(RedisMessageType::Error(
                    "SET expects an value argument!".to_string(),
                ))
            }
        };

        let action: ConfigAction = match action_value.as_string() {
            Some(val) => val.into(),
            None => {
                return Err(RedisMessageType::Error(
                    "SET expects a Stringable key argument!".to_string(),
                ))
            }
        };

        let config_value: ConfigValue = match value_value.as_string() {
            Some(val) => val.into(),
            None => {
                return Err(RedisMessageType::Error(
                    "SET expects a Stringable value argument!".to_string(),
                ))
            }
        };

        return Ok(ConfigCommand {
            action: action,
            value: config_value,
        });
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
        Err(message) => return message,
    };

    debug!("set command? {:#?}", set_command);

    DB.set(set_command.key.clone(), set_command.get_data_unit());
    return RedisMessageType::SimpleString("OK".into());
}

fn config(args: &[RedisMessageType]) -> RedisMessageType {
    let config_command = match ConfigCommand::new(args) {
        Ok(command) => command,
        Err(err) => return err,
    };

    return match config_command.action {
        ConfigAction::Get => match config_command.value {
            ConfigValue::DbDir => {
                return RedisMessageType::Array(vec![
                    RedisMessageType::BulkString("dir".into()),
                    RedisMessageType::BulkString(
                        (DB.get_config()
                            .db_dir
                            .to_str()
                            .expect("Error getting the db_dir string. Should never happen.")
                            .to_string()),
                    ),
                ])
            }
            ConfigValue::DbFilename => {
                return RedisMessageType::Array(vec![
                    RedisMessageType::BulkString("dbfilename".into()),
                    RedisMessageType::BulkString((DB.get_config().db_filename)),
                ])
            }
            ConfigValue::Unknown(val) => {
                return RedisMessageType::Error(format!("Unknown get config value: {}!", val))
            }
        },
        ConfigAction::Unknown(val) => {
            return RedisMessageType::Error(format!("Unkown config operation: {}!", val))
        }
    };
}
