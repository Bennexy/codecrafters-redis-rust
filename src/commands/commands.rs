use std::fmt::Display;

use crate::{
    commands::{config::ConfigCommand, echo::EchoCommand, get::GetCommand, set::SetCommand},
    parser::messages::RedisMessageType,
};

pub trait Execute {
    fn execute(&self, args: &[RedisMessageType]) -> RedisMessageType;
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Command {
    PING,
    ECHO(EchoCommand),
    GET(GetCommand),
    SET(SetCommand),
    CONFIG(ConfigCommand),
    Unknown(String),
}

impl Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::PING => "PING".into(),
            Self::ECHO(_) => "ECHO".into(),
            Self::GET(_) => "GET".into(),
            Self::SET(_) => "SET".into(),
            Self::CONFIG(_) => "CONFIG".into(),
            Self::Unknown(value) => format!("Unkown::{value}"),
        };

        return write!(f, "{}", name);
    }
}

impl From<&str> for Command {
    fn from(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "PING" => Self::PING,
            "GET" => Self::GET(GetCommand::new()),
            "SET" => Self::SET(SetCommand::new()),
            "ECHO" => Self::ECHO(EchoCommand::new()),
            "CONFIG" => Self::CONFIG(ConfigCommand::new()),
            _other => Self::Unknown(s.to_uppercase().clone()),
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

impl Execute for Command {
    fn execute(&self, args: &[RedisMessageType]) -> RedisMessageType {
        match self {
            Self::PING => return RedisMessageType::SimpleString("PONG".into()),
            Self::Unknown(val) => {
                return RedisMessageType::Error(format!("Unknown Redis command: {}", val.clone()))
            },
            Self::ECHO(command) => command.execute(args),
            Self::GET(command) => command.execute(args),
            Self::SET(command) => command.execute(args),
            Self::CONFIG(command) => command.execute(args),
            Self::Unknown(val) => {
                return RedisMessageType::Error(format!("Unknown Redis command: {}", val.clone()))
            }
            
        }
    }
}