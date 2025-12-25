use std::collections::VecDeque;

use anyhow::Result;

use crate::{
    commands::{
        config::ConfigCommand,
        echo::EchoCommand,
        get::GetCommand,
        info::InfoCommand,
        keys::KeysCommand,
        ping::PingCommand,
        psync::PsyncCommand,
        replconf::ReplConfCommand,
        set::SetCommand,
        traits::{Command, Parsed, Unparsed},
    },
    parser::messages::RedisMessageType,
    redis_commands,
};

redis_commands! {
    Ping => PingCommand,
    Echo => EchoCommand,
    Set => SetCommand,
    Get => GetCommand,
    Config => ConfigCommand,
    Keys => KeysCommand,
    Info => InfoCommand,
    ReplConf => ReplConfCommand,
    Psync => PsyncCommand
}

impl UnparsedCommandType {
    pub fn new(mut args: VecDeque<RedisMessageType>) -> Result<Self, RedisMessageType> {
        let command_arg = match args
            .pop_front()
            .ok_or(RedisMessageType::error("No argument passed to redis!"))?
        {
            RedisMessageType::BulkString(val) => val,
            _ => {
                return Err(RedisMessageType::error(
                    "Command must be encoded as a bulk string!",
                ))
            }
        };

        let command = match command_arg.to_uppercase().as_str() {
            "PING" => Self::Ping(Command::<Unparsed, PingCommand>::new(args)),
            "GET" => Self::Get(Command::<Unparsed, GetCommand>::new(args)),
            "SET" => Self::Set(Command::<Unparsed, SetCommand>::new(args)),
            "ECHO" => Self::Echo(Command::<Unparsed, EchoCommand>::new(args)),
            "CONFIG" => Self::Config(Command::<Unparsed, ConfigCommand>::new(args)),
            "KEYS" => Self::Keys(Command::<Unparsed, KeysCommand>::new(args)),
            "INFO" => Self::Info(Command::<Unparsed, InfoCommand>::new(args)),
            "REPLCONF" => Self::ReplConf(Command::<Unparsed, ReplConfCommand>::new(args)),
            "PSYNC" => Self::Psync(Command::<Unparsed, PsyncCommand>::new(args)),
            // "SAVE" => Self::SAVE(SaveCommand::new(args)),
            _other => {
                return Err(RedisMessageType::error(format!(
                    "Unknown command name: '{}'",
                    _other
                )))
            }
        };

        return Ok(command);
    }
}
