use std::collections::VecDeque;

use anyhow::Result;
use log::trace;

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
        let mut command_arg = match args
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

        let cmd = command_arg.make_contiguous();

        let command = Self::from_bytes(cmd, args)?;
        trace!("Parsed command {}", command.name().to_ascii_uppercase());

        return Ok(command);
    }
}
