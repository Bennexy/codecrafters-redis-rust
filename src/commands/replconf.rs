use std::collections::VecDeque;

use crate::{
    commands::traits::{ArgErrorMessageGenerator, CommandName, Execute, Parse},
    parser::messages::RedisMessageType,
};

pub struct ReplConfCommand;

impl ReplConfCommand {
    fn new() -> Self {
        return Self {};
    }
}

// could be moved into a procedural macro in the future
impl CommandName for ReplConfCommand {
    fn command_name() -> &'static str {
        return "replconf";
    }
}
impl ArgErrorMessageGenerator<ReplConfCommand> for ReplConfCommand {}

impl Parse for ReplConfCommand {
    fn parse(_args: VecDeque<RedisMessageType>) -> Result<Self, RedisMessageType> {
        return Ok(Self::new());
    }
}

impl Execute for ReplConfCommand {
    fn execute(self) -> Result<RedisMessageType, RedisMessageType> {
        return Ok(RedisMessageType::simple_string("OK"));
    }
}
