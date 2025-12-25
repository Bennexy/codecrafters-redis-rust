use std::collections::VecDeque;

use crate::{
    commands::{
        echo,
        traits::{ArgErrorMessageGenerator, CommandName, Execute, Parse},
    }, db::data_store::get_db, parser::messages::RedisMessageType
};

// no arg support needed
pub struct InfoCommand;

impl InfoCommand {
    fn new() -> Self {
        return Self ;
    }
}

// could be moved into a procedural macro in the future
impl CommandName for InfoCommand {
    fn command_name() -> &'static str {
        return "info";
    }
}
impl ArgErrorMessageGenerator<InfoCommand> for InfoCommand {}

impl Parse for InfoCommand {
    fn parse(mut args: VecDeque<RedisMessageType>) -> Result<Self, RedisMessageType> {
        return Ok(Self::new());
    }
}

impl Execute for InfoCommand {
    fn execute(self) -> Result<RedisMessageType, RedisMessageType> {
        let role = get_db().get_config().replication_data.role.name();


        return Ok(RedisMessageType::BulkString(format!("role:{}", role)));
    }
}
