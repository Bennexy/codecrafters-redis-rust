use std::collections::VecDeque;

use crate::{
    commands::{
        echo,
        traits::{ArgErrorMessageGenerator, CommandName, Execute, Parse},
    },
    consts::LF,
    db::data_store::get_db,
    parser::messages::RedisMessageType,
};

// no arg support needed
pub struct InfoCommand;

impl InfoCommand {
    fn new() -> Self {
        return Self;
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
        let repl_data = get_db().get_config().replication_data;

        return Ok(RedisMessageType::BulkString(format!(
            "role:{}{LF}master_replid:{}{LF}master_repl_offset:{}{LF}",
            repl_data.role.name(),
            repl_data.master_repl_id,
            repl_data.master_repl_offset
        )));
    }
}
