use std::collections::VecDeque;

use crate::{
    commands::traits::{ArgErrorMessageGenerator, CommandName, Execute, Parse},
    db::data_store::get_db,
    parser::messages::RedisMessageType,
};

#[allow(dead_code)]
pub struct PsyncCommand {
    replication_id: String,
    replication_offset: u128,
}

impl PsyncCommand {
    pub fn new(replication_id: String, replication_offset: u128) -> Self {
        return Self {
            replication_id,
            replication_offset,
        };
    }
}

impl CommandName for PsyncCommand {
    fn command_name() -> &'static str {
        return "psync";
    }
}
impl ArgErrorMessageGenerator<PsyncCommand> for PsyncCommand {}

impl Parse for PsyncCommand {
    fn parse(mut args: VecDeque<RedisMessageType>) -> Result<Self, RedisMessageType> {
        if !args.is_empty() {
            return Err(Self::arg_count_error());
        }

        let replication_id = args
            .pop_front()
            .ok_or(Self::arg_count_error())?
            .bulk_string_value()?;
        let offset = args
            .pop_front()
            .ok_or(Self::arg_count_error())?
            .bulk_string_value()?
            .parse::<u128>()
            .unwrap_or(0);

        return Ok(PsyncCommand::new(replication_id, offset));
    }
}

impl Execute for PsyncCommand {
    fn execute(self) -> Result<RedisMessageType, RedisMessageType> {
        let data = get_db().get_config().replication_data;
        return Ok(RedisMessageType::simple_string(format!(
            "FULLRESYNC {} {}",
            data.master_repl_id, data.master_repl_offset
        )));
    }
}
