use crate::{commands::commands::Execute, db::data_store::get_db, RedisMessageType};

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SaveCommand;

impl Execute for SaveCommand {
    fn execute(&self, args: &[RedisMessageType]) -> RedisMessageType {
        unimplemented!();
    }
}

impl SaveCommand {
    pub fn new() -> Self {
        return SaveCommand {};
    }
}
