use log::debug;

use crate::{
    commands::commands::Execute,
    db::data_store::{DataUnit, get_db},
    parser::messages::RedisMessageType,
};

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct GetCommand;

impl GetCommand {
    pub fn new() -> Self {
        return Self {};
    }
}

impl Execute for GetCommand {
    fn execute(&self, args: &[RedisMessageType]) -> RedisMessageType {

        let value = match args.get(0) {
            Some(val) => val,
            None => {
                return RedisMessageType::Error(
                    "GET expects an argument!".to_string(),
                )
            }
        };

        let key = match value.as_string() {
            Some(val) => val,
            None => {
                return RedisMessageType::Error(
                    "GET expects a Stringable key argument!".to_string(),
                )
            }
        };

        match get_db().get(&key) {
            Some(val) => return RedisMessageType::BulkString(val.clone()),
            None => return RedisMessageType::NullBulkString,
        }
    }
}
