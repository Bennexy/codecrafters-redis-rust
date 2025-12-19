use log::debug;

use crate::{commands::commands::Execute, db::data_store::DataUnit, parser::messages::RedisMessageType};

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct EchoCommand;

impl EchoCommand {
    pub fn new() -> Self {
        return Self {};
    }
}

impl Execute for EchoCommand {
    fn execute(&self, args: &[RedisMessageType]) -> RedisMessageType {

        let value = match args.get(0) {
            Some(val) => val,
            None => return RedisMessageType::Error("ECHO expects an argument!".to_string()),
        };
    
        let echo_value = match value.as_string() {
            Some(val) => val,
            None => {
                return RedisMessageType::Error("GET expects a Stringable key argument!".to_string())
            }
        };

        return RedisMessageType::BulkString(echo_value);
    }
}
