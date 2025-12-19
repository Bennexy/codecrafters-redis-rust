use std::time::Duration;use std::time::Instant;

use log::debug;

use crate::{
    commands::commands::Execute,
    db::data_store::{DataUnit, get_db},
    parser::messages::RedisMessageType,
};

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SetCommand;

impl SetCommand {
    pub fn new() -> Self {
        return Self {};
    }
}

impl Execute for SetCommand {
    fn execute(&self, args: &[RedisMessageType]) -> RedisMessageType {
        let key_value = match args.get(0) {
            Some(val) => val,
            None => return RedisMessageType::Error("SET expects an key argument!".to_string()),
        };

        let value_value = match args.get(1) {
            Some(val) => val,
            None => return RedisMessageType::Error("SET expects an value argument!".to_string()),
        };

        let key = match key_value.as_string() {
            Some(val) => val,
            None => {
                return RedisMessageType::Error(
                    "SET expects a Stringable key argument!".to_string(),
                )
            }
        };

        // todo: save not only string values
        let save_value = match value_value.as_string() {
            Some(val) => val,
            None => {
                return RedisMessageType::Error(
                    "SET expects a Stringable value argument!".to_string(),
                )
            }
        };

        let expiry = match parse_expiry(args) {
            Ok(value) => value,
            Err(value) => match (value) {
                Some(val) => return val,
                _ => None,
            },
        };

        get_db().set(key, DataUnit::new(save_value, expiry));
        return RedisMessageType::SimpleString("OK".into());
    }
}

fn parse_expiry(args: &[RedisMessageType]) -> Result<Option<Duration>, Option<RedisMessageType>> {
    let time_arg = args
        .get(2)
        .ok_or(None)?
        .as_string()
        .ok_or(RedisMessageType::error(
            "Provided time arg is not a string!",
        ))?;

    if !vec!["EX", "PX", "EXAT", "PXAT"].contains(&time_arg.as_str()) {
        return Ok(None);
    }

    let string_numeric_value = args.get(3).ok_or_else(|| {
        RedisMessageType::Error(format!("SET with {} expects a value argument!", &time_arg))
    })?;

    let numeric_value = match string_numeric_value {
        RedisMessageType::BulkString(val) => match i64::from_str_radix(val, 10) {
            Ok(val) => val.max(0) as u64,
            Err(_) => {
                return Err(Some(RedisMessageType::Error(format!(
                    "SET with {} expects a Bulk String numeric value argument. Not {}!",
                    time_arg, val
                ))))
            }
        },
        val => {
            return Err(Some(RedisMessageType::Error(format!(
                "SET with {} expects a Bulk String numeric value argument! Argument is of type {}",
                time_arg, val
            ))))
        }
    };

    let expiration_duration: Duration = match time_arg.as_str() {
        "EX" => Duration::from_secs(numeric_value),
        "PX" => Duration::from_millis(numeric_value),
        "EXAT" | "PXAT" => return Err(Some(RedisMessageType::error("EXAT and EXAT arguments are not yet supported in this implementation!"))),
        _ => panic!("Can never happen!")
    };

    return Ok(Some(expiration_duration));
}
