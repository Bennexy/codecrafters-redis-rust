use crate::{commands::commands::Execute, db::data_store::get_db, parser::messages::RedisMessageType};

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ConfigAction {
    Get,
    Unknown(String),
}

impl From<String> for ConfigAction {
    fn from(value: String) -> Self {
        let uppercase = value.to_uppercase();
        if uppercase == "GET" {
            return ConfigAction::Get;
        } else {
            return ConfigAction::Unknown(uppercase);
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ConfigValue {
    DbDir,
    DbFilename,
    Unknown(String),
}

impl From<String> for ConfigValue {
    fn from(value: String) -> Self {
        let uppercase = value.to_uppercase();
        if uppercase == "DIR" {
            return ConfigValue::DbDir;
        } else if uppercase == "DBFILENAME" {
            return ConfigValue::DbFilename;
        } else {
            return ConfigValue::Unknown(uppercase);
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ConfigCommand;

impl ConfigCommand {
    pub fn new() -> Self {
        return Self {};
    }
}

impl Execute for ConfigCommand {
    fn execute(&self, args: &[RedisMessageType]) -> RedisMessageType {
        let action_value = match args.get(0) {
            Some(val) => val,
            None => {
                return RedisMessageType::Error(
                    "SET expects an key argument!".to_string(),
                )
            }
        };

        let value_value = match args.get(1) {
            Some(val) => val,
            None => {
                return RedisMessageType::Error(
                    "SET expects an value argument!".to_string(),
                )
            }
        };

        let action: ConfigAction = match action_value.as_string() {
            Some(val) => val.into(),
            None => {
                return RedisMessageType::Error(
                    "SET expects a Stringable key argument!".to_string(),
                )
            }
        };

        let config_value: ConfigValue = match value_value.as_string() {
            Some(val) => val.into(),
            None => {
                return RedisMessageType::Error(
                    "SET expects a Stringable value argument!".to_string(),
                )
            }
        };

        return match action {
            ConfigAction::Get => match config_value {
                ConfigValue::DbDir => {
                    return RedisMessageType::Array(vec![
                        RedisMessageType::BulkString("dir".into()),
                        RedisMessageType::BulkString(
                            (get_db().get_config()
                                .db_dir
                                .to_str()
                                .expect("Error getting the db_dir string. Should never happen.")
                                .to_string()),
                        ),
                    ])
                }
                ConfigValue::DbFilename => {
                    return RedisMessageType::Array(vec![
                        RedisMessageType::BulkString("dbfilename".into()),
                        RedisMessageType::BulkString((get_db().get_config().db_filename)),
                    ])
                }
                ConfigValue::Unknown(val) => {
                    return RedisMessageType::Error(format!(
                        "Unknown get config value: {}!",
                        val
                    ))
                }
            },
            ConfigAction::Unknown(val) => {
                return RedisMessageType::Error(format!(
                    "Unkown config operation: {}!",
                    val
                ))
            }
        };
    }
}
