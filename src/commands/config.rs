use std::collections::VecDeque;

use crate::{
    commands::traits::{ArgErrorMessageGenerator, CommandName, Execute, Parse},
    db::data_store::get_db,
    parser::messages::RedisMessageType,
};

// more items could be implemented
enum ConfigItem {
    Dir,
    DbFile,
}

impl TryFrom<String> for ConfigItem {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.to_ascii_uppercase().as_str() {
            "DIR" => Ok(Self::Dir),
            "DBFILE" => Ok(Self::DbFile),
            _ => Err(value),
        }
    }
}

enum Action {
    // Get should support many entries.
    Get(Vec<ConfigItem>),
    Set((ConfigItem, String)),
    Help,
    Rewrite,
    ResetStat,
}

pub struct ConfigCommand {
    action: Action,
}

impl ConfigCommand {
    fn new(action: Action) -> Self {
        return Self { action };
    }
}

// could be moved into a procedural macro in the future
impl CommandName for ConfigCommand {
    fn command_name() -> &'static str {
        return "config";
    }
}
impl ArgErrorMessageGenerator<ConfigCommand> for ConfigCommand {}

fn parse_get_command(args: VecDeque<RedisMessageType>) -> Result<Action, RedisMessageType> {
    let mut items = Vec::with_capacity(args.len());

    for arg in args.iter() {
        let item = ConfigItem::try_from(arg.bulk_string_value()?).map_err(|err| {
            RedisMessageType::error(format!(
                "ERR Unknown option or number of arguments for CONFIG GET - '{}'",
                err
            ))
        })?;

        items.push(item);
    }

    return Ok(Action::Get(items));
}

fn parse_set_command(mut args: VecDeque<RedisMessageType>) -> Result<Action, RedisMessageType> {
    let arg = args
        .pop_front()
        .ok_or_else(Self::arg_count_error)?
        .bulk_string_value()?;

    let config_item = ConfigItem::try_from(arg).map_err(|err| {
        RedisMessageType::error(format!(
            "ERR Unknown option or number of arguments for CONFIG SET - '{}'",
            err
        ))
    })?;

    let value = args
        .pop_front()
        .ok_or_else(Self::arg_count_error)?
        .bulk_string_value()?;

    if !args.is_empty() {
        return Err(RedisMessageType::error(
            "ERR As of now only a single value may be set for the CONFIG SET command!",
        ));
    }

    return Ok(Action::Set((config_item, value)));
}


impl Parse for ConfigCommand {
    fn parse(mut args: VecDeque<RedisMessageType>) -> Result<Self, RedisMessageType> {
        let key = args
            .pop_front()
            .ok_or(Self::arg_count_error())?
            .bulk_string_value()?;

        let action = match key.to_ascii_uppercase().as_str() {
            "HELP" => Action::Help,
            "REWRITE" => Action::Rewrite,
            "RESETSTAT" => Action::ResetStat,
            "GET" => Self::parse_get_command(args)?,
            "SET" => Self::parse_set_command(args)?,
            _val => {
                return Err(RedisMessageType::error(format!(
                    "ERR unkown subcommand '{}'. Try CONFIG HELP",
                    _val
                )))
            }
        };

        return Ok(Self::new(action));
    }
}


fn execute_help() -> RedisMessageType {
    return RedisMessageType::bulk_string_array(vec![
        "CONFIG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        "GET <pattern>",
        "    Return parameters matching the <pattern> and their values.",
        "SET <directive> <value>",
        "    Set the configuration <directive> to <value>.",
        "HELP",
        "    Prints this help.",
    ]);
}

fn execute_get(items: Vec<ConfigItem>) -> Result<RedisMessageType, RedisMessageType> {
    let config = get_db().get_config();
    let result: VecDeque<RedisMessageType> = items
        .iter()
        .map(|item| match item {
            ConfigItem::DbFile => vec!["dbfilename", &config.db_filename],
            ConfigItem::Dir => vec![
                "dir",
                config.db_dir.to_str().expect(
                    "ERR Unable to get the dir due to technikal reason. Should never happen!",
                ),
            ],
        })
        .flat_map(|inner| {
            inner
                .into_iter()
                .map(|val| RedisMessageType::bulk_string(val))
        })
        .collect();

    return Ok(RedisMessageType::Array(result));
}

fn _execute_set(
    _item: ConfigItem,
    _value: String,
) -> Result<RedisMessageType, RedisMessageType> {
    return Ok(RedisMessageType::NullBulkString);
}

impl Execute for ConfigCommand {
    fn execute(self) -> Result<RedisMessageType, RedisMessageType> {
        let result = match self.action {
            Action::Help => execute_help(),
            Action::Get(action) => execute_get(action)?,
            Action::Set(val) => unimplemented!(),
            Action::ResetStat => unimplemented!(),
            Action::Rewrite => unimplemented!(),
        };

        return Ok(result);
    }
}
