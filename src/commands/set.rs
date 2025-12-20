use std::{
    collections::{HashMap, btree_map::Values}, time::{Duration, SystemTime, UNIX_EPOCH}, usize
};

use log::{debug, trace};

use crate::{
    commands::commands::Execute,
    db::data_store::{get_db, DataUnit},
    parser::messages::RedisMessageType,
};

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SetCommand;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum SetArguments {
    NX,             // -- Only set the key if it does not already exist.
    XX,             // -- Only set the key if it already exists.
    IFEQ, // ifeq-value -- Set the key’s value and expiration only if its current value is equal to ifeq-value. If the key doesn’t exist, it won’t be created.
    IFNE, // ifne-value -- Set the key’s value and expiration only if its current value is not equal to ifne-value. If the key doesn’t exist, it will be created.
    IFDEQ, // ifeq-digest -- Set the key’s value and expiration only if the hash digest of its current value is equal to ifeq-digest. If the key doesn’t exist, it won’t be created. See the Hash Digest section below for more information.
    IFDNE, // ifne-digest -- Set the key’s value and expiration only if the hash digest of its current value is not equal to ifne-digest. If the key doesn’t exist, it will be created. See the Hash Digest section below for more information.
    GET, // -- Return the old string stored at key, or nil if key did not exist. An error is returned and SET aborted if the value stored at key is not a string.
    EX(Duration), // seconds -- Set the specified expire time, in seconds (a positive integer).
    PX(Duration), // milliseconds -- Set the specified expire time, in milliseconds (a positive integer).
    EXAT(Duration), // imestamp-seconds -- Set the specified Unix time at which the key will expire, in seconds (a positive integer).
    PXAT(Duration), // imestamp-milliseconds -- Set the specified Unix time at which the key will expire, in milliseconds (a positive integer).
    KEEPTTL,        // -- Retain the time to live associated with the key.
}

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

        let old_value = get_db().get(&key);

        let arguments = match parse_arguments(args) {
            Ok(val) => val,
            Err(err) => return err,
        };

        let expiry = arguments.iter().find(|arg| arg.is_expiry_argument()).map(|v| match v {
            SetArguments::EX(value)
            | SetArguments::PX(value)
            | SetArguments::EXAT(value)
            | SetArguments::PXAT(value) => *value,
            SetArguments::KEEPTTL => unimplemented!("Needs impl"),
            _ => unreachable!(),
        });

        let set_logic_argument = arguments.iter().find(|arg| arg.is_set_logic_argument());
        let return_value_argument = arguments.iter().find(|arg| arg.is_get_logic_argument()).is_some();


        debug!("{:?}", arguments);

        match set_logic_argument {
            Some(val) => match val {
                SetArguments::NX => {
                    if old_value.is_some() {
                        let error = format!("Not setting value for key: '{}' due to 'NX' argument (create only command) and exsisting value.", &key);
                        trace!("{}", error);
                        return RedisMessageType::error(error);
                    }
                    get_db().set(key, DataUnit::new(save_value, expiry));
                },
                SetArguments::XX => {
                    if old_value.is_none() {
                        let error = format!("Not setting value for key: '{}' due to 'XX' argument (update only command) and non exsisting value.", &key);
                        trace!("{}", error);
                        return RedisMessageType::error(error);
                    }
                    get_db().set(key, DataUnit::new(save_value, expiry));
                },
                _ => unreachable!(),
            },
            None => get_db().set(key, DataUnit::new(save_value, expiry))
        }

        
        if return_value_argument {
            if let Some(old) = old_value {
                return RedisMessageType::SimpleString(old);
            };
        }
                
        let mut return_value = RedisMessageType::SimpleString("OK".into());
        return return_value;
    }
}

impl SetArguments {
    fn decode(string: &String) -> Result<SetArguments, RedisMessageType> {
        match string.as_str() {
            "NX" => Ok(SetArguments::NX),
            "XX" => Ok(SetArguments::XX),
            "IFEQ" => Ok(SetArguments::IFEQ),
            "IFNE" => Ok(SetArguments::IFNE),
            "IFDEQ" => Ok(SetArguments::IFDEQ),
            "IFDNE" => Ok(SetArguments::IFDNE),
            "GET" => Ok(SetArguments::GET),
            "EX" => Ok(SetArguments::EX(Duration::from_secs(0))),
            "PX" => Ok(SetArguments::PX(Duration::from_secs(0))),
            "EXAT" => Ok(SetArguments::EXAT(Duration::from_secs(0))),
            "PXAT" => Ok(SetArguments::PXAT(Duration::from_secs(0))),
            "KEEPTTL" => Ok(SetArguments::KEEPTTL),
            unknown_value => Err(RedisMessageType::error(format!(
                "Unknown Set command argument: {}",
                unknown_value
            ))),
        }
    }

    fn is_expiry_argument(&self) -> bool {
        return match self {
            SetArguments::KEEPTTL
            | SetArguments::EX(_)
            | SetArguments::PX(_)
            | SetArguments::EXAT(_)
            | SetArguments::PXAT(_) => true,
            _ => false,
        };
    }

    fn is_set_logic_argument(&self) -> bool {
        return match self {
            SetArguments::NX | SetArguments::XX => true,
            _ => false,
        };
    }

    fn is_get_logic_argument(&self) -> bool {
        return match self {
            SetArguments::GET => true,
            _ => false,
        };
    }
}

fn parse_arguments(args: &[RedisMessageType]) -> Result<Vec<SetArguments>, RedisMessageType> {
    let mut arguments = Vec::new();

    let arg_buffer = &args[2..];
    if arg_buffer.is_empty() {
        return Ok(arguments);
    }

    let mut i: usize = 0;
    while i < arg_buffer.len() {
        let mut argument = match arg_buffer
            .get(i)
            .expect("Issue while parsing arguments on set command. Should never happen")
        {
            RedisMessageType::BulkString(val) => SetArguments::decode(&val)?,
            _ => {
                return Err(RedisMessageType::error(format!(
                    "Expected to recieve a BulkString type argument not {}",
                    arg_buffer.get(i).unwrap()
                )))
            }
        };

        if argument.is_expiry_argument() {
            i += 1;
            let expiry_value = parse_expiry(arg_buffer.get(i))?;
            argument = match argument {
                SetArguments::EX(_val) => SetArguments::EX(Duration::from_secs(expiry_value)),
                SetArguments::PX(_val) => SetArguments::PX(Duration::from_millis(expiry_value)),
                SetArguments::EXAT(_val) => SetArguments::EXAT(expiry_from_unix_secs(expiry_value)),
                SetArguments::PXAT(_val) => SetArguments::PXAT(expiry_from_unix_millis(expiry_value)),
                val => val,
            };

        }

        arguments.push(argument);
        i += 1;
    }

    let mut expiry_argument_present = false;
    let mut set_logic_present = false;
    for arg in &arguments {
        if arg.is_expiry_argument() {
            if expiry_argument_present {
                return Err(RedisMessageType::error("Invalid SET command. Recieved multiple expiry arguments, only one of {'EX', 'PX', 'EXAT', 'PXAT'} is allowed."));
            };
            expiry_argument_present = true;
        };
        if arg.is_set_logic_argument() {
            if set_logic_present {
                return Err(RedisMessageType::error("Invalid SET command. Recieved both {'NX', 'XX'} only one of them may be selected!"))
            }
            set_logic_present = true;
        }
    }

    return Ok(arguments);
}

fn parse_expiry(arg: Option<&RedisMessageType>) -> Result<u64, RedisMessageType> {
    let arg = arg.ok_or_else(|| RedisMessageType::error("invalid"))?;

    let numeric_value = match arg {
        RedisMessageType::BulkString(val) => match u64::from_str_radix(val, 10) {
            Ok(val) => val.max(0) as u64,
            Err(_) => {
                return Err(RedisMessageType::Error(format!(
                    "SET with expiry argument expects a Bulk String numeric value argument. Not {}!",
                    val
                )))
            }
        },
        val => {
            return Err(RedisMessageType::Error(format!(
                "SET with expiry argument expects a Bulk String numeric value argument! Argument is of type {}",
                val
            )))
        }
    };

    trace!("Expiration duration: {:?}", numeric_value);

    return Ok(numeric_value);
}

// todo: needs overhaul!
pub fn expiry_from_unix_secs(unix_secs: u64) -> Duration {
    let now_system = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System time before Unix epoch");

    let delta = unix_secs - now_system.as_secs();
    return Duration::from_secs(delta);
}

pub fn expiry_from_unix_millis(unix_millis: u64) -> Duration {
    let now_system = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System time before Unix epoch");

    let now_millis = now_system.as_millis();

    // truncating u64 -> u64 is safe for reasonable timestamps
    let delta = unix_millis - now_millis as u64;
    return Duration::from_millis(delta);
}

