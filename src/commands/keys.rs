use crate::{commands::commands::Execute, db::data_store::get_db, RedisMessageType};

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct KeysCommand;

impl Execute for KeysCommand {
    fn execute(&self, args: &[RedisMessageType]) -> RedisMessageType {
        let arg = args
            .get(0)
            .expect("Keys argument should have at least 1 argument!");

        let mut search_string = arg
            .as_string()
            .expect("KEYS first argument (search string) must be of type string!");

        if !search_string.ends_with("*") {
            return RedisMessageType::error("Only '*' is supported for search string patterns!");
        }

        let _ = search_string.pop();
        let prefix = search_string.as_str();

        let data: Vec<RedisMessageType> = get_db()
            .get_all_keys()
            .into_iter()
            .filter(|key| key.starts_with(prefix))
            .map(RedisMessageType::BulkString)
            .collect();

        return RedisMessageType::Array(data);
    }
}

impl KeysCommand {
    pub fn new() -> Self {
        return KeysCommand {};
    }
}
