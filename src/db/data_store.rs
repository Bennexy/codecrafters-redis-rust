use core::str;
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex, RwLock},
    time::{Duration, Instant},
};

use dashmap::DashMap;

use log::info;
use once_cell::sync::{Lazy, OnceCell};

use crate::utils::cli::Args;

// static DB: Lazy<DataStore> = Lazy::new(|| {DataStore::init(DbConfig::empty())});
static DB: OnceCell<DataStore> = OnceCell::new();
pub fn get_db() -> &'static DataStore {
    return DB.get().expect("The db has not been initialized yet. This should never happen!");
}

pub fn init_db(db_config: DbConfig) {
    let data_store = DataStore::init(db_config);
    DB.set(data_store).expect("Config can only be initialized once");
}

#[derive(Debug, Clone)]
pub struct DbConfig {
    pub db_dir: PathBuf,
    pub db_filename: String,
}

impl DbConfig {
    fn empty() -> Self {
        return Self {
            db_dir: PathBuf::new(),
            db_filename: String::new(),
        };
    }

    pub fn new(db_dir: PathBuf, db_filename: String) -> Self {
        return Self { db_dir, db_filename }
    }
}

#[derive(Debug)]
pub struct DataStore {
    db: Arc<DashMap<String, DataUnit>>,
    config: Arc<RwLock<DbConfig>>,
}

impl DataStore {
    fn init(db_config: DbConfig) -> Self {
        return Self {
            db: Arc::new(DashMap::new()),
            config: Arc::new(RwLock::new(db_config)),
        };
    }

    pub fn get_config(&self) -> DbConfig {
        let config = self
            .config
            .read()
            .expect("Unable to get global config. Should never happen");
        return config.clone();
    }

    /// gets the key, if it has expired return None and remove the key from the db.
    pub fn get<S: Into<String>>(&self, key: S) -> Option<String> {

        let key = key.into();
        // needs limited scope, else it will threadlock
        let value = self
            .db
            .get(&key)?
            .clone();

        info!("key {} - is expired: {}!", key, value.is_expired());
        if value.is_expired() {
            self.remove_key(&key);
            return None;
        }

        return Some(value.data);
    }

    fn remove_key<S: Into<String>>(&self, key: S) {
        self.db.remove(&key.into());
    }

    /// Upsets the current HashSet
    pub fn set<S: Into<String>>(&self, key: S, mut value: DataUnit) {
        // Do not change without carefully reading the comments!!!

        self.db.entry(key.into())
            .and_modify(|existing_value| {
                // Update existing value - We "Swap" the old and the new value for performance reasons. Do NOT use value after the .and_modify call! 
                // This will use the old_value and not the expected new value
                std::mem::swap(existing_value, &mut value);
            })
            // .or_insert only is called if the key does not exsist. The usage of value is acceptable here since the values arent swapped
            .or_insert(value); 
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataUnit {
    pub data: String,
    created: Instant,
    expiry_deadline: Option<Instant>,
}

impl DataUnit {
    pub fn new<S: Into<String>>(data: S, ttl: Option<Duration>) -> Self {
        let now = Instant::now();
        let expiry_deadline = match ttl {
            None => None,
            Some(val) => Some(now + val),
        };

        return Self {
            data: data.into(),
            created: now,
            expiry_deadline: expiry_deadline,
        };
    }

    pub fn is_expired(&self) -> bool {
        return self
            .expiry_deadline
            .map(|deadline| Instant::now() >= deadline)
            .unwrap_or(false);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(test)]
    mod test_data_store {

        use crate::db::data_store::{DataStore, DataUnit, DbConfig};
        use std::{
            thread,
            time::{Duration, Instant},
        };

        #[test]
        fn test_set_get_remove() {
            let mut dataStore = DataStore::init(DbConfig::empty());
            dataStore.set("key", DataUnit::new("value", None));


            assert!(dataStore.db.contains_key("key"), "DataStore must contain the key after setting it");
            assert_eq!("value", dataStore.get("key").unwrap(), "DataStore must have the correct value connected to the key");


            dataStore.set("key", DataUnit::new("value2", None));
            assert_eq!("value2", dataStore.get("key").unwrap(), "DataStore must have the overridden value connected to the key");
            
            dataStore.remove_key("key");
            assert!(!dataStore.db.contains_key("key"), "DataStore must not contain the key after removing it");
        }

        #[test]
        fn test_set_get_not_expired() {
            let mut dataStore = DataStore::init(DbConfig::empty());
            let data = DataUnit::new("value", Some(Duration::from_millis(10)));
            dataStore.set(
                "key",
                data,
            );

            assert_eq!("value", dataStore.get("key").unwrap(), "Value should not expire instantly!");
        }

        #[test]
        fn test_set_get_expired_multiple_values() {
            let mut dataStore = DataStore::init(DbConfig::empty());
            let mut data = DataUnit::new("value", Some(Duration::from_secs(10)));
            // Alter the data object after construction
            data.expiry_deadline = Some(data.created - Duration::from_millis(10));
            let data2 = DataUnit::new("value2".to_string(), Some(Duration::from_secs(0)));

            dataStore.set("key",data);
            dataStore.set("key2",data2);
            
            assert!(dataStore.get(&"key".to_string()).is_none(), "Value should be expired!");
            assert!(dataStore.get(&"key".to_string()).is_none(), "Value should be expired!");
            assert!(dataStore.get(&"key2".to_string()).is_none(), "Value should be expired!");

            assert!(!dataStore.db.contains_key("key"));
            assert!(!dataStore.db.contains_key("key2"));
        }
    }


    #[cfg(test)]
    mod concurrency_tests {
        use super::*;
        use std::sync::Arc;
        use std::thread;
        use std::time::Duration;

        #[test]
        fn test_concurrent_set_get() {
            let store = Arc::new(DataStore::init(DbConfig::empty()));

            // Spawn multiple writer threads
            let mut handles = Vec::new();
            for i in 0..100 {
                let store_clone = Arc::clone(&store);
                handles.push(thread::spawn(move || {
                    let key = format!("key{}", i);
                    let value = format!("value{}", i);
                    store_clone.set(key, DataUnit::new(value, None));
                }));
            }

            // Spawn multiple reader threads
            for i in 0..100 {
                let store_clone = Arc::clone(&store);
                handles.push(thread::spawn(move || {
                    let key = format!("key{}", i);
                    let _ = store_clone.get(&key);
                }));
            }

            // Wait for all threads to finish
            for handle in handles {
                handle.join().expect("Thread panicked");
            }

            // Verify that all keys are present
            for i in 0..100 {
                let key = format!("key{}", i);
                assert!(store.db.contains_key(&key));
            }
        }
    }


    #[cfg(test)]
    mod test_data_unit {
        use std::{
            thread,
            time::{Duration, Instant},
        };

        use crate::db::data_store::DataUnit;

        #[test]
        fn test_is_expired_no_expiry() {
            let data = DataUnit {
                data: "data value".into(),
                created: Instant::now(),
                expiry_deadline: None,
            };

            assert!(!data.is_expired(), "Data with no expiry should never expire!");
        }

        #[test]
        fn test_is_expired_some_expiry() {
            let now = Instant::now();
            let mut data = DataUnit {
                data: "data value".into(),
                created: now,
                expiry_deadline: Some(now + Duration::from_millis(50)),
            };
            assert!(!data.is_expired(), "Data should not expire instantly!");
            data.expiry_deadline = Some(now - Duration::from_millis(1));
            assert!(data.is_expired(), "Data should expire after the set durration!");
        }
    }
}
