#![allow(warnings)]

use core::str;
use std::{
    collections::HashMap,
    error::Error,
    io::{self, BufRead, BufReader, ErrorKind, Read, Write},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener, TcpStream},
    result::Result,
    sync::{Arc, RwLock},
};

pub mod consts;
pub mod parser;
pub mod utils;

use crate::parser::messages::RedisMessageType;
use anyhow::anyhow;
use bytes::BytesMut;
use log::{debug, error, info, trace};
use once_cell::sync::Lazy;
use utils::{cli::Args, logger::set_log_level, thread_pool::ThreadPool};

static GLOBAL_MAP: Lazy<Arc<RwLock<HashMap<String, String>>>> =
    Lazy::new(|| Arc::new(RwLock::new(HashMap::new())));

fn main() {
    let args: Args = Args::parse();

    let server_address = SocketAddr::new(args.host, args.port);
    let pool = ThreadPool::new(args.threads.into());

    info!("Starting server");
    let mut listener = match TcpListener::bind(server_address) {
        Ok(server) => server,
        Err(err) => panic!("Unable to bind TcpListener to address: {}", server_address),
    };

    set_log_level(&args);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => pool.execute(|| recieve_message(stream)),
            Err(err) => {
                error!("Error while recieving tcp message: {}", err)
            }
        }
    }
}

/// Reads the data provided in a single TCP message.
fn read_message(stream: &mut TcpStream) -> Result<Vec<u8>, io::Error> {
    let mut data = BytesMut::with_capacity(4096);
    let mut temp_buffer: [u8; 1024] = [0; 1024];

    loop {
        let bytes_read = stream.read(&mut temp_buffer)?;
        trace!("Bytes recieved: {bytes_read}");

        let vals = &temp_buffer.as_slice()[..bytes_read];
        data.extend_from_slice(vals);

        if bytes_read < 1024 || bytes_read == 0 {
            break;
        }
    }

    // data.shrink_to_fit();
    return Ok(data.to_vec());
}

fn recieve_message(mut stream: TcpStream) {
    let peer = stream.peer_addr().unwrap();
    'connection: loop {
        let raw_message = match read_message(&mut stream) {
            Ok(raw_message) => {
                trace!("Successfully read tcp message. {:#?}", raw_message);
                if (raw_message.len() == 0) {
                    info!("No bytes recieved. Closing connection");
                    return;
                }
                raw_message
            }
            Err(err) => {
                match err.kind() {
                    ErrorKind::BrokenPipe => info!("Pipe to client {} broke", peer),
                    _ => error!("Encounterd IO exception while connected to {}", err),
                }
                break 'connection;
            }
        };

        let message_input =
            str::from_utf8(&raw_message).expect("Unable to parse input bytestream to str utf8");
        trace!("Message recieved: {}", message_input);
        let command = RedisMessageType::decode(message_input)
            .expect("unable to parse RedisMessageType from input byte stream")
            .0;

        let response = match command {
            RedisMessageType::Array(val) => process_command_array(val),
            other => panic!(
                "Expected an RedisMessageType::Array as a command input, but got: {}",
                other.to_string()
            ),
        };

        stream.write_all(response.encode().as_bytes());
    }
}

fn process_command_array(array: Vec<RedisMessageType>) -> RedisMessageType {
    let command = array
        .get(0)
        .expect("Redis expects at least a single command!");

    let command = match command {
        RedisMessageType::SimpleString(ref val) | &RedisMessageType::BulkString(ref val) => val,
        other => panic!(
            "Redis requires the first argument to be a string. Recieved a {}",
            other.to_string()
        ),
    }
    .to_uppercase();

    if command == "ECHO" {
        let echo_val = array
            .get(1)
            .expect("Echo expects a second argument to echo!");
        let value = match echo_val {
            RedisMessageType::BulkString(val) | RedisMessageType::SimpleString(val) => {
                val.to_string()
            }
            RedisMessageType::Integer(val) => val.to_string(),
            other => panic!("Redis cannot echo value of type: {}", other.to_string()),
        };

        return RedisMessageType::BulkString(value);
    } else if command == "PING" {
        return RedisMessageType::SimpleString("PONG".into());
    } else if command == "SET" {
        let key = array
            .get(1)
            .expect("SET expects a key argument!")
            .as_string()
            .unwrap();
        let value = array
            .get(2)
            .expect("SET expects a value argument!")
            .as_string()
            .unwrap();
        match GLOBAL_MAP.try_write() {
            Ok(mut val) => {
                val.insert(key, value);
                return RedisMessageType::SimpleString("OK".into());
            }
            Err(err) => {
                let msg = "Unable to set key due to lock!";
                error!("{msg}");
                return RedisMessageType::Error(msg.into());
            }
        }
    } else if command == "GET" {
        let key = array
            .get(1)
            .expect("GET expects a key argument!")
            .as_string()
            .unwrap();
        match GLOBAL_MAP.try_read() {
            Ok(map) => match map.get(&key) {
                Some(val) => return RedisMessageType::BulkString(val.clone()),
                None => return RedisMessageType::NullBulkString,
            },
            Err(err) => {
                let msg = "Unable to read key due to lock!";
                error!("{msg}");
                return RedisMessageType::Error(msg.into());
            }
        }
    } else {
        error!("unknown command {}", command);
        return RedisMessageType::Error(format!("unknown command {}", command).into());
    };
}
