#![allow(warnings)]

use core::str;
use std::{
    error::Error,
    io::{self, BufRead, BufReader, ErrorKind, Read, Write},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener, TcpStream},
    result::Result,
};

pub mod consts;
pub mod parser;
pub mod utils;

use crate::parser::messages::RedisMessageType;
use anyhow::anyhow;
use bytes::BytesMut;
use log::{error, info, trace};
use utils::{cli::Args, logger::set_log_level, thread_pool::ThreadPool};

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
fn process_message(stream: &mut TcpStream) -> Result<Vec<u8>, io::Error> {
    let mut buffer = BytesMut::with_capacity(4096);

    loop {
        buffer.reserve(1024);

        let bytes_read = stream.read(&mut buffer)?;

        if bytes_read == 0 {
            break;
        }
    }

    return Ok(buffer.to_vec());
}

fn recieve_message(mut stream: TcpStream) {
    let peer = stream.peer_addr().unwrap();
    'connection: loop {
        let raw_message = match process_message(&mut stream) {
            Ok(raw_message) => {
                trace!("Successfully read tcp message. {:#?}", raw_message);
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
        let command = RedisMessageType::decode(message_input)
            .expect("unable to parse RedisMessageType from input byte stream")
            .0;

        let response = match command {
            RedisMessageType::Array(val) => process_command_array(val),
            other => panic!("Expected an RedisMessageType::Array as a command input, but got: {}", other.to_string())
        };


        stream.write_all(response.encode().as_bytes());
    }
}


fn process_command_array(array: Vec<RedisMessageType>) -> RedisMessageType {

    let command = array.get(0).expect("Redis expects at least a single command!");

    let command = match command {
        RedisMessageType::SimpleString(val) | &RedisMessageType::BulkString(val) => val,
        other => panic!("Redis requires the first argument to be a string. Recieved a {}", other.to_string())
    }.to_uppercase();

    let response = if command == "ECHO" {
        let echo_val = array.get(1).expect("Echo expects a second argument to echo!");
        let value = match echo_val {
            RedisMessageType::BulkString(val) | RedisMessageType::SimpleString(val) => val.to_string(),
            RedisMessageType::Integer(val) => val.to_string(),
            other => panic!("Redis cannot echo value of type: {}", other.to_string())
        };

        RedisMessageType::BulkString(value)

    } else if command == "PING" {
        RedisMessageType::SimpleString("PONG".into())
    } else {
        panic!("unknown command {}", command)
    };

    return response;
}