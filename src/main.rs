#![allow(warnings)]

use anyhow::anyhow;
use bytes::BytesMut;
use core::str;
use log::{debug, error, info, trace};
use std::{
    io::{self, ErrorKind, Read, Write},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener, TcpStream},
    result::Result,
};
use utils::{cli::Args, logger::set_log_level, thread_pool::ThreadPool};

pub mod consts;
pub mod parser;
pub mod utils;
pub mod commands;
pub mod db;

use crate::{commands::commands::{Command, Execute}, db::data_store::init_db, parser::messages::RedisMessageType, utils::logger::generate_hex_log};

fn main() {
    let args: Args = Args::parse();
    init_db(args.get_db_config());

    let server_address = SocketAddr::new(args.host, args.port);
    let pool = ThreadPool::new(args.threads.into());

    info!("Starting server with {} threads on ip: {} and port: {}", args.threads, server_address.ip(), server_address.port());
    let mut listener = match TcpListener::bind(server_address) {
        Ok(server) => server,
        Err(err) => panic!("Unable to bind TcpListener to address: {}", server_address),
    };


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
        trace!("{:?}", generate_hex_log(&temp_buffer));

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
                trace!("Successfully read tcp message. {:?}", generate_hex_log(&raw_message));
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
        trace!("Message recieved: {}", generate_hex_log(&raw_message));
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

    let (command, args) = array.split_first().expect("Redis expects at least a single command!");

    let command: Command = Command::from(command);
    return command.execute(args);

}
