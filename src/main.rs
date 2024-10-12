#![allow(warnings)]

use std::{
    error::Error,
    io::{self, BufRead, BufReader, ErrorKind, Read, Write},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener, TcpStream},
    result::Result,
};

mod parser;
mod utils;

use bytes::BytesMut;
use log::{error, info, trace};
use parser::cli::Args;
use redis_starter_rust::utils::messages::{RedisMessageType, RespEncoder};
use utils::{logger::set_log_level, thread_pool::ThreadPool};

fn main() {
    let args: Args = Args::parse();

    let server_address = SocketAddr::new(args.host, args.port);
    let pool = ThreadPool::new(args.threads.into());

    info!("Starting server");
    let mut listener = match TcpListener::bind(server_address) {
        Ok(server) => server,
        Err(err) => panic!("Unable to bin TcpListener to arress: {}", server_address),
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
    let mut data = Vec::new();
    let mut temp_buffer: [u8; 1024] = [0; 1024];

    loop {
        let bytes_read = match stream.read(&mut temp_buffer) {
            Ok(val) => val,
            Err(err) => break,
        };

        let vals = &temp_buffer.as_slice()[..bytes_read];
        data.extend_from_slice(vals);

        if bytes_read < 1024 {
            break;
        }
    }

    let string_data = String::from_utf8(data.clone()).unwrap();

    // stream.write_all(format!("Recieved request {:?}", string_data).as_bytes())?;

    return Ok(data);
}

fn recieve_message(mut stream: TcpStream) {
    let peer = stream.peer_addr().unwrap();
    'connection: loop {
        let raw_message = match process_message(&mut stream) {
            Ok(raw_message) => {
                trace!("Successfully read tcp message.");
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

        // let command = RedisMessage::parse(raw_message);

        let message = RedisMessageType::SimpleString("Pong".into());

        stream.write_all(message.encode().as_slice());
    }
}
