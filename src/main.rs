#![allow(warnings)]

use core::str;
use log::{debug, error, info, trace};
use std::{
    io::{self, ErrorKind, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    result::Result,
};
use utils::{cli::Args, thread_pool::ThreadPool};

pub mod commands;
pub mod consts;
pub mod db;
pub mod parser;
pub mod utils;

use crate::{
    commands::command::UnparsedCommandType,
    db::data_store::{get_db, init_db, ServerRole},
    parser::messages::RedisMessageType,
    utils::logger::generate_hex_log,
};

fn main() {
    let args: Args = Args::parse();
    init_db(args.get_db_config());

    let server_address = SocketAddr::new(args.host, args.port);
    let pool = ThreadPool::new(args.threads.into());

    match get_db().get_config().replication_data.role {
        ServerRole::Master => (),
        ServerRole::Slave((host, port)) => {
            pool.execute(move || connect_slave_to_master(host, port))
        }
    }

    info!(
        "Starting server with {} threads on ip: {} and port: {}",
        args.threads,
        server_address.ip(),
        server_address.port()
    );
    let listener = match TcpListener::bind(server_address) {
        Ok(server) => server,
        Err(err) => panic!(
            "Unable to bind TcpListener to address: {} due to {}",
            server_address, err
        ),
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
    const BUFFER_SIZE: usize = 1024;
    let mut data = Vec::with_capacity(BUFFER_SIZE * 4); // pre-allocate
    let mut buf = [0u8; BUFFER_SIZE];

    loop {
        let n = stream.read(&mut buf)?;
        trace!("Bytes received: {}", n);

        data.extend_from_slice(&buf[..n]);

        if n < BUFFER_SIZE {
            break; // no more data immediately available or EOF
        }
    }

    Ok(data)
}

fn recieve_message(mut stream: TcpStream) {
    let peer = stream.peer_addr().unwrap();
    'connection: loop {
        let raw_message = match read_message(&mut stream) {
            Ok(raw_message) => {
                trace!(
                    "Successfully read tcp message. {:?}",
                    generate_hex_log(&raw_message)
                );
                if raw_message.is_empty() {
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
        debug!("Message recieved: {:?}", generate_hex_log(&raw_message));

        let response = match process_message(message_input) {
            Ok(message) => message,
            Err(message) => message,
        };

        stream
            .write_all(response.encode().as_bytes())
            .expect("Failed to write to stream. Should never happen!");
    }
}

fn read_simple_string_response(stream: &mut TcpStream) -> String {
    let message = read_message(stream).unwrap();
    let message_input =
        str::from_utf8(&message).expect(format!("Unable to parse input bytestream to str utf8 -> {:?}", message).as_str());
    let parsed_message = RedisMessageType::decode(message_input)
        .expect("unable to parse RedisMessageType from input byte stream")
        .0;

    return match parsed_message {
        RedisMessageType::SimpleString(val) => val,
        _ => panic!("Expected a \"PONG\" response from the master server"),
    };
}

fn process_message(message: &str) -> Result<RedisMessageType, RedisMessageType> {
    let parsed_message = RedisMessageType::decode(message)
        .expect("unable to parse RedisMessageType from input byte stream")
        .0;

    let command: UnparsedCommandType = match parsed_message {
        RedisMessageType::Array(val) => UnparsedCommandType::new(val)?,
        other => panic!(
            "Expected an RedisMessageType::Array as a command input, but got: {}",
            other.to_string()
        ),
    };

    return command.parse()?.execute();
}

fn connect_slave_to_master(master_host: String, master_port: u16) {
    info!("Starting slave to master connection");
    let stream = TcpStream::connect(format!("{}:{}", master_host, master_port))
        .expect("Failed to connect to master!");

    repl_handshake(stream);
}

fn repl_handshake(mut stream: TcpStream) {
    debug!("Handshake 1/3 Sending ping to master");
    {
        let ping = RedisMessageType::bulk_string_array(vec!["PING"]);
        stream
            .write_all(ping.encode().as_bytes())
            .expect("Failed to write to stream. Should never happen!");

        let val = read_simple_string_response(&mut stream);
        if val != "PONG" {
            panic!("Expected a \"PONG\" response from the master server")
        }
    }
    debug!("Handshake 1/3 Successfully completed. PONG response recieved.");

    debug!("Handshake 2/3 Sending replconf to master");
    {
        trace!("Sending replconf 1/2 listenport to master");
        {
            let listen_port = get_db().get_config().current_listening_port;
            let replconf = RedisMessageType::bulk_string_array(vec![
                "REPLCONF",
                "listening-port",
                format!("{}", listen_port).as_str(),
            ]);

            stream
                .write_all(replconf.encode().as_bytes())
                .expect("Failed to write to stream. Should never happen!");

            let val = read_simple_string_response(&mut stream);
            if val != "OK" {
                panic!("Expected a \"OK\" response from the master server")
            }
        }
        trace!("Sending replconf 2/2 capa to master");
        {
            let listen_port = get_db().get_config().current_listening_port;
            let replconf = RedisMessageType::bulk_string_array(vec!["REPLCONF", "capa", "psync2"]);

            stream
                .write_all(replconf.encode().as_bytes())
                .expect("Failed to write to stream. Should never happen!");

            let val = read_simple_string_response(&mut stream);
            if val != "OK" {
                panic!("Expected a \"OK\" response from the master server")
            }
        }
    }
    debug!("Handshake 2/3 Successfully completed. 2/2 REPLCONF responses recieved.");

    debug!("Handshake 3/3 Sending PSYNC to master");
    {
        let command = RedisMessageType::bulk_string_array(vec!["PSYNC", "?", "-1"].into());
        stream
        .write_all(command.encode().as_bytes())
        .expect("Failed to write to stream. Should never happen!");

        let val = read_simple_string_response(&mut stream);
        if !val.starts_with("FULLRESYNC") {
            panic!("Expected a \"FULLRESYNC ...\" response from the master server")
        }
    }
    debug!("Handshake 3/3 Successfully completed. PSYNC response recieved.")
}
