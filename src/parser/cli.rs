use std::{
    net::{IpAddr, Ipv4Addr},
    str::FromStr,
};

use log::LevelFilter;

pub struct Args {
    pub host: IpAddr,
    pub port: u16,
    pub threads: u8,
    pub log_level: LevelFilter,
}

impl Args {
    fn print_help() {
        println!("Usage: program_name [options]");
        println!("Options:");
        println!("  --log-level level      Specifies the log-level (default: error)");
        println!("                         Options: trace, debug, info, warn, error, off");
        println!("  --host <hostname>      Specifies the host of the server (default: 127.0.0.1)");
        println!("  --port <port_number>   Specifies the port of the server (default: 6379)");
        println!("  --threads <num>        Specifies the number of threads of the server to run (default: 4)");
        println!("  --replicaof <hostname> <port_number>");
        println!("                         Specifies the host and port of the server to replicate (default: None)");
    }

    pub fn parse() -> Args {
        let mut port: u16 = 6379;
        let mut host: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let mut threads: u8 = 4;
        let mut log_level = LevelFilter::Error;

        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--help" => {
                    Args::print_help();
                    panic!("exit after help");
                }
                "--port" => {
                    port = args
                        .next()
                        .expect("Port must be specified")
                        .parse::<u16>()
                        .expect("Failed to parse port");
                }
                "--host" => {
                    let host_string = args.next().expect("Host Addr must be specified");

                    host = match IpAddr::from_str(&host_string) {
                        Ok(val) => val,
                        Err(err) => {
                            panic!("Given ip is neither ipv4 not ipv6: {}", err);
                        }
                    }
                }

                "--threads" => {
                    threads = args
                        .next()
                        .expect("Thread count must be specified")
                        .parse::<u8>()
                        .expect("Failed to parse thread count");
                }

                "--log-level" => {
                    let raw = args.next().expect("Log Level must be specified");
                    log_level = match LevelFilter::from_str(raw.as_str()) {
                        Ok(val) => val,
                        Err(err) => panic!("{}", err.to_string()),
                    };
                }
                _ => {
                    Args::print_help();
                    panic!("Invalid argument")
                }
            }
        }
        return Args {
            host,
            port,
            threads,
            log_level,
        };
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv6Addr;

    use super::*;

    #[test]
    fn parse_ipv4_address() {
        let result = IpAddr::V4(Ipv4Addr::LOCALHOST);

        assert_eq!(result, IpAddr::from_str("127.0.0.1").unwrap());
    }

    #[test]
    fn parse_ipv6_address() {
        let result = IpAddr::V6(Ipv6Addr::LOCALHOST);

        assert_eq!(result, IpAddr::from_str("::1").unwrap());
    }
}
