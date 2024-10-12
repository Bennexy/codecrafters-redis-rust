use crate::parser::cli::Args;
use std::io::Write;

pub fn set_log_level(args: &Args) {
    env_logger::Builder::new()
        .filter_level(args.log_level)
        .format(|buf, record| {
            writeln!(
                buf,
                "{} [{}] {}: {}",
                chrono::Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.level(),
                record.file().unwrap_or("unknown"),
                record.args()
            )
        })
        .init();
}
