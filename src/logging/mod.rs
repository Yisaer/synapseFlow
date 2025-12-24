mod cleanup;
mod filename;
mod rolling_file;

use crate::config::LoggingConfig;
use std::io;

pub use cleanup::prune_rotated_logs;
pub use filename::{format_rotated_filename, parse_rotated_filename, RotatedLogName};
pub use rolling_file::RollingFileWriter;

pub enum LogOutput {
    Stdout,
    File(RollingFileWriter),
}

pub fn open_output(cfg: &LoggingConfig) -> io::Result<LogOutput> {
    match cfg.output {
        crate::config::LoggingOutput::Stdout => Ok(LogOutput::Stdout),
        crate::config::LoggingOutput::File => {
            let writer = RollingFileWriter::open(cfg.file.clone())?;
            Ok(LogOutput::File(writer))
        }
    }
}
