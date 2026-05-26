pub mod cli;
pub mod client;
pub mod core;
pub mod exceptions;
pub mod kinesis;
pub mod time;

pub use crate::core::{AwsLogs, AwsLogsConfig, ColorPreference};
pub use crate::exceptions::AwsLogsError;
pub use crate::kinesis::{KinesisSearch, KinesisSearchConfig};
pub use crate::time::parse_datetime;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
