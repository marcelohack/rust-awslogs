use thiserror::Error;

#[derive(Debug, Error)]
pub enum AwsLogsError {
    #[error("awslogs doesn't understand '{0}' as a date.")]
    UnknownDate(String),

    #[error(
        "The number of streams that match your pattern '{pattern}' is '{count}'. \
         AWS API limits the number of streams you can filter by to {limit}.\
         It might be helpful to you to not filter streams by any pattern and filter the output of awslogs."
    )]
    TooManyStreamsFiltered {
        pattern: String,
        count: usize,
        limit: usize,
    },

    #[error("No streams match your pattern '{0}' for the given time period.")]
    NoStreamsFiltered(String),

    #[error(transparent)]
    Aws(#[from] anyhow::Error),
}

impl AwsLogsError {
    /// Exit code matching the Python implementation.
    pub fn code(&self) -> i32 {
        match self {
            AwsLogsError::UnknownDate(_) => 3,
            AwsLogsError::TooManyStreamsFiltered { .. } => 6,
            AwsLogsError::NoStreamsFiltered(_) => 7,
            AwsLogsError::Aws(_) => 1,
        }
    }

    /// Hint string (alias of Display) — matches Python's `exc.hint()`.
    pub fn hint(&self) -> String {
        self.to_string()
    }
}
