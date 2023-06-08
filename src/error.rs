use std::fmt::Debug;

use compact_str::{format_compact, CompactString};
use miette::Diagnostic;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
pub enum RpcError {
    KeyNotFound,
    CasFailed,
}

#[derive(Debug, Error, Diagnostic)]
pub enum ExternalError {
    #[error("I/O error")]
    #[diagnostic(code(external::io))]
    Io(#[from] std::io::Error),

    #[error("JSON error")]
    #[diagnostic(code(external::json))]
    Json(#[from] serde_json::Error),

    #[error("Channel error")]
    #[diagnostic(code(external::channel))]
    Channel,
}

impl<T> From<mpsc::error::SendError<T>> for ExternalError {
    fn from(_: mpsc::error::SendError<T>) -> Self {
        ExternalError::Channel
    }
}

impl From<oneshot::error::RecvError> for ExternalError {
    fn from(_: oneshot::error::RecvError) -> Self {
        ExternalError::Channel
    }
}

#[derive(Debug, Error, Diagnostic)]
#[error("Node error: {reason}")]
#[diagnostic(code(node))]
pub struct NodeError {
    pub reason: CompactString,
    pub source: Option<ExternalError>,
}

impl NodeError {
    pub fn new(reason: impl Into<CompactString>) -> Self {
        Self {
            reason: reason.into(),
            source: None,
        }
    }
}

pub trait WithReason<T> {
    fn with_reason(self, reason: impl Into<CompactString>) -> Result<T, NodeError>;
}

impl<T, U> WithReason<T> for Result<T, U>
where
    U: Into<ExternalError>,
{
    fn with_reason(self, reason: impl Into<CompactString>) -> Result<T, NodeError> {
        self.map_err(|e| NodeError {
            reason: reason.into(),
            source: Some(e.into()),
        })
    }
}

pub trait FromSerde<T> {
    fn map_ser_error(self, val: impl Debug) -> Result<T, NodeError>;
}

impl<T> FromSerde<T> for Result<T, serde_json::Error> {
    fn map_ser_error(self, val: impl Debug) -> Result<T, NodeError> {
        self.map_err(|e| NodeError {
            reason: format_compact!("Failed to serialize: {val:#?}"),
            source: Some(e.into()),
        })
    }
}
