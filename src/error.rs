use std::{borrow::Borrow, fmt::Debug};

use compact_str::{format_compact, CompactString};
use miette::Diagnostic;
use serde_json::Value;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug, Error, Diagnostic)]
pub enum NodeErrorKind {
    #[error("I/O error")]
    #[diagnostic(code(kind::io))]
    Io(#[from] std::io::Error),

    #[error("JSON error")]
    #[diagnostic(code(kind::json))]
    Json(#[from] serde_json::Error),

    #[error("Channel error")]
    #[diagnostic(code(kind::channel))]
    Channel,

    #[error(transparent)]
    #[diagnostic(transparent)]
    Rpc(#[from] RpcError),
}

impl<T> From<mpsc::error::SendError<T>> for NodeErrorKind {
    fn from(_: mpsc::error::SendError<T>) -> Self {
        NodeErrorKind::Channel
    }
}

impl From<oneshot::error::RecvError> for NodeErrorKind {
    fn from(_: oneshot::error::RecvError) -> Self {
        NodeErrorKind::Channel
    }
}

#[derive(Debug, Error, Diagnostic)]
#[error("Node error: {reason}")]
#[diagnostic(code(node))]
pub struct NodeError {
    pub reason: CompactString,
    pub source: Option<NodeErrorKind>,
}

impl NodeError {
    pub fn new(reason: impl Into<CompactString>) -> Self {
        Self {
            reason: reason.into(),
            source: None,
        }
    }

    pub fn new_with(reason: impl Into<CompactString>, source: impl Into<NodeErrorKind>) -> Self {
        Self {
            reason: reason.into(),
            source: Some(source.into()),
        }
    }
}

pub trait WithReason<T> {
    fn with_reason(self, reason: impl Into<CompactString>) -> Result<T, NodeError>;
}

impl<T, U> WithReason<T> for Result<T, U>
where
    U: Into<NodeErrorKind>,
{
    fn with_reason(self, reason: impl Into<CompactString>) -> Result<T, NodeError> {
        self.map_err(|e| NodeError {
            reason: reason.into(),
            source: Some(e.into()),
        })
    }
}

impl<T> WithReason<T> for Option<T> {
    fn with_reason(self, reason: impl Into<CompactString>) -> Result<T, NodeError> {
        self.ok_or_else(|| NodeError {
            reason: reason.into(),
            source: None,
        })
    }
}

pub trait JsonDeError<T> {
    fn de(src: impl Borrow<Value>) -> Result<T, NodeError>;
}

impl<T> JsonDeError<T> for T
where
    T: serde::de::DeserializeOwned,
{
    #[track_caller]
    fn de(src: impl Borrow<Value>) -> Result<T, NodeError> {
        let src = src.borrow();
        T::deserialize(src).map_err(|e| NodeError {
            reason: format_compact!("Failed to deserialize: {src}"),
            source: Some(e.into()),
        })
    }
}

pub trait JsonSerError<T> {
    #[track_caller]
    fn ser_val(&self) -> Result<Value, NodeError>;
    #[track_caller]
    fn ser_str(&self) -> Result<String, NodeError>;
}

impl<T> JsonSerError<T> for T
where
    T: serde::Serialize + Debug,
{
    fn ser_val(&self) -> Result<Value, NodeError> {
        serde_json::to_value(self).map_err(|e| NodeError {
            reason: format_compact!("Failed to serialize: {self:#?}"),
            source: Some(e.into()),
        })
    }

    fn ser_str(&self) -> Result<String, NodeError> {
        serde_json::to_string(self).map_err(|e| NodeError {
            reason: format_compact!("Failed to serialize: {self:#?}"),
            source: Some(e.into()),
        })
    }
}

#[derive(Debug, Error, Diagnostic)]
#[diagnostic(code(rpc))]
pub enum RpcError {
    #[error("Key not found")]
    KeyNotFound,
    #[error("CAS error: {0}")]
    CasFailed(String),
    #[error("Unknown error, code: {0}")]
    Unknown(u8, String),
}
