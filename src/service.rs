use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{
    error::{JsonDeError, NodeError, RpcError},
    message::Message,
    node::Node,
};

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SeqKvRequest {
    Read {
        key: Value,
    },
    Write {
        key: Value,
        value: Value,
    },
    Cas {
        key: Value,
        from: Value,
        to: Value,
        create_if_not_exists: Option<bool>,
    },
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SeqKvResponse {
    ReadOk { value: Value },
    WriteOk,
    CasOk,
    Error { code: u8, text: String },
}

impl Node {
    pub fn handle_seqkv(&self, msg: Message<Value>) -> Result<(), NodeError> {
        match SeqKvResponse::de(&msg.body.payload)? {
            SeqKvResponse::ReadOk { value } => self.ack(msg, Ok(value)),
            SeqKvResponse::WriteOk => self.ack(msg, Ok(json!(null))),
            SeqKvResponse::CasOk => self.ack(msg, Ok(json!(null))),
            SeqKvResponse::Error { code, text } => match code {
                20 => self.ack(msg, Err(RpcError::KeyNotFound(text))),
                22 => self.ack(msg, Err(RpcError::CasFailed(text))),
                c => self.ack(msg, Err(RpcError::Unknown(c, text))),
            },
        }
    }

    pub async fn seqkv_read(
        &self,
        key: impl Into<Value>,
    ) -> Result<Result<Value, RpcError>, NodeError> {
        self.rpc("seq-kv".into(), SeqKvRequest::Read { key: key.into() })
            .await
    }

    pub async fn seqkv_cas(
        &self,
        key: impl Into<Value>,
        from: impl Into<Value>,
        to: impl Into<Value>,
    ) -> Result<Result<Value, RpcError>, NodeError> {
        self.rpc(
            "seq-kv".into(),
            SeqKvRequest::Cas {
                key: key.into(),
                from: from.into(),
                to: to.into(),
                create_if_not_exists: Some(true),
            },
        )
        .await
    }
}
