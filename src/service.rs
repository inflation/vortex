use compact_str::format_compact;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{
    error::{JsonDeError, NodeError, RpcError},
    message::Message,
    node::Node,
};

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SeqKv {
    Read {
        key: Value,
    },
    ReadOk {
        value: Value,
    },
    Write {
        key: Value,
        value: Value,
    },
    WriteOk,
    Cas {
        key: Value,
        from: Value,
        to: Value,
        create_if_not_exists: Option<bool>,
    },
    CasOk,
    Error {
        code: u8,
        text: String,
    },
}

pub async fn handle_seqkv(msg: Message<Value>, node: &Node) -> Result<(), NodeError> {
    match SeqKv::de(&msg.body.payload)? {
        SeqKv::ReadOk { value } => node.ack(msg, Ok(value)),
        SeqKv::WriteOk => node.ack(msg, Ok(json!(null))),
        SeqKv::CasOk => node.ack(msg, Ok(json!(null))),
        SeqKv::Error { code, text } => match code {
            20 => node.ack(msg, Err(RpcError::KeyNotFound)),
            22 => node.ack(msg, Err(RpcError::CasFailed)),
            _ => Err(NodeError::new(format_compact!(
                "Error {code} from seq-kv: {text}"
            ))),
        },
        _ => Err(NodeError::new("Unexpected message from seq-kv")),
    }
}
