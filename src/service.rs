use anyhow::bail;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{message::Message, node::Node};

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SeqKv {
    Read { key: Value },
    ReadOk { value: Value },
    Write { key: Value, value: Value },
    WriteOk,
    Cas { key: Value, from: Value, to: Value },
    CasOk,
    Error { code: u8, text: String },
}

pub async fn handle_seqkv(msg: Message<Value>, node: &Node) -> anyhow::Result<()> {
    match SeqKv::deserialize(&msg.body.payload)? {
        SeqKv::ReadOk { value } => {
            node.ack(msg, Some(value))?;
        }
        SeqKv::WriteOk => {
            node.ack(msg, None)?;
        }
        SeqKv::CasOk => {
            node.ack(msg, None)?;
        }
        SeqKv::Error { code, text } => {
            if code == 20 {
                node.ack(msg, Some(json!(null)))?;
            } else {
                bail!("Error {code} from seq-kv: {text}");
            }
        }
        _ => bail!("Unexpected message from seq-kv"),
    }

    Ok(())
}
