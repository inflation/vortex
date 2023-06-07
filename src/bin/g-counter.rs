use std::sync::Arc;

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::Level;
use vortex::{
    message::Message,
    node::Node,
    service::{handle_seqkv, SeqKv},
};

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Payload {
    Add { delta: u64 },
    AddOk,
    Read,
    ReadOk { value: u64 },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(Level::DEBUG)
        .with_writer(std::io::stderr)
        .init();

    let (node, mut rx) = Node::new_arc()?;

    while let Some(msg) = rx.recv().await {
        tokio::spawn(handle_msg(msg, node.clone()));
    }

    Ok(())
}

async fn handle_msg(msg: Message<Value>, node: Arc<Node>) -> anyhow::Result<()> {
    match msg.src.as_str() {
        "seq-kv" => handle_seqkv(msg, node.as_ref()).await?,
        _ => match Payload::deserialize(&msg.body.payload)? {
            Payload::Add { delta } => {
                let val = node
                    .rpc("seq-kv".into(), SeqKv::Read { key: "val".into() })
                    .await?
                    .as_u64()
                    .context("Invalid number")?;
                node.rpc(
                    "seq-kv".into(),
                    SeqKv::Cas {
                        key: "val".into(),
                        from: val.into(),
                        to: (val + delta).into(),
                    },
                )
                .await?;
                node.reply(&msg, Payload::AddOk).await?;
            }
            Payload::AddOk => {}
            Payload::Read => {
                let reply = node
                    .rpc("seq-kv".into(), SeqKv::Read { key: "val".into() })
                    .await?;
                let value = match reply {
                    Value::Null => {
                        node.rpc(
                            "seq-kv".into(),
                            SeqKv::Write {
                                key: "val".into(),
                                value: 0.into(),
                            },
                        )
                        .await?;
                        0
                    }
                    Value::Number(n) => n.as_u64().context("Invalid number")?,
                    _ => bail!("Unexpected value from seq-kv: {:?}", reply),
                };

                node.reply(&msg, Payload::ReadOk { value }).await?
            }
            Payload::ReadOk { .. } => {}
        },
    }

    Ok(())
}
