use std::sync::Arc;

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{error, Level};
use vortex::{
    error::RpcError,
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
        let node = node.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_msg(msg, node).await {
                error!("{e:?}");
            }
        });
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
                    .map(|v| v.as_u64().context("Invalid number"))
                    .unwrap_or(Ok(0))?;
                _ = node
                    .rpc(
                        "seq-kv".into(),
                        SeqKv::Cas {
                            key: "val".into(),
                            from: val.into(),
                            to: (val + delta).into(),
                            create_if_not_exists: Some(true),
                        },
                    )
                    .await?;
                node.reply(&msg, Payload::AddOk).await?;
            }
            Payload::AddOk => {}
            Payload::Read => {
                let value = match node
                    .rpc("seq-kv".into(), SeqKv::Read { key: "val".into() })
                    .await?
                {
                    Ok(v) => v.as_u64().context("Invalid number")?,
                    Err(RpcError::KeyNotFound) => 0,
                    reply @ Err(_) => bail!("Unexpected reply from seq-kv: {reply:?}"),
                };

                node.reply(&msg, Payload::ReadOk { value }).await?
            }
            Payload::ReadOk { .. } => {}
        },
    }

    Ok(())
}
