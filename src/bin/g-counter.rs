use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{error, info};
use vortex::{
    error::{JsonDeError, NodeError, RpcError, WithReason},
    init_tracing,
    message::Message,
    node::Node,
};

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Request {
    Add { delta: u64 },
    Read,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Response {
    AddOk,
    ReadOk { value: u64 },
    Error { code: u8, text: String },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> miette::Result<()> {
    init_tracing();

    let (node, mut rx) = Node::new_arc()?;
    let (c_tx, mut c_rx) = tokio::sync::mpsc::channel(1);

    loop {
        tokio::select! {
            msg = rx.recv() => match msg {
                Some(msg) => {
                    let node = node.clone();
                    let c_tx = c_tx.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_msg(msg, node).await {
                            _ = c_tx.send(e).await;
                        }
                    });
                },
                None => break
            },
            err = c_rx.recv() => if let Some(err) = err {
                return Err(err)?;
            }
        }
    }

    Ok(())
}

async fn handle_msg(msg: Message<Value>, node: Arc<Node>) -> Result<(), NodeError> {
    match msg.src.as_str() {
        "seq-kv" => node.handle_seqkv(msg)?,
        _ => match Request::de(&msg.body.payload)? {
            Request::Add { delta } => {
                let id = node.id.as_str();
                let mut val = node
                    .seqkv_read(id)
                    .await?
                    .ok()
                    .map(u64::de)
                    .unwrap_or(Ok(0))?;
                while let Err(RpcError::CasFailed(s)) = node.seqkv_cas(id, val, val + delta).await?
                {
                    info!("CAS failed, retrying: {s}");
                    val = node
                        .seqkv_read(id)
                        .await?
                        .with_reason("Failed to read after a failed CAS")
                        .and_then(u64::de)?;
                }
                node.reply(&msg, Response::AddOk).await?;
            }
            Request::Read => {
                let mut value = 0;
                for id in &node.node_ids {
                    value += match node.seqkv_read(id.as_str()).await? {
                        Ok(v) => u64::de(v)?,
                        Err(RpcError::KeyNotFound(text)) => {
                            error!("Key not found: {text}");
                            node.reply(&msg, Response::Error { code: 20, text }).await?;
                            return Ok(());
                        }
                        Err(e) => {
                            return Err(NodeError {
                                reason: "seq-kv read failed".into(),
                                source: Some(e.into()),
                            })
                        }
                    };
                }

                node.reply(&msg, Response::ReadOk { value }).await?
            }
        },
    }

    Ok(())
}
