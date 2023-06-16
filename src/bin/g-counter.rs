use std::sync::Arc;

use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{info, instrument};
use vortex::{
    error::{JsonDeError, NodeError},
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
    init_tracing()?;

    let (node, mut rx) = Node::new_arc()?;
    let (c_tx, mut c_rx) = tokio::sync::mpsc::channel(1);

    loop {
        tokio::select! {
            msg = rx.recv() => match msg {
                Some(msg) => {
                    let node = node.clone();
                    let c_tx = c_tx.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_msg(msg, node, ).await {
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
        "seq-kv" => node.handle_kv(msg),
        _ => match Request::de(&msg.body.payload)? {
            Request::Add { delta } => handle_add(delta, &node, &msg).await,
            Request::Read => handle_read(node, msg).await,
        },
    }
}

#[instrument("Add", skip(msg))]
async fn handle_add(delta: u64, node: &Arc<Node>, msg: &Message<Value>) -> Result<(), NodeError> {
    let id = node.id.as_str();
    let val = node
        .kv_read("seq-kv", id)
        .await?
        .map(u64::de)
        .unwrap_or(Ok(0))?;
    node.kv_write("seq-kv", id, val + delta).await?;

    node.reply(msg, Response::AddOk).await
}

#[instrument("Read", skip(msg))]
async fn handle_read(node: Arc<Node>, msg: Message<Value>) -> Result<(), NodeError> {
    node.kv_write(
        "seq-kv",
        format!("barrier:{}", rand::thread_rng().gen::<u32>()),
        0,
    )
    .await?;

    let mut value = 0;
    for id in &node.node_ids {
        value += match node.kv_read("seq-kv", id.as_str()).await? {
            Some(v) => u64::de(v)?,
            None => {
                info!("Key not found: {id}");
                0
            }
        };
    }
    node.reply(&msg, Response::ReadOk { value }).await
}
