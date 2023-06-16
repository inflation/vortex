use std::{collections::HashMap, sync::Arc};

use compact_str::CompactString;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tinyset::SetU32;

use tracing::{debug_span, instrument, Instrument};
use vortex::{
    error::{JsonDeError, NodeError},
    init_tracing,
    message::Message,
    node::Node,
};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Request {
    Broadcast {
        message: u32,
    },
    BroadcastOk,
    BroadcastBatch {
        messages: SetU32,
    },
    BroadcastBatchOk,
    Read,
    Topology {
        topology: HashMap<CompactString, Vec<CompactString>>,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    ReadOk { messages: SetU32 },
    TopologyOk,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> miette::Result<()> {
    init_tracing()?;

    let peers = Arc::new(RwLock::new(vec![]));
    let messages = Arc::new(RwLock::new(SetU32::new()));
    let buffer = Arc::new(RwLock::new(SetU32::new()));

    let (node, mut rx) = Node::new_arc()?;
    let (c_tx, mut c_rx) = tokio::sync::mpsc::channel(1);

    tokio::spawn(handle_batch_sending(
        peers.clone(),
        buffer.clone(),
        node.clone(),
    ));

    loop {
        tokio::select! {
            msg = rx.recv() => match msg {
                Some(msg) => {
                    let messages = messages.clone();
                    let buffer = buffer.clone();
                    let node = node.clone();
                    let peers = peers.clone();
                    let c_tx = c_tx.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_msg(msg, node, peers, messages, buffer).await {
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

async fn handle_msg(
    msg: Message<Value>,
    node: Arc<Node>,
    peers: Arc<RwLock<Vec<CompactString>>>,
    messages: Arc<RwLock<SetU32>>,
    buffer: Arc<RwLock<SetU32>>,
) -> Result<(), NodeError> {
    match Request::de(&msg.body.payload)? {
        Request::Broadcast { message } => handle_broadcast(&buffer, message, &node, &msg).await,
        Request::BroadcastOk => handle_broadcast_ok(&node, msg),
        Request::BroadcastBatch { messages: batch } => {
            handle_broadcast_batch(&messages, &batch, buffer, &node, &msg).await
        }
        Request::BroadcastBatchOk => handle_broadcast_batch_ok(&node, msg),
        Request::Read => handle_read(messages, &node, &msg).await,
        Request::Topology { ref topology } => handle_topology(topology, &node, peers, msg).await,
    }
}

#[instrument("Broadcast", skip(msg))]
async fn handle_broadcast(
    buffer: &Arc<parking_lot::lock_api::RwLock<parking_lot::RawRwLock, SetU32>>,
    message: u32,
    node: &Arc<Node>,
    msg: &Message<Value>,
) -> Result<(), NodeError> {
    buffer.write().insert(message);
    node.reply(msg, Request::BroadcastOk).await
}

#[instrument("Broadcast Ok", skip(msg))]
fn handle_broadcast_ok(node: &Arc<Node>, msg: Message<Value>) -> Result<(), NodeError> {
    node.ack(msg, Ok(json!(null)))
}

#[instrument("Broadcast Batch", skip(msg))]
async fn handle_broadcast_batch(
    messages: &Arc<RwLock<SetU32>>,
    batch: &SetU32,
    buffer: Arc<RwLock<SetU32>>,
    node: &Arc<Node>,
    msg: &Message<Value>,
) -> Result<(), NodeError> {
    {
        let mut mm = messages.write();
        *mm = batch | &mm;
        let mut buf = buffer.write();
        *buf = batch | &buf;
    }
    node.reply(msg, Request::BroadcastBatchOk).await
}

#[instrument("Broadcast Batch Ok", skip(msg))]
fn handle_broadcast_batch_ok(node: &Arc<Node>, msg: Message<Value>) -> Result<(), NodeError> {
    node.ack(msg, Ok(json!(null)))
}

#[instrument("Read", skip(msg))]
async fn handle_read(
    messages: Arc<RwLock<SetU32>>,
    node: &Arc<Node>,
    msg: &Message<Value>,
) -> Result<(), NodeError> {
    let messages = messages.read().clone();
    node.reply(msg, Response::ReadOk { messages }).await
}

#[instrument("Topology", skip(msg))]
async fn handle_topology(
    topology: &HashMap<CompactString, Vec<CompactString>>,
    node: &Arc<Node>,
    peers: Arc<RwLock<Vec<CompactString>>>,
    msg: Message<Value>,
) -> Result<(), NodeError> {
    if let Some(p) = topology.get(&node.id) {
        *peers.write() = p.clone();
    }
    node.reply(&msg, Response::TopologyOk).await
}

async fn handle_batch_sending(
    peers: Arc<RwLock<Vec<CompactString>>>,
    buffer: Arc<RwLock<SetU32>>,
    node: Arc<Node>,
) {
    loop {
        async {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            let pending = std::mem::take(&mut *buffer.write());
            if pending.is_empty() {
                return;
            }

            let peers = peers.read().clone();
            for peer in peers {
                _ = node
                    .rpc(
                        peer,
                        Request::BroadcastBatch {
                            messages: pending.clone(),
                        },
                    )
                    .await;
            }
        }
        .instrument(debug_span!("Sending batch message", id = node.id.as_str()))
        .await
    }
}
