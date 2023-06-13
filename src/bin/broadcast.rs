use std::{collections::HashMap, sync::Arc};

use compact_str::CompactString;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tinyset::SetU32;

use tracing::{debug, instrument};
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
    init_tracing();

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

#[instrument(skip_all, fields(?msg))]
async fn handle_msg(
    msg: Message<Value>,
    node: Arc<Node>,
    peers: Arc<RwLock<Vec<CompactString>>>,
    messages: Arc<RwLock<SetU32>>,
    buffer: Arc<RwLock<SetU32>>,
) -> Result<(), NodeError> {
    match Request::de(&msg.body.payload)? {
        Request::Broadcast { message } => {
            debug!("Broadcasting message");
            node.reply(&msg, Request::BroadcastOk).await?;
            buffer.write().insert(message);
        }
        Request::BroadcastOk => {
            debug!("Received broadcast_ok");
            node.ack(msg, Ok(json!(null)))?;
        }
        Request::BroadcastBatch { messages: batch } => {
            debug!("Received broadcast_batch");
            {
                let mut mm = messages.write();
                *mm = &batch | &mm;
                let mut buf = buffer.write();
                *buf = &batch | &buf;
            }
            node.reply(&msg, Request::BroadcastBatchOk).await?;
        }
        Request::BroadcastBatchOk => {
            debug!("Received broadcast_batch_ok");
            node.ack(msg, Ok(json!(null)))?;
        }
        Request::Read => {
            let messages = messages.read().clone();
            node.reply(&msg, Response::ReadOk { messages }).await?;
        }
        Request::Topology { ref topology } => {
            debug!(?topology, "Received topology");
            if let Some(p) = topology.get(&node.id) {
                *peers.write() = p.clone();
            }
            node.reply(&msg, Response::TopologyOk).await?;
        }
    }

    Ok(())
}

async fn handle_batch_sending(
    peers: Arc<RwLock<Vec<CompactString>>>,
    buffer: Arc<RwLock<SetU32>>,
    node: Arc<Node>,
) {
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let pending = std::mem::take(&mut *buffer.write());
        if pending.is_empty() {
            continue;
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
}
