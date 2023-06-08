use std::{collections::HashMap, sync::Arc};

use compact_str::{format_compact, CompactString};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tinyset::SetU32;

use tracing::{debug, instrument};
use vortex::{
    error::{FromSerde, NodeError},
    init_tracing,
    message::Message,
    node::Node,
};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Payload {
    Broadcast {
        message: u32,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: SetU32,
    },
    Topology {
        topology: HashMap<CompactString, Vec<CompactString>>,
    },
    TopologyOk,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> miette::Result<()> {
    init_tracing();

    let messages = Arc::new(RwLock::new(SetU32::new()));

    let (node, mut rx) = Node::new_arc()?;
    let (c_tx, mut c_rx) = tokio::sync::mpsc::channel(1);
    loop {
        tokio::select! {
            msg = rx.recv() => match msg {
                Some(msg) => {
                    let messages = messages.clone();
                    let node = node.clone();
                    let c_tx = c_tx.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_msg(msg, node, messages).await {
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

#[instrument(skip(node))]
async fn handle_msg(
    msg: Message<Value>,
    node: Arc<Node>,
    messages: Arc<RwLock<SetU32>>,
) -> Result<(), NodeError> {
    match Payload::deserialize(&msg.body.payload).map_ser_error(&msg.body.payload)? {
        Payload::Broadcast { message } => {
            debug!(body = ?msg.body, "Broadcasting message");
            messages.write().insert(message);
            node.reply(&msg, Payload::BroadcastOk).await?;

            let peers = node.peers.read().clone();
            for peer in peers.iter() {
                if peer == &msg.src {
                    continue;
                }

                _ = node
                    .rpc(peer.clone(), Payload::Broadcast { message })
                    .await?;
            }
        }
        Payload::BroadcastOk => {
            debug!(body = ?msg.body, "Received broadcast_ok");
            if let Some(reply) = msg.body.in_reply_to {
                let token = format_compact!("{}:{reply}", msg.src);
                if let Some((_, tx)) = node.pending_reply.remove(&token) {
                    if tx.send(Ok(json!(null))).is_ok() {
                        return Ok(());
                    }
                }
            }

            return Err(NodeError::new("Invalid broadcast_ok"));
        }
        Payload::Read => {
            let messages = messages.read().clone();
            node.reply(&msg, Payload::ReadOk { messages }).await?;
        }
        Payload::ReadOk { messages: _ } => {}
        Payload::Topology { ref topology } => {
            debug!(?topology, "Received topology");
            if let Some(peers) = topology.get(&node.id) {
                *node.peers.write() = peers.clone();
            }
            node.reply(&msg, Payload::TopologyOk).await?;
        }
        Payload::TopologyOk => {}
    }

    Ok(())
}
