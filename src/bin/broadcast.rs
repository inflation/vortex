use std::{collections::HashMap, sync::Arc};

use anyhow::bail;
use compact_str::{format_compact, CompactString};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tinyset::SetU32;

use tracing::{debug, info, instrument, Level};
use vortex::{message::Message, node::Node};

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
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(Level::DEBUG)
        .with_writer(std::io::stderr)
        .init();

    let (node, mut rx) = Node::new_arc()?;
    info!("Starting node...");

    while let Some(msg) = rx.recv().await {
        tokio::spawn(handle_msg(msg, node.clone()));
    }

    Ok(())
}

#[instrument(skip(node))]
async fn handle_msg(msg: Message<Value>, node: Arc<Node>) -> anyhow::Result<()> {
    match Payload::deserialize(&msg.body.payload)? {
        Payload::Broadcast { message } => {
            debug!(body = ?msg.body, "Broadcasting message");
            node.messages.write().insert(message);
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

            bail!("Invalid broadcast_ok");
        }
        Payload::Read => {
            let messages = node.messages.read().clone();
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
