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

#[instrument(skip_all, fields(msg))]
async fn handle_msg(
    msg: Message<Value>,
    node: Arc<Node>,
    messages: Arc<RwLock<SetU32>>,
) -> Result<(), NodeError> {
    match Request::de(&msg.body.payload)? {
        Request::Broadcast { message } => {
            debug!("Broadcasting message");
            node.reply(&msg, Request::BroadcastOk).await?;

            if !messages.read().contains(message) {
                messages.write().insert(message);

                let peers = node.peers.read().clone();
                for peer in peers {
                    if peer == msg.src {
                        continue;
                    }

                    _ = node.rpc(peer, Request::Broadcast { message }).await?;
                }
            }
        }
        Request::BroadcastOk => {
            debug!("Received broadcast_ok");
            node.ack(msg, Ok(json!(null)))?;
        }
        Request::Read => {
            let messages = messages.read().clone();
            node.reply(&msg, Response::ReadOk { messages }).await?;
        }
        Request::Topology { ref topology } => {
            debug!(?topology, "Received topology");
            if let Some(peers) = topology.get(&node.id) {
                *node.peers.write() = peers.clone();
            }
            node.reply(&msg, Response::TopologyOk).await?;
        }
    }

    Ok(())
}
