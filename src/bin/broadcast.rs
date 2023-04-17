use std::{collections::HashMap, sync::Arc};

use serde::{Deserialize, Serialize};
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
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(Level::DEBUG)
        .with_writer(std::io::stderr)
        .init();

    let node = Arc::new(Node::new()?);
    info!("Starting node...");

    while let Some(msg) = node.in_chan.lock().await.recv().await {
        tokio::spawn(handle_msg(msg, node.clone()));
    }

    Ok(())
}

#[instrument(skip(node))]
async fn handle_msg(msg: Message<Payload>, node: Arc<Node<Payload>>) -> anyhow::Result<()> {
    match msg.body.payload {
        Payload::Broadcast { message } => {
            debug!("Broadcasting message {:#?}", msg.body);
            node.messages.lock().insert(message);
            node.reply(&msg, Payload::BroadcastOk).await?;

            let peers = node.peers.lock().clone();
            for peer in peers.iter() {
                if peer == &msg.src {
                    continue;
                }

                node.rpc(peer.clone(), Payload::Broadcast { message })
                    .await?;
            }
        }
        Payload::BroadcastOk => {
            debug!("Received broadcast ok {:#?}", msg.body);
            let token = format!("{}:{}", msg.src, msg.body.in_reply_to.unwrap());
            let (_, tx) = node.pending_broadcasts.remove(&token).unwrap();
            tx.send(()).ok();
        }
        Payload::Read => {
            let messages = node.messages.lock().clone();
            node.reply(&msg, Payload::ReadOk { messages }).await?;
        }
        Payload::ReadOk { messages: _ } => {}
        Payload::Topology { ref topology } => {
            debug!("Received topology {topology:#?}");
            if let Some(peers) = topology.get(&node.id) {
                *node.peers.lock() = peers.clone();
            }
            node.reply(&msg, Payload::TopologyOk).await?;
        }
        Payload::TopologyOk => {}
    }

    Ok(())
}
