use std::sync::Arc;

use anyhow::bail;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::instrument;
use vortex::{message::Message, node::Node};

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Payload {
    Echo { echo: String },
    EchoOk { echo: String },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let node = Arc::new(Node::new()?);

    while let Some(msg) = node.in_chan.lock().await.recv().await {
        tokio::spawn(handle_msg(msg, node.clone()));
    }

    Ok(())
}

#[instrument(skip(node))]
async fn handle_msg(msg: Message<Value>, node: Arc<Node>) -> anyhow::Result<()> {
    match Payload::deserialize(&msg.body.payload)? {
        Payload::Echo { echo } => {
            node.reply(&msg, Payload::EchoOk { echo }).await?;
        }
        _ => bail!("Unexpected msg: {msg:?}"),
    }

    Ok(())
}
