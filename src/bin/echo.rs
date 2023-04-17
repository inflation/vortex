use std::sync::Arc;

use anyhow::bail;
use serde::{Deserialize, Serialize};
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
async fn handle_msg(msg: Message<Payload>, node: Arc<Node<Payload>>) -> anyhow::Result<()> {
    match &msg.body.payload {
        Payload::Echo { echo } => {
            node.reply(&msg, Payload::EchoOk { echo: echo.clone() })
                .await?;
        }
        _ => bail!("Unexpected msg: {msg:?}"),
    }

    Ok(())
}
