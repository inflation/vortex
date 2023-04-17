use std::sync::{atomic::Ordering, Arc};

use anyhow::bail;
use serde::{Deserialize, Serialize};
use vortex::{message::Message, node::Node};

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Payload {
    Generate,
    GenerateOk { id: String },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let node = Arc::new(Node::new()?);
    while let Some(msg) = node.in_chan.lock().await.recv().await {
        tokio::spawn(handle_msg(msg, node.clone()));
    }

    Ok(())
}

async fn handle_msg(msg: Message<Payload>, node: Arc<Node<Payload>>) -> anyhow::Result<()> {
    match &msg.body.payload {
        Payload::Generate => {
            let id = format!("{}-{}", node.id, node.msg_id.load(Ordering::Relaxed));
            node.reply(&msg, Payload::GenerateOk { id }).await?;
        }
        _ => bail!("Unexpected msg: {msg:?}"),
    }

    Ok(())
}
