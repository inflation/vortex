use std::sync::{atomic::Ordering, Arc};

use compact_str::format_compact;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use vortex::{
    error::{FromSerde, NodeError},
    init_tracing,
    message::Message,
    node::Node,
};

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Payload {
    Generate,
    GenerateOk { id: String },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> miette::Result<()> {
    init_tracing();

    let (node, mut rx) = Node::new_arc()?;
    let (c_tx, mut c_rx) = tokio::sync::mpsc::channel(1);
    loop {
        tokio::select! {
            msg = rx.recv() => match msg {
                Some(msg) => {
                    let node = node.clone();
                    let c_tx = c_tx.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_msg(msg, node).await {
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
    match Payload::deserialize(&msg.body.payload).map_ser_error(&msg.body.payload)? {
        Payload::Generate => {
            let id = format!("{}-{}", node.id, node.msg_id.load(Ordering::Relaxed));
            node.reply(&msg, Payload::GenerateOk { id }).await?;
        }
        _ => return Err(NodeError::new(format_compact!("Unexpected msg: {msg:?}"))),
    }

    Ok(())
}
