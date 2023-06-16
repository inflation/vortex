use std::sync::{atomic::Ordering, Arc};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{debug_span, Instrument};
use vortex::{
    error::{JsonDeError, NodeError},
    init_tracing, main_loop,
    message::Message,
    node::Node,
};

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Request {
    Generate,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Response {
    GenerateOk { id: String },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> miette::Result<()> {
    init_tracing()?;

    main_loop(handle_msg)?.await
}

async fn handle_msg(msg: Message<Value>, node: Arc<Node>) -> Result<(), NodeError> {
    match Request::de(&msg.body.payload)? {
        Request::Generate => {
            let id = format!("{}-{}", node.id, node.msg_id.load(Ordering::Relaxed));
            let span = debug_span!("Generate", id = node.id.as_str());
            node.reply(&msg, Response::GenerateOk { id })
                .instrument(span)
                .await
        }
    }
}
