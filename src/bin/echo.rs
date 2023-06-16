use std::sync::Arc;

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
    Echo { echo: String },
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Response {
    EchoOk { echo: String },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> miette::Result<()> {
    init_tracing()?;

    main_loop(handle_msg)?.await
}

async fn handle_msg(msg: Message<Value>, node: Arc<Node>) -> Result<(), NodeError> {
    match Request::de(&msg.body.payload)? {
        Request::Echo { echo } => {
            let span = debug_span!("Echo", ?echo);
            node.reply(&msg, Response::EchoOk { echo })
                .instrument(span)
                .await
        }
    }
}
