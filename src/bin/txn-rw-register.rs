use std::sync::Arc;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_tuple::{Deserialize_tuple, Serialize_tuple};
use tracing::instrument;
use vortex::{
    error::{JsonDeError, NodeError},
    init_tracing, main_loop,
    message::Message,
    node::Node,
};

type State = DashMap<u64, u64>;

#[derive(Deserialize, Serialize, Debug, Clone)]
enum OpType {
    #[serde(rename = "r")]
    Read,
    #[serde(rename = "w")]
    Write,
}

#[derive(Deserialize_tuple, Serialize_tuple, Debug, Clone)]
struct Op {
    kind: OpType,
    key: u64,
    val: Option<u64>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Request {
    Txn { txn: Vec<Op> },
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Response {
    TxnOk { txn: Vec<Op> },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> miette::Result<()> {
    init_tracing()?;

    let state = Arc::new(State::new());
    main_loop(move |msg, node| handle_msg(msg, node, state))?.await
}

async fn handle_msg(
    msg: Message<Value>,
    node: Arc<Node>,
    state: Arc<State>,
) -> Result<(), NodeError> {
    match msg.src.as_str() {
        "seq-kv" | "lin-kv" => node.handle_kv(&msg),
        _ => match Request::de(&msg.body.payload)? {
            Request::Txn { txn } => handle_txn(txn, &node, &msg, &state).await,
        },
    }
}

#[instrument("Txn", skip(txn))]
async fn handle_txn(
    mut txn: Vec<Op>,
    node: &Arc<Node>,
    msg: &Message<Value>,
    state: &Arc<State>,
) -> Result<(), NodeError> {
    txn.iter_mut().for_each(|op| match &op.kind {
        OpType::Read => op.val = state.get(&op.key).map(|v| *v),
        OpType::Write => {
            if let Some(val) = op.val {
                state.insert(op.key, val);
            }
        }
    });

    node.reply(msg, Response::TxnOk { txn }).await
}
