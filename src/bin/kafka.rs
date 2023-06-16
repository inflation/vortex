use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use compact_str::CompactString;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_tuple::{Deserialize_tuple, Serialize_tuple};
use tracing::instrument;
use vortex::{
    error::{JsonDeError, JsonSerError, NodeError, WithReason},
    init_tracing,
    message::Message,
    node::Node,
};

#[derive(Deserialize_tuple, Serialize_tuple, Debug, Clone)]
struct Log {
    offset: u64,
    message: u64,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Request {
    Send {
        key: CompactString,
        msg: u64,
    },
    Poll {
        offsets: HashMap<CompactString, u64>,
    },
    CommitOffsets {
        offsets: HashMap<CompactString, u64>,
    },
    ListCommittedOffsets {
        keys: Vec<CompactString>,
    },
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
#[allow(clippy::enum_variant_names)]
enum Response {
    SendOk {
        offset: u64,
    },
    PollOk {
        msgs: HashMap<CompactString, Vec<Log>>,
    },
    CommitOffsetsOk {},
    ListCommittedOffsetsOk {
        offsets: HashMap<CompactString, u64>,
    },
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
struct State {
    logs: BTreeMap<u64, u64>,
    offset: u64,
    committed_offset: u64,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> miette::Result<()> {
    init_tracing()?;

    let (node, mut rx) = Node::new_arc()?;
    let (c_tx, mut c_rx) = tokio::sync::mpsc::channel(1);

    loop {
        tokio::select! {
            msg = rx.recv() => match msg {
                Some(msg) => {
                    let node = node.clone();
                    let c_tx = c_tx.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_msg(msg, node, ).await {
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

    opentelemetry::global::shutdown_tracer_provider();
    Ok(())
}

async fn handle_msg(msg: Message<Value>, node: Arc<Node>) -> Result<(), NodeError> {
    match msg.src.as_str() {
        "seq-kv" | "lin-kv" => node.handle_kv(msg),
        _ => match Request::de(&msg.body.payload)? {
            Request::Send { key, msg: message } => handle_send(key, message, &node, &msg).await,
            Request::Poll { offsets } => handle_poll(offsets, &node, &msg).await,
            Request::CommitOffsets { offsets } => handle_commit(offsets, &node, &msg).await,
            Request::ListCommittedOffsets { keys } => handle_list_committed(keys, node, msg).await,
        },
    }
}

#[instrument("Send", skip(node, msg))]
async fn handle_send(
    key: CompactString,
    message: u64,
    node: &Arc<Node>,
    msg: &Message<Value>,
) -> Result<(), NodeError> {
    let mut kv_state = node.kv_read("lin-kv", key.as_str()).await?;
    let mut state = kv_state
        .as_ref()
        .map_or_else(|| Ok(State::default()), State::de)?;
    state.logs.insert(state.offset, message);
    while !node
        .kv_cas("lin-kv", key.as_str(), kv_state, state.ser_val()?)
        .await?
    {
        let s = node
            .kv_read("lin-kv", key.as_str())
            .await?
            .with_reason("Failed to read after CAS")?;

        state = State::de(&s)?;
        state.offset += 1;
        state.logs.insert(state.offset, message);

        kv_state = Some(s);
    }

    node.reply(
        msg,
        Response::SendOk {
            offset: state.offset,
        },
    )
    .await
}

#[instrument("Poll", skip(node, msg))]
async fn handle_poll(
    offsets: HashMap<CompactString, u64>,
    node: &Arc<Node>,
    msg: &Message<Value>,
) -> Result<(), NodeError> {
    let mut msgs = HashMap::new();
    for (key, val) in offsets {
        let kv_state = node.kv_read("lin-kv", key.as_str()).await?;

        if let Some(state) = kv_state {
            let s = State::de(&state)?;
            msgs.insert(
                key,
                s.logs
                    .range(val..)
                    .map(|(&offset, &message)| Log { offset, message })
                    .collect(),
            );
        }
    }

    node.reply(msg, Response::PollOk { msgs }).await
}

#[instrument("Commit Offsets", skip(node, msg))]
async fn handle_commit(
    offsets: HashMap<CompactString, u64>,
    node: &Arc<Node>,
    msg: &Message<Value>,
) -> Result<(), NodeError> {
    for (key, val) in offsets {
        let kv_state = node.kv_read("lin-kv", key.as_str()).await?;

        if let Some(mut kv_state) = kv_state {
            let mut state = State::de(&kv_state)?;
            state.committed_offset = val;
            while !node
                .kv_cas("lin-kv", key.as_str(), kv_state, state.ser_val()?)
                .await?
            {
                let s = node
                    .kv_read("lin-kv", key.as_str())
                    .await?
                    .with_reason("Failed to read after CAS")?;

                state = State::de(&s)?;
                state.committed_offset = val;

                kv_state = s;
            }
        }
    }

    node.reply(msg, Response::CommitOffsetsOk {}).await
}

#[instrument("List Committed Offsets", skip(node, msg))]
async fn handle_list_committed(
    keys: Vec<CompactString>,
    node: Arc<Node>,
    msg: Message<Value>,
) -> Result<(), NodeError> {
    let mut offsets = HashMap::new();
    for key in keys {
        let kv_state = node.kv_read("lin-kv", key.as_str()).await?;

        if let Some(state) = kv_state {
            let s = State::de(&state)?;
            offsets.insert(key, s.committed_offset);
        }
    }

    node.reply(&msg, Response::ListCommittedOffsetsOk { offsets })
        .await
}
