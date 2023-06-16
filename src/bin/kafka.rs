use std::{
    collections::{BTreeMap, HashMap},
    future,
    sync::Arc,
};

use compact_str::CompactString;
use futures::{stream::FuturesUnordered, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::instrument;
use vortex::{
    error::{JsonDeError, JsonSerError, NodeError, WithReason},
    init_tracing, main_loop,
    message::Message,
    node::Node,
};

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
        msgs: HashMap<CompactString, Vec<(u64, u64)>>,
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

    main_loop(handle_msg)?.await
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
    state.offset += 1;
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
    let msgs = offsets
        .into_iter()
        .map(|(key, val)| async move {
            let kv_state = node.kv_read("lin-kv", key.as_str()).await;
            kv_state.transpose().map(|kv_state| {
                kv_state.and_then(|v| {
                    State::de(v).map(|v| {
                        (
                            key,
                            v.logs
                                .range(val..)
                                .map(|(&k, &v)| (k, v))
                                .collect::<Vec<_>>(),
                        )
                    })
                })
            })
        })
        .collect::<FuturesUnordered<_>>()
        .filter_map(future::ready)
        .try_collect()
        .await?;

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
