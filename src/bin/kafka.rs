use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use compact_str::CompactString;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_tuple::{Deserialize_tuple, Serialize_tuple};
use tracing::{debug, instrument};
use vortex::{
    error::{JsonDeError, NodeError},
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

#[derive(Debug)]
struct State {
    logs: BTreeMap<u64, u64>,
    offset: u64,
    committed_offset: u64,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> miette::Result<()> {
    init_tracing();

    let (node, mut rx) = Node::new_arc()?;
    let (c_tx, mut c_rx) = tokio::sync::mpsc::channel(1);

    let states = Arc::new(DashMap::new());

    loop {
        tokio::select! {
            msg = rx.recv() => match msg {
                Some(msg) => {
                    let node = node.clone();
                    let c_tx = c_tx.clone();
                    let states = states.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_msg(msg, node, states).await {
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

#[instrument(skip(node, states))]
async fn handle_msg(
    msg: Message<Value>,
    node: Arc<Node>,
    states: Arc<DashMap<CompactString, State>>,
) -> Result<(), NodeError> {
    match msg.src.as_str() {
        "seq-kv" => node.handle_seqkv(msg)?,
        _ => match Request::de(&msg.body.payload)? {
            Request::Send { key, msg: message } => {
                debug!(?key, ?message, "Received log");

                let mut state = states.entry(key).or_insert(State {
                    logs: BTreeMap::new(),
                    offset: 1,
                    committed_offset: 0,
                });

                let offset = state.offset;
                debug!(offset, message, "Send log");
                node.reply(&msg, Response::SendOk { offset }).await?;

                state.logs.insert(offset, message);
                state.offset += 1;
            }
            Request::Poll { offsets } => {
                debug!(?offsets, "Poll logs");

                let msgs = offsets
                    .into_iter()
                    .filter_map(|(key, val)| {
                        states.get(&key).map(|state| {
                            (
                                key,
                                state
                                    .logs
                                    .range(val..)
                                    .map(|(&offset, &message)| Log { offset, message })
                                    .collect(),
                            )
                        })
                    })
                    .collect();
                node.reply(&msg, Response::PollOk { msgs }).await?;
            }

            Request::CommitOffsets { offsets } => {
                debug!(?offsets, "Commit offsets");

                for (key, val) in offsets {
                    if let Some(mut state) = states.get_mut(&key) {
                        state.committed_offset = val;
                    }
                }
                node.reply(&msg, Response::CommitOffsetsOk {}).await?;
            }
            Request::ListCommittedOffsets { keys } => {
                debug!(?keys, "List committed offsets");

                let offsets = keys
                    .into_iter()
                    .filter_map(|key| states.get(&key).map(|state| (key, state.committed_offset)))
                    .collect();

                node.reply(&msg, Response::ListCommittedOffsetsOk { offsets })
                    .await?;
            }
        },
    }

    Ok(())
}
