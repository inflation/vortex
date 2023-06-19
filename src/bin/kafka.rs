use std::{
    collections::{BTreeMap, HashMap},
    future,
    sync::Arc,
};

use compact_str::{format_compact, CompactString};
use dashmap::DashMap;
use futures::{stream::FuturesUnordered, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{debug, instrument};
use vortex::{
    error::{JsonDeError, JsonSerError, NodeError, WithReason},
    init_tracing, main_loop,
    message::Message,
    node::Node,
};

type Logs = HashMap<CompactString, Vec<(u64, u64)>>;
type State = DashMap<CompactString, BTreeMap<u64, u64>>;

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
    Query {
        offsets: HashMap<CompactString, u64>,
    },
    QueryOk {
        query_logs: Logs,
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
        msgs: Logs,
    },
    CommitOffsetsOk {},
    ListCommittedOffsetsOk {
        offsets: HashMap<CompactString, u64>,
    },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> miette::Result<()> {
    init_tracing()?;

    let logs = Arc::new(DashMap::new());
    main_loop(move |msg, node| handle_msg(msg, node, logs))?.await
}

async fn handle_msg(
    msg: Message<Value>,
    node: Arc<Node>,
    logs: Arc<State>,
) -> Result<(), NodeError> {
    match msg.src.as_str() {
        "seq-kv" | "lin-kv" => node.handle_kv(&msg),
        _ => match Request::de(&msg.body.payload)? {
            Request::Send { key, msg: message } => {
                handle_send(key, message, &node, &msg, &logs).await
            }
            Request::Poll { offsets } => handle_poll(offsets, &node, &msg, &logs).await,
            Request::CommitOffsets { offsets } => handle_commit(offsets, &node, &msg).await,
            Request::ListCommittedOffsets { keys } => {
                handle_list_committed(keys, &node, &msg).await
            }
            Request::Query { offsets } => handle_query(offsets, &node, &msg, &logs).await,
            Request::QueryOk { query_logs } => handle_query_ok(&node, &msg, query_logs).await,
        },
    }
}

#[instrument("Send", skip(logs))]
async fn handle_send(
    key: CompactString,
    message: u64,
    node: &Arc<Node>,
    msg: &Message<Value>,
    logs: &Arc<State>,
) -> Result<(), NodeError> {
    let key_offset = format_compact!("{key}:offset");
    let mut kv_offset = node.kv_read("lin-kv", key_offset.as_str()).await?;
    let mut offset = kv_offset.as_ref().map_or_else(|| Ok(0), u64::de)?;
    offset += 1;

    while !node
        .kv_cas("lin-kv", key_offset.as_str(), kv_offset, offset)
        .await?
    {
        let s = node
            .kv_read("lin-kv", key_offset.as_str())
            .await?
            .with_reason("Failed to read after CAS")?;
        offset = u64::de(&s)?;
        offset += 1;

        kv_offset = Some(s);
    }
    logs.entry(key).or_default().insert(offset, message);

    node.reply(msg, Response::SendOk { offset }).await
}

#[instrument("Poll")]
async fn handle_poll(
    offsets: HashMap<CompactString, u64>,
    node: &Arc<Node>,
    msg: &Message<Value>,
    logs: &Arc<State>,
) -> Result<(), NodeError> {
    let mut queries = node
        .node_ids
        .iter()
        .filter(|&id| id != &node.id)
        .map(|id| async {
            node.rpc(
                id.clone(),
                Request::Query {
                    offsets: offsets.clone(),
                },
            )
            .await
            .and_then(|q| q.with_reason("Failed to query").and_then(Logs::de))
        })
        .collect::<FuturesUnordered<_>>()
        .try_fold(
            HashMap::new(),
            |mut acc: HashMap<CompactString, BTreeMap<u64, u64>>, logs| {
                for (key, log) in logs {
                    acc.entry(key).or_default().extend(log);
                }
                future::ready(Ok(acc))
            },
        )
        .await?;
    offsets.into_iter().for_each(|(key, offset)| {
        if let Some(log) = logs.get(&key) {
            queries.entry(key).or_default().extend(log.range(offset..))
        }
    });

    let msgs = queries
        .into_iter()
        .map(|(key, val)| (key, val.into_iter().collect()))
        .collect();

    node.reply(msg, Response::PollOk { msgs }).await
}

#[instrument("Commit Offsets")]
async fn handle_commit(
    offsets: HashMap<CompactString, u64>,
    node: &Arc<Node>,
    msg: &Message<Value>,
) -> Result<(), NodeError> {
    for (key, val) in offsets {
        let key = format_compact!("{key}:committed");
        let kv_committed = node.kv_read("lin-kv", key.as_str()).await?;

        if let Some(mut kv_committed) = kv_committed {
            let mut committed = u64::de(&kv_committed)?;
            if committed >= val {
                debug!("Already committed");
                continue;
            }

            while !node
                .kv_cas("lin-kv", key.as_str(), kv_committed, val)
                .await?
            {
                let s = node
                    .kv_read("lin-kv", key.as_str())
                    .await?
                    .with_reason("Failed to read after CAS")?;

                committed = u64::de(&s)?;
                if committed >= val {
                    debug!("Already committed");
                    break;
                }

                kv_committed = val.into();
            }
        }
    }

    node.reply(msg, Response::CommitOffsetsOk {}).await
}

#[instrument("List Committed Offsets")]
async fn handle_list_committed(
    keys: Vec<CompactString>,
    node: &Arc<Node>,
    msg: &Message<Value>,
) -> Result<(), NodeError> {
    let offsets = keys
        .into_iter()
        .map(|key| async move {
            let k = format_compact!("{key}:committed");
            let kv_committed = node.kv_read("lin-kv", k.as_str()).await;
            kv_committed
                .transpose()
                .map(|v| v.and_then(u64::de).map(|v| (k, v)))
        })
        .collect::<FuturesUnordered<_>>()
        .filter_map(future::ready)
        .try_collect()
        .await?;

    node.reply(msg, Response::ListCommittedOffsetsOk { offsets })
        .await
}

#[instrument("Quey", skip(logs))]
async fn handle_query(
    offsets: HashMap<CompactString, u64>,
    node: &Node,
    msg: &Message<Value>,
    logs: &Arc<State>,
) -> Result<(), NodeError> {
    let query_logs = offsets
        .into_iter()
        .filter_map(|(k, v)| {
            logs.get(&k)
                .map(|log| (k, log.range(v..).map(|(&k, &v)| (k, v)).collect::<Vec<_>>()))
        })
        .collect();

    node.reply(msg, Request::QueryOk { query_logs }).await
}

#[instrument("Query Ok")]
async fn handle_query_ok(
    node: &Node,
    msg: &Message<Value>,
    query_logs: Logs,
) -> Result<(), NodeError> {
    node.ack(msg, Ok(query_logs.ser_val()?))
}
