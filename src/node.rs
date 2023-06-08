use std::{
    io::Write,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};

use compact_str::{format_compact, CompactString};
use dashmap::DashMap;
use parking_lot::RwLock;
use serde_json::Value;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

use crate::{
    error::{ExternalError, FromSerde, NodeError, RpcError, WithReason},
    io::{stdin, stdout},
    message::{Body, Init, InitOk, Message, Payload},
};

#[derive(Debug)]
pub struct Node {
    pub id: CompactString,
    pub msg_id: AtomicU32,
    pub peers: RwLock<Vec<CompactString>>,
    pub out_chan: mpsc::Sender<Message<Value>>,
    pub handles: [JoinHandle<Result<(), ExternalError>>; 2],
    pub pending_reply: DashMap<CompactString, oneshot::Sender<Result<Value, RpcError>>>,
}

impl Node {
    pub fn new() -> Result<(Self, mpsc::Receiver<Message<Value>>), NodeError> {
        let mut line = String::new();
        std::io::stdin()
            .read_line(&mut line)
            .with_reason("Failed to read init message")?;
        let mut output = std::io::stdout().lock();

        let init_msg: Message<Init> =
            serde_json::from_str(&line).with_reason("Failed to parse init message")?;
        let reply = Message {
            src: init_msg.dst,
            dst: init_msg.src,
            body: Body {
                msg_id: None,
                in_reply_to: init_msg.body.msg_id,
                payload: InitOk {},
            },
        };
        let init_ok = serde_json::to_string(&reply).map_ser_error(&reply)?;
        writeln!(&mut output, "{init_ok}").with_reason("Failed to write init_ok to stdout")?;

        let (tx_in, rx_in) = mpsc::channel(8);
        let (tx_out, rx_out) = mpsc::channel(8);

        let stdin = tokio::task::spawn_blocking(|| stdin(tx_in));
        let stdout = tokio::task::spawn_blocking(|| stdout(rx_out));

        Ok((
            Self {
                id: init_msg.body.payload.node_id,
                msg_id: 1.into(),
                peers: RwLock::new(vec![]),
                out_chan: tx_out,
                handles: [stdin, stdout],
                pending_reply: DashMap::new(),
            },
            rx_in,
        ))
    }

    pub fn new_arc() -> Result<(Arc<Self>, mpsc::Receiver<Message<Value>>), NodeError> {
        let (node, rx) = Self::new()?;
        Ok((Arc::new(node), rx))
    }

    pub async fn send(&self, peer: CompactString, msg: impl Payload) -> Result<u32, NodeError> {
        let id = self.msg_id.load(Ordering::Relaxed);
        self.out_chan
            .send(Message {
                src: self.id.clone(),
                dst: peer,
                body: Body {
                    msg_id: Some(id),
                    in_reply_to: None,
                    payload: serde_json::to_value(&msg).map_ser_error(&msg)?,
                },
            })
            .await
            .with_reason("Failed to send message")?;
        self.msg_id.fetch_add(1, Ordering::Relaxed);

        Ok(id)
    }

    pub async fn reply<T>(&self, from: &Message<T>, msg: impl Payload) -> Result<(), NodeError> {
        self.out_chan
            .send(Message {
                src: from.dst.clone(),
                dst: from.src.clone(),
                body: Body {
                    msg_id: Some(self.msg_id.load(Ordering::Relaxed)),
                    in_reply_to: from.body.msg_id,
                    payload: serde_json::to_value(&msg).map_ser_error(&msg)?,
                },
            })
            .await
            .with_reason(format!(
                "Failed to reply to message: {:?}",
                from.body.msg_id
            ))?;
        self.msg_id.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    pub async fn rpc<P>(
        &self,
        peer: CompactString,
        msg: P,
    ) -> Result<Result<Value, RpcError>, NodeError>
    where
        P: Payload,
    {
        let msg_id = self.msg_id.load(Ordering::Relaxed);
        let msg = Message {
            src: self.id.clone(),
            dst: peer.clone(),
            body: Body {
                msg_id: Some(msg_id),
                in_reply_to: None,
                payload: serde_json::to_value(&msg).map_ser_error(&msg)?,
            },
        };
        self.out_chan
            .send(msg.clone())
            .await
            .with_reason("Failed to send initial RPC message")?;
        self.msg_id.fetch_add(1, Ordering::Relaxed);

        let token = format_compact!("{peer}:{msg_id}");
        let (tx, mut rx) = oneshot::channel();
        self.pending_reply.insert(token.clone(), tx);

        loop {
            tokio::select!(
                _ = tokio::time::sleep(Duration::from_secs(1)) => {
                    self.out_chan
                        .send(msg.clone())
                        .await
                        .with_reason("Failed to send retry RPC message")?;
                }
                res = &mut rx => {
                    self.pending_reply.remove(&token);
                    return res.with_reason("Failed to get pending reply");
                }
            )
        }
    }

    pub fn ack(&self, msg: Message<Value>, val: Result<Value, RpcError>) -> Result<(), NodeError> {
        if let Some(reply) = msg.body.in_reply_to {
            let token = format_compact!("seq-kv:{reply}");
            if let Some((_, tx)) = self.pending_reply.remove(&token) {
                if tx.send(val).is_ok() {
                    return Ok(());
                }
            }
        }

        Err(NodeError::new("Failed to ack message"))
    }
}
