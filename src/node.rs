use core::fmt;
use std::{
    io::Write,
    sync::atomic::{AtomicU32, Ordering},
    time::Duration,
};

use compact_str::{format_compact, CompactString};
use dashmap::DashMap;
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, instrument, Span};

use crate::{
    error::{JsonSerError, NodeError, RpcError, WithReason},
    io::{stdin, stdout},
    message::{Body, Init, InitOk, Message, Payload},
};

const RPC_LATENCY: Duration = Duration::from_millis(300);

pub struct Node {
    pub id: CompactString,
    pub node_ids: Vec<CompactString>,
    pub msg_id: AtomicU32,
    pub out_chan: mpsc::Sender<Message<Value>>,
    pending_reply: DashMap<CompactString, oneshot::Sender<Result<Value, RpcError>>>,
}

impl Node {
    #[instrument("Init", fields(id))]
    pub fn new() -> Result<(Self, mpsc::Receiver<Message<Value>>), NodeError> {
        let mut line = String::new();
        std::io::stdin()
            .read_line(&mut line)
            .with_reason("Failed to read init message")?;
        let mut output = std::io::stdout().lock();

        let init_msg: Message<Init> =
            serde_json::from_str(&line).with_reason("Failed to parse init message")?;
        debug!(msg = line, "Received init message");
        Span::current().record("id", init_msg.body.payload.node_id.as_str());
        let reply = Message {
            src: init_msg.dst,
            dst: init_msg.src,
            body: Body {
                msg_id: None,
                in_reply_to: init_msg.body.msg_id,
                payload: InitOk {},
            },
        };
        let init_ok = reply.ser_str()?;
        writeln!(&mut output, "{init_ok}").with_reason("Failed to write init_ok to stdout")?;

        let (tx_in, rx_in) = mpsc::channel(8);
        let (tx_out, rx_out) = mpsc::channel(8);

        tokio::task::spawn_blocking(|| stdin(tx_in));
        tokio::task::spawn_blocking(|| stdout(rx_out));

        info!("Node initialized");

        Ok((
            Self {
                id: init_msg.body.payload.node_id,
                node_ids: init_msg.body.payload.node_ids,
                msg_id: 1.into(),
                out_chan: tx_out,
                pending_reply: DashMap::new(),
            },
            rx_in,
        ))
    }

    pub async fn send(&self, peer: CompactString, msg: impl Payload) -> Result<(), NodeError> {
        self.out_chan
            .send(Message {
                src: self.id.clone(),
                dst: peer,
                body: Body {
                    msg_id: Some(self.msg_id.fetch_add(1, Ordering::AcqRel)),
                    in_reply_to: None,
                    payload: msg.ser_val()?,
                },
            })
            .await
            .with_reason("Failed to send message")?;

        Ok(())
    }

    pub async fn reply<T>(&self, from: &Message<T>, msg: impl Payload) -> Result<(), NodeError> {
        self.out_chan
            .send(Message {
                src: from.dst.clone(),
                dst: from.src.clone(),
                body: Body {
                    msg_id: Some(self.msg_id.fetch_add(1, Ordering::AcqRel)),
                    in_reply_to: from.body.msg_id,
                    payload: msg.ser_val()?,
                },
            })
            .await
            .with_reason(format!(
                "Failed to reply to message: {:?}",
                from.body.msg_id
            ))?;

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
        let msg_id = self.msg_id.fetch_add(1, Ordering::AcqRel);
        let msg = Message {
            src: self.id.clone(),
            dst: peer.clone(),
            body: Body {
                msg_id: Some(msg_id),
                in_reply_to: None,
                payload: msg.ser_val()?,
            },
        };
        self.out_chan
            .send(msg.clone())
            .await
            .with_reason("Failed to send initial RPC message")?;

        let token = format_compact!("{peer}:{msg_id}");
        let (tx, mut rx) = oneshot::channel();
        self.pending_reply.insert(token.clone(), tx);

        loop {
            tokio::select!(
                _ = tokio::time::sleep(RPC_LATENCY) => {
                    self.out_chan
                        .send(msg.clone())
                        .await
                        .with_reason("Failed to send retry RPC message")?;
                }
                res = &mut rx => {
                    match res {
                        Ok(res) => {
                            return Ok(res);
                        },
                        Err(_) => {
                            error!("Failed to receive RPC reply");
                            return Err(NodeError::new("Failed to receive RPC reply"))
                        },
                    }
                }
            )
        }
    }

    pub fn ack(&self, msg: &Message<Value>, val: Result<Value, RpcError>) -> Result<(), NodeError> {
        match msg.body.in_reply_to {
            Some(reply) => {
                let token = format_compact!("{}:{reply}", msg.src);
                match self.pending_reply.remove(&token) {
                    Some((_, tx)) => tx
                        .send(val)
                        .map_err(|_| NodeError::new("Failed to send ack")),
                    None => {
                        debug!("No pending reply: {token}, maybe already received");
                        Ok(())
                    }
                }
            }
            None => {
                error!("Invalid Ok message");
                Err(NodeError::new("Invalid Ok message"))
            }
        }
    }
}

impl fmt::Debug for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.id.as_str())
    }
}
