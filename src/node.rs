use std::{
    io::Write,
    sync::atomic::{AtomicU32, Ordering},
    time::Duration,
};

use anyhow::Context;
use dashmap::DashMap;
use parking_lot::Mutex;
use tinyset::SetU32;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

use crate::{
    io::{stdin, stdout},
    message::{Body, Init, InitOk, Message, Payload},
};

#[derive(Debug)]
pub struct Node<P>
where
    P: Payload,
{
    pub id: String,
    pub msg_id: AtomicU32,
    pub messages: Mutex<SetU32>,
    pub peers: Mutex<Vec<String>>,
    pub in_chan: tokio::sync::Mutex<mpsc::Receiver<Message<P>>>,
    pub out_chan: mpsc::Sender<Message<P>>,
    pub handles: [JoinHandle<anyhow::Result<()>>; 2],
    pub pending_broadcasts: DashMap<String, oneshot::Sender<()>>,
}

impl<P> Node<P>
where
    P: Payload,
{
    pub fn new() -> anyhow::Result<Self> {
        let mut line = String::new();
        std::io::stdin()
            .read_line(&mut line)
            .context("Failed to read init message")?;
        let mut output = std::io::stdout().lock();

        let init_msg: Message<Init> =
            serde_json::from_str(&line).context("Failed to parse init message")?;
        let reply = Message {
            src: init_msg.dst,
            dst: init_msg.src,
            body: Body {
                msg_id: None,
                in_reply_to: init_msg.body.msg_id,
                payload: InitOk {},
            },
        };
        serde_json::to_writer(&mut output, &reply)?;
        writeln!(&mut output)?;

        let (tx_in, rx_in) = mpsc::channel(8);
        let (tx_out, rx_out) = mpsc::channel(8);

        let stdin = tokio::task::spawn_blocking(|| stdin::<P>(tx_in));
        let stdout = tokio::task::spawn_blocking(|| stdout::<P>(rx_out));

        Ok(Self {
            id: init_msg.body.payload.node_id,
            msg_id: 1.into(),
            messages: Mutex::new(SetU32::new()),
            peers: Mutex::new(vec![]),
            in_chan: tokio::sync::Mutex::new(rx_in),
            out_chan: tx_out,
            handles: [stdin, stdout],
            pending_broadcasts: DashMap::new(),
        })
    }

    pub async fn send(&self, peer: String, msg: P) -> anyhow::Result<u32> {
        let id = self.msg_id.load(Ordering::Relaxed);
        self.out_chan
            .send(Message {
                src: self.id.clone(),
                dst: peer,
                body: Body {
                    msg_id: Some(id),
                    in_reply_to: None,
                    payload: msg,
                },
            })
            .await?;
        self.msg_id.fetch_add(1, Ordering::Relaxed);

        Ok(id)
    }

    pub async fn reply<T>(&self, from: &Message<T>, msg: P) -> anyhow::Result<()> {
        self.out_chan
            .send(Message {
                src: from.dst.clone(),
                dst: from.src.clone(),
                body: Body {
                    msg_id: Some(self.msg_id.load(Ordering::Relaxed)),
                    in_reply_to: from.body.msg_id,
                    payload: msg,
                },
            })
            .await?;
        self.msg_id.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    pub async fn rpc(&self, peer: String, msg: P) -> anyhow::Result<()> {
        let msg_id = self.msg_id.load(Ordering::Relaxed);
        let msg = Message {
            src: self.id.clone(),
            dst: peer.clone(),
            body: Body {
                msg_id: Some(msg_id),
                in_reply_to: None,
                payload: msg,
            },
        };
        self.out_chan.send(msg.clone()).await?;
        self.msg_id.fetch_add(1, Ordering::Relaxed);

        let token = format!("{}:{msg_id}", peer);
        let (tx, mut rx) = oneshot::channel();
        self.pending_broadcasts.insert(token.clone(), tx);

        loop {
            tokio::select!(
                _ = tokio::time::sleep(Duration::from_secs(1)) => {
                    self.out_chan.send(msg.clone()).await?;
                }
                _ = &mut rx => {
                    self.pending_broadcasts.remove(&token);
                    return Ok(());
                }
            )
        }
    }
}
