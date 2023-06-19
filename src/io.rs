use std::io::{BufRead, Write};

use serde_json::Value;
use tokio::sync::mpsc;
use tracing::error;

use crate::message::Message;

pub fn stdin(tx: mpsc::Sender<Message<Value>>) {
    let mut buffer = String::new();
    let mut stdin = std::io::stdin().lock();
    while stdin
        .read_line(&mut buffer)
        .expect("Failed to read from stdin")
        != 0
    {
        match serde_json::from_str(&buffer) {
            Ok(msg) => tx.blocking_send(msg).expect("Failed to send from stdin"),
            Err(e) => {
                error!(buffer, ?e, "Failed to parse message");
                return;
            }
        }
        buffer.clear();
    }
}

pub fn stdout(mut rx: mpsc::Receiver<Message<Value>>) {
    let mut output = std::io::stdout().lock();
    while let Some(msg) = rx.blocking_recv() {
        serde_json::to_writer(&mut output, &msg).expect("Failed to serialize to stdout");
        writeln!(output).expect("Failed to write to stdout");
    }
}
