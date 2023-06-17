use std::io::{BufRead, Write};

use serde_json::Value;
use tokio::sync::mpsc;
use tracing::error;

use crate::{
    error::{NodeError, WithReason},
    message::Message,
};

pub fn stdin(tx: mpsc::Sender<Message<Value>>) -> Result<(), NodeError> {
    for line in std::io::stdin().lock().lines() {
        let line = line.with_reason("Failed to read from stdin")?;
        match serde_json::from_str(&line) {
            Ok(msg) => tx
                .blocking_send(msg)
                .with_reason("Failed to send from stdin")?,
            Err(e) => {
                error!(?line, "Failed to parse message");
                return Err(NodeError::new_with("Failed to parse message", e));
            }
        }
    }

    Ok(())
}

pub fn stdout(mut rx: mpsc::Receiver<Message<Value>>) -> Result<(), NodeError> {
    let mut output = std::io::stdout().lock();
    while let Some(msg) = rx.blocking_recv() {
        serde_json::to_writer(&mut output, &msg).with_reason("Failed to serialize to stdout")?;
        writeln!(output).with_reason("Failed to write to stdout")?;
    }

    Ok(())
}
