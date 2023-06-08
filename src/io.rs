use std::io::{BufRead, Write};

use serde_json::Value;
use tokio::sync::mpsc;
use tracing::error;

use crate::{error::ExternalError, message::Message};

pub fn stdin(tx: mpsc::Sender<Message<Value>>) -> Result<(), ExternalError> {
    for line in std::io::stdin().lock().lines() {
        let line = line?;
        if let Ok(msg) = serde_json::from_str(&line) {
            tx.blocking_send(msg)?;
        } else {
            error!(line, "Failed to parse message");
        };
    }

    Ok(())
}

pub fn stdout(mut rx: mpsc::Receiver<Message<Value>>) -> Result<(), ExternalError> {
    let mut output = std::io::stdout().lock();

    while let Some(msg) = rx.blocking_recv() {
        serde_json::to_writer(&mut output, &msg)?;
        writeln!(output)?;
    }

    Ok(())
}
