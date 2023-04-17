use std::io::{BufRead, Write};

use anyhow::Context;
use tokio::sync::mpsc;
use tracing::{debug, error};

use crate::message::{Message, Payload};

pub fn stdin<P: Payload>(tx: mpsc::Sender<Message<P>>) -> anyhow::Result<()> {
    for line in std::io::stdin().lock().lines() {
        let line = line.context("Failed to read message")?;
        debug!("Received message: {line}");
        if let Ok(msg) = serde_json::from_str(&line) {
            tx.blocking_send(msg)?;
        } else {
            error!("Failed to parse message: {line}");
        };
    }

    Ok(())
}

pub fn stdout<P: Payload>(mut rx: mpsc::Receiver<Message<P>>) -> anyhow::Result<()> {
    // let mut output = std::io::stdout().lock();
    let mut output = std::io::stdout();

    while let Some(msg) = rx.blocking_recv() {
        serde_json::to_writer(&mut output, &msg)?;
        writeln!(output)?;
    }

    Ok(())
}
