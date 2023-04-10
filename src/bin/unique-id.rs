use std::io::BufRead;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use vortex::{message::Message, node::Node};

#[derive(Deserialize, Serialize, Debug)]
#[serde(tag = "type", rename = "generate")]
struct Generate {}

#[derive(Deserialize, Serialize, Debug)]
#[serde(tag = "type", rename = "generate_ok")]
struct GenerateOk {
    id: String,
}

fn main() -> anyhow::Result<()> {
    let stdin = std::io::stdin().lock();
    let mut stdout = std::io::stdout().lock();

    let mut input = stdin.lines();
    let mut node = Node::new(&mut input, &mut stdout)?;

    node.run(&mut input, &mut stdout, |node, line| {
        let msg: Message<Generate> = serde_json::from_str(&line).context("Invalid message")?;
        let reply = msg.reply(
            Some(node.msg_id),
            GenerateOk {
                id: format!("{}-{}", node.id, node.msg_id),
            },
        );
        Ok(reply)
    })?;

    Ok(())
}
