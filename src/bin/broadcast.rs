use std::{collections::HashMap, io::BufRead};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use vortex::{message::Message, node::Node};

#[derive(Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Request {
    Broadcast {
        message: i32,
    },
    Read,
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
}

#[derive(Serialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
#[allow(clippy::enum_variant_names)]
enum Response {
    BroadcastOk {},
    ReadOk { messages: Vec<i32> },
    TopologyOk {},
}

fn main() -> anyhow::Result<()> {
    let stdin = std::io::stdin().lock();
    let mut stdout = std::io::stdout().lock();

    let mut input = stdin.lines();
    let mut node = Node::new(&mut input, &mut stdout)?;

    node.run(&mut input, &mut stdout, |node, line| {
        let msg: Message<Request> = serde_json::from_str(&line).context("Invalid message")?;

        let reply: Message<Response> = match msg.body.payload {
            Request::Broadcast { message } => {
                node.messages.push(message);
                msg.reply(Some(node.msg_id), Response::BroadcastOk {})
            }
            Request::Read => msg.reply(
                Some(node.msg_id),
                Response::ReadOk {
                    messages: node.messages.clone(),
                },
            ),
            Request::Topology { ref topology } => {
                node.topology = topology.clone();
                msg.reply(Some(node.msg_id), Response::TopologyOk {})
            }
        };

        Ok(reply)
    })?;

    Ok(())
}
