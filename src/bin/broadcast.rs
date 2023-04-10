use std::{
    collections::{HashMap, HashSet},
    io::{BufRead, Write},
};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use vortex::{message::Message, node::Node};

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Request {
    Broadcast {
        message: i32,
    },
    BroadcastOk {},
    Read,
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
}

#[derive(Serialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
#[allow(clippy::enum_variant_names)]
enum Response {
    ReadOk { messages: HashSet<i32> },
    TopologyOk {},
}

fn main() -> anyhow::Result<()> {
    let stdin = std::io::stdin().lock();
    let mut stdout = std::io::stdout().lock();

    let mut input = stdin.lines();
    let mut node = Node::new(&mut input, &mut stdout)?;

    for line in input {
        let line = line.context("Failed to read message")?;
        let msg: Message<Request> = serde_json::from_str(&line).context("Invalid message")?;

        match msg.body.payload {
            Request::Broadcast { message } => {
                node.messages.insert(message);
                serde_json::to_writer(
                    &mut stdout,
                    &msg.reply(Some(node.msg_id), Request::BroadcastOk {}),
                )?;
                writeln!(stdout)?;
                node.msg_id += 1;

                for peer in &node.peers {
                    if peer == &msg.src {
                        continue;
                    }

                    serde_json::to_writer(
                        &mut stdout,
                        &node.send(peer.clone(), Request::Broadcast { message }),
                    )?;
                    writeln!(stdout)?;
                    node.msg_id += 1;
                }
            }
            Request::BroadcastOk {} => {}
            Request::Read => {
                serde_json::to_writer(
                    &mut stdout,
                    &msg.reply(
                        Some(node.msg_id),
                        Response::ReadOk {
                            messages: node.messages.clone(),
                        },
                    ),
                )?;
                writeln!(stdout)?;
                node.msg_id += 1;
            }
            Request::Topology { ref topology } => {
                if let Some(peers) = topology.get(&node.id) {
                    node.peers = peers.clone();
                }
                serde_json::to_writer(
                    &mut stdout,
                    &msg.reply(Some(node.msg_id), Response::TopologyOk {}),
                )?;
                writeln!(stdout)?;
                node.msg_id += 1;
            }
        };
    }

    Ok(())
}
