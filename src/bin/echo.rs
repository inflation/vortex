use std::io::BufRead;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use vortex::{
    message::{Body, Message},
    node::Node,
};

#[derive(Deserialize, Serialize, Debug)]
#[serde(tag = "type", rename = "echo")]
struct Echo {
    echo: String,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(tag = "type", rename = "echo_ok")]
struct EchoOk {
    echo: String,
}

fn main() -> anyhow::Result<()> {
    let stdin = std::io::stdin().lock();
    let mut stdout = std::io::stdout().lock();

    let mut input = stdin.lines();
    let mut node = Node::new(&mut input, &mut stdout)?;

    node.run(&mut input, &mut stdout, |msg_id, line| {
        let msg: Message<Echo> = serde_json::from_str(&line).context("Invalid message")?;
        let reply = Message {
            src: msg.dst,
            dst: msg.src,
            body: Body {
                msg_id: Some(msg_id),
                in_reply_to: msg.body.msg_id,
                payload: EchoOk {
                    echo: msg.body.payload.echo,
                },
            },
        };
        Ok(reply)
    })?;

    Ok(())
}
