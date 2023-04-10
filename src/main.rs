use std::io::{BufRead, Write};

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct Message {
    src: String,
    #[serde(rename = "dest")]
    dst: String,
    body: Body,
}

#[derive(Deserialize, Serialize, Debug)]
struct Body {
    msg_id: Option<u32>,
    in_reply_to: Option<u32>,
    #[serde(flatten)]
    payload: Payload,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Payload {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },
}

fn main() -> anyhow::Result<()> {
    let stdin = std::io::stdin().lock();
    let mut stdout = std::io::stdout().lock();
    let mut msg_id = 0;

    let mut input = stdin.lines();
    let init_msg: Message =
        serde_json::from_str(&input.next().context("Failed to read init message")??)
            .context("Failed to parse init message")?;
    if let Payload::Init { .. } = init_msg.body.payload {
        let reply = Message {
            src: init_msg.dst,
            dst: init_msg.src,
            body: Body {
                msg_id: None,
                in_reply_to: init_msg.body.msg_id,
                payload: Payload::InitOk,
            },
        };
        serde_json::to_writer(&mut stdout, &reply)?;
        writeln!(stdout)?;
        msg_id += 1;
    } else {
        bail!("Invalid init message");
    }

    for line in input {
        let line = line?;
        let msg: Message = serde_json::from_str(&line)?;
        match msg.body.payload {
            Payload::Echo { echo } => {
                let reply = Message {
                    src: msg.dst,
                    dst: msg.src,
                    body: Body {
                        msg_id: Some(msg_id),
                        in_reply_to: msg.body.msg_id,
                        payload: Payload::EchoOk { echo },
                    },
                };
                serde_json::to_writer(&mut stdout, &reply)?;
                writeln!(stdout)?;
                msg_id += 1;
            }
            _ => bail!("Invalid message"),
        }
    }

    Ok(())
}
