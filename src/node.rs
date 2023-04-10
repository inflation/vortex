use std::io::{Lines, StdinLock, StdoutLock, Write};

use anyhow::Context;
use serde::Serialize;

use crate::message::{Body, Init, InitOk, Message};

pub struct Node {
    pub id: String,
    pub msg_id: u32,
}

impl Node {
    pub fn new(input: &mut Lines<StdinLock>, output: &mut StdoutLock) -> anyhow::Result<Self> {
        let init_msg: Message<Init> =
            serde_json::from_str(&input.next().context("Failed to read init message")??)
                .context("Failed to parse init message")?;
        let reply = Message {
            src: init_msg.dst,
            dst: init_msg.src,
            body: Body {
                msg_id: None,
                in_reply_to: init_msg.body.msg_id,
                payload: InitOk {},
            },
        };
        serde_json::to_writer(&mut *output, &reply)?;
        writeln!(output)?;

        Ok(Self {
            id: init_msg.body.payload.node_id,
            msg_id: 1,
        })
    }

    pub fn run<F, P>(
        &mut self,
        input: &mut Lines<StdinLock>,
        stdout: &mut StdoutLock,
        f: F,
    ) -> anyhow::Result<()>
    where
        F: Fn(u32, String) -> anyhow::Result<Message<P>>,
        P: Serialize,
    {
        for line in input {
            let line = line?;
            serde_json::to_writer(&mut *stdout, &f(self.msg_id, line)?)?;
            writeln!(stdout)?;
            self.msg_id += 1;
        }

        Ok(())
    }
}
