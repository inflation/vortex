use std::{
    collections::HashSet,
    io::{Lines, StdinLock, StdoutLock, Write},
};

use anyhow::Context;
use serde::Serialize;

use crate::message::{Body, Init, InitOk, Message};

pub struct Node {
    pub id: String,
    pub msg_id: u32,
    pub messages: HashSet<i32>,
    pub peers: Vec<String>,
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
            messages: HashSet::new(),
            peers: vec![],
        })
    }

    pub fn run<F, P>(
        &mut self,
        input: &mut Lines<StdinLock>,
        stdout: &mut StdoutLock,
        f: F,
    ) -> anyhow::Result<()>
    where
        F: Fn(&mut Self, String) -> anyhow::Result<Message<P>>,
        P: Serialize,
    {
        for line in input {
            let line = line?;
            serde_json::to_writer(&mut *stdout, &f(self, line)?)?;
            writeln!(stdout)?;
            self.msg_id += 1;
        }

        Ok(())
    }

    pub fn send<U>(&self, peer: String, msg: U) -> Message<U> {
        Message {
            src: self.id.clone(),
            dst: peer,
            body: Body {
                msg_id: Some(self.msg_id),
                in_reply_to: None,
                payload: msg,
            },
        }
    }
}
