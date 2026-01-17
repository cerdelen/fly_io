use anyhow::Context;
use rusty_maelstrom::*;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, io::StdoutLock};

struct SingleNodeBroadcast {
    seen_messages: Vec<usize>,
}

impl SingleNodeBroadcast {
    fn new() -> Self {
        Self { seen_messages: vec![] }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
struct Topology {
    #[serde(flatten)]
    topology: HashMap<String, Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum SingleNodeRpcPayload {
    Broadcast { message: usize },
    BroadcastOk,
    Read,
    ReadOk { messages: Vec<usize> },
    Topology { topology: Topology },
    TopologyOk,
}

impl NodeType for SingleNodeBroadcast {
    type Payload = SingleNodeRpcPayload;

    fn step(
        &mut self,
        input: Events<Self::Payload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        if let Events::Message(input) = input {
            if let SingleNodeRpcPayload::Broadcast{ message } = input.body.payload {
                self.seen_messages.push(message);
                let mut resp = input.to_reply();
                resp.body.payload = SingleNodeRpcPayload::BroadcastOk;
                resp.send(output).context("Send respond message BroadcastOk")?;
            } else if let SingleNodeRpcPayload::Read = input.body.payload {
                let mut resp = input.to_reply();
                resp.body.payload = SingleNodeRpcPayload::ReadOk { messages: self.seen_messages.clone() };
                resp.send(output).context("Send respond message BroadcastOk")?;
            } else if let SingleNodeRpcPayload::Topology { topology: _ }= input.body.payload.clone() {
                let mut resp = input.to_reply();
                resp.body.payload = SingleNodeRpcPayload::TopologyOk;
                resp.send(output).context("Send respond message BroadcastOk")?;
            }
        }
        Ok(())
    }

    fn init(&mut self, _init: InitPayload) {}
}

fn main() -> anyhow::Result<()> {
    let mut node = Node::<SingleNodeBroadcast>::new(SingleNodeBroadcast::new())?;

    node.run()?;

    Ok(())
}
