use anyhow::Context;
use rusty_maelstrom::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    io::StdoutLock,
};

struct MultiNodeBroadcast {
    seen_messages: HashSet<usize>,
    neighbours: Vec<String>,
    id: String,
}

impl MultiNodeBroadcast {
    fn new() -> Self {
        Self {
            seen_messages: HashSet::new(),
            neighbours: Vec::new(),
            id: String::new(),
        }
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
enum MultiNodeRpcPayload {
    Broadcast { message: usize },
    BroadcastOk,
    Read,
    ReadOk { messages: Vec<usize> },
    Topology { topology: Topology },
    TopologyOk,
    Sync { message: usize },
}

impl MultiNodeBroadcast {
    fn gossip(&self, message: usize, output: &mut StdoutLock) -> anyhow::Result<()> {
        let mut sync_msg = Message {
            src: self.id.clone(),
            dest: String::new(),
            body: Body {
                msg_id: Some(0),
                in_reply_to: None,
                payload: MultiNodeRpcPayload::Sync { message },
            },
        };
        for neighbour in &self.neighbours {
            sync_msg.dest = neighbour.clone();
            sync_msg.send(output)
                .context("Send Sync message BroadcastOk")?;
        };
        Ok(())
    }
}

impl NodeType for MultiNodeBroadcast {
    type Payload = MultiNodeRpcPayload;

    fn step(
        &mut self,
        input: Events<Self::Payload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        if let Events::Message(input) = input {
            if let MultiNodeRpcPayload::Broadcast { message } = input.body.payload {
                if self.seen_messages.insert(message) {
                    self.gossip(message, output).context("Gossip from broadcast failed")?;
                }
                let mut resp = input.to_reply();
                resp.body.payload = MultiNodeRpcPayload::BroadcastOk;
                resp.send(output)
                    .context("Send respond message BroadcastOk")?;
            } else if let MultiNodeRpcPayload::Read = input.body.payload {
                let mut resp = input.to_reply();
                resp.body.payload = MultiNodeRpcPayload::ReadOk {
                    messages: self.seen_messages.iter().copied().collect(),
                };
                resp.send(output)
                    .context("Send respond message BroadcastOk")?;
            } else if let MultiNodeRpcPayload::Topology { topology } = &input.body.payload {
                if let Some(neighbours) = topology.topology.get(&self.id) {
                    self.neighbours.extend_from_slice(neighbours);
                };
                let mut resp = input.to_reply();
                resp.body.payload = MultiNodeRpcPayload::TopologyOk;
                resp.send(output)
                    .context("Send respond message BroadcastOk")?;
            } else if let MultiNodeRpcPayload::Sync { message } = input.body.payload {
                if self.seen_messages.insert(message) {
                    self.gossip(message, output).context("Gossip from broadcast failed")?;
                }
            }
        }
        Ok(())
    }

    fn init(&mut self, init: InitPayload) {
        self.id = init.node_id;
    }
}

fn main() -> anyhow::Result<()> {
    let mut node = Node::<MultiNodeBroadcast>::new(MultiNodeBroadcast::new())?;

    node.run()?;

    Ok(())
}
