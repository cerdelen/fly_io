use anyhow::Context;
use rusty_maelstrom::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    io::StdoutLock,
};

struct MultiNodeBroadcast {
    id: String,
    neighbours: Vec<String>,
    seen_messages: HashSet<usize>,
}

impl MultiNodeBroadcast {
    fn new() -> Self {
        Self {
            id: String::new(),
            neighbours: Vec::new(),
            seen_messages: HashSet::new(),
        }
    }

    fn send_sync(&mut self, dest: String, output: &mut StdoutLock) -> anyhow::Result<()> {
        let msg = Message {
            src: self.id.clone(),
            dest,
            body: Body {
                msg_id: None,
                in_reply_to: None,
                payload: MultiNodeRpcPayload::Sync {
                    messages: self.seen_messages.iter().cloned().collect(),
                },
            },
        };
        msg.send(output).context("Send Sync message Broadcast")?;
        Ok(())
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
    Sync { messages: Vec<usize> },
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
                self.seen_messages.insert(message);
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
            } else if let MultiNodeRpcPayload::Sync { messages } = input.body.payload {
                for message in messages {
                    self.seen_messages.insert(message);
                }
            }
        } else if let Events::Heartbeat = input {
            for neighbour in self.neighbours.clone() {
                self.send_sync(neighbour, output).context("Send sync error")?;
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
