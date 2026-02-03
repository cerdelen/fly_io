use anyhow::{bail, Context};
use rusty_maelstrom::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    io::StdoutLock,
};

struct MultiNodeBroadcast {
    id: String,
    neighbours: Vec<String>,
    seen_messages: HashSet<usize>,
    known: HashMap<String, HashSet<usize>>,
    seen_by_broadcast: HashSet<usize>,
    wrong_syncs: HashSet<usize>,
    sync_counter: usize,
    /// caching what we sent as a sync message so we can updated known messages of neighbour when
    /// receiving the SyncOK
    sent_sync_messages: HashMap<usize, Vec<usize>>,
}

impl MultiNodeBroadcast {
    fn new() -> Self {
        Self {
            id: String::new(),
            neighbours: Vec::new(),
            seen_messages: HashSet::new(),
            known: HashMap::new(),
            seen_by_broadcast: HashSet::new(),
            wrong_syncs: HashSet::new(),
            sync_counter: 0,
            sent_sync_messages: HashMap::new(),
        }
    }

    fn handle_incoming_sync_ok(&mut self, msg: Message<MultiNodeRpcPayload>) -> anyhow::Result<()> {
        if let Some(sync_message) = self.sent_sync_messages.remove(&msg.body.in_reply_to.unwrap()) {
            if let Some(neighbour_known_messages) = self.known.get_mut(&msg.src) {
                for message in sync_message {
                    neighbour_known_messages.insert(message);
                }
            } else {
                bail!("got syncOk of not neighbour");
            }
        }
        Ok(())
    }

    fn handle_incoming_sync(&mut self, mut sync_msg: Message<MultiNodeRpcPayload>, output: &mut StdoutLock) -> anyhow::Result<()> {
        if let MultiNodeRpcPayload::Sync { messages } = sync_msg.body.payload {
            for message in messages {
                self.seen_messages.insert(message);
                if let Some(neighbour_known) = self.known.get_mut(&sync_msg.src) {
                    neighbour_known.insert(message);
                }
            }
        } else {
            bail!("into handle_incoming_sync with non sync message");
        }
        sync_msg.body.payload = MultiNodeRpcPayload::SyncOk;
        let reply = sync_msg.to_reply();
        reply.send(output).context("Error sending SyncOK as reply")?;
        Ok(())
    }

    fn trigger_sync(&mut self, output: &mut StdoutLock) -> anyhow::Result<()> {
        // already_knows.extend(self.neighbours.clone());
        let time_stamp = std::time::Instant::now();
        for neighbour in &self.neighbours {
            if let Some(known_messages_of_neighbour) = self.known.get(neighbour) {
                let diff: Vec<_> = self
                    .seen_messages
                    .symmetric_difference(known_messages_of_neighbour)
                    .copied()
                    .collect();
                if !diff.is_empty() {
                    let msg = Message {
                        src: self.id.clone(),
                        dest: neighbour.clone(),
                        body: Body {
                            msg_id: Some(self.sync_counter),
                            in_reply_to: None,
                            payload: MultiNodeRpcPayload::Sync {
                                messages: diff.clone(),
                            },
                        },
                    };
                    msg.send(output).context("Send Sync message Broadcast")?;
                    self.sync_counter += 1;
                    // without Ack messages we cannot set the messages we send as known as there
                    // might be partitions
                    // if let Some(neighbour_known) = self.known.get_mut(neighbour) {
                    //     neighbour_known.extend(diff);
                    // }
                }
            } else {
                bail!("trying to send Sync to unknown neighbour");
            }
        }
        let time_stamp_end = std::time::Instant::now();
        eprintln!(
            "a whole trigger_sync takes {:?} at {time_stamp_end:?}",
            time_stamp_end - time_stamp
        );
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
    SyncOk,
}

impl NodeType for MultiNodeBroadcast {
    type Payload = MultiNodeRpcPayload;

    fn step(
        &mut self,
        input: Events<Self::Payload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        match input {
            Events::Message(input) => {
                match &input.body.payload {
                    MultiNodeRpcPayload::Broadcast { message } => {
                        self.seen_messages.insert(*message);
                        // self.seen_by_broadcast.insert(message);
                        let mut resp = input.to_reply();
                        resp.body.payload = MultiNodeRpcPayload::BroadcastOk;
                        resp.send(output)
                            .context("Send respond message BroadcastOk")?;
                        self.trigger_sync(output).context("trigger sync errored")?;
                    },
                    MultiNodeRpcPayload::Read => {
                        let mut resp = input.to_reply();
                        let mut messages: Vec<usize> = self.seen_messages.iter().copied().collect();
                        messages.sort();
                        resp.body.payload = MultiNodeRpcPayload::ReadOk { messages };
                        // resp.body.payload = MultiNodeRpcPayload::ReadOk {
                        //     messages: self.seen_messages.iter().copied().collect::<Vec<usize>>().sort(),
                        // };
                        resp.send(output).context("Send respond message ReadOk")?;
                    },
                    MultiNodeRpcPayload::Topology { topology } => {
                        if let Some(neighbours) = topology.topology.get(&self.id) {
                            self.neighbours.extend_from_slice(neighbours);
                        };
                        let mut resp = input.to_reply();
                        resp.body.payload = MultiNodeRpcPayload::TopologyOk;
                        resp.send(output)
                            .context("Send respond message TopologyOk")?;
                    },
                    MultiNodeRpcPayload::Sync { messages } => self.handle_incoming_sync(input, output).context("handle sync errored")?,
                    MultiNodeRpcPayload::SyncOk => self.handle_incoming_sync_ok(input).context("handle syncOk errored")?,
                    _ => todo!(),
                    // MultiNodeRpcPayload::ReadOk { messages } => todo!(),
                    // MultiNodeRpcPayload::BroadcastOk => todo!(),
                    // MultiNodeRpcPayload::TopologyOk => todo!(),
                }
            }
            Events::Heartbeat => {}
            _ => {}
        }
        Ok(())
    }

    fn init(&mut self, init: InitPayload) {
        self.id = init.node_id;
        for node in init.node_ids {
            self.known.insert(node, HashSet::new());
        }
    }
}

fn main() -> anyhow::Result<()> {
    let mut node = Node::<MultiNodeBroadcast>::new(MultiNodeBroadcast::new())?;

    node.run()?;

    Ok(())
}
