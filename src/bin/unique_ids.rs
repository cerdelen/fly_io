use anyhow::Context;
use rusty_maelstrom::*;
use serde::{Deserialize, Serialize};
use std::io::StdoutLock;

struct UniqueIdsNode {
    id_gen: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum GeneratePayload {
    Generate,
    GenerateOk { id: String },
}

impl NodeType for UniqueIdsNode {
    type Payload = GeneratePayload;

    fn step(
        &mut self,
        input: Events<Self::Payload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        if let Events::Message(input) = input {
            if let GeneratePayload::Generate = input.body.payload {
                let mut resp = input.to_reply();
                self.id_gen += 1;
                resp.body.payload = GeneratePayload::GenerateOk {
                    id: format!("{}{}", self.id_gen, resp.src),
                };
                resp.send(output).context("Send respond message generate")?;
            }
        }
        Ok(())
    }

    fn init(&mut self, _init: InitPayload) {}
}

fn main() -> anyhow::Result<()> {
    let mut node = Node::<UniqueIdsNode>::new(UniqueIdsNode { id_gen: 0 })?;

    node.run()?;

    Ok(())
}
