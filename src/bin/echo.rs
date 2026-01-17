use std::io::StdoutLock;

use anyhow::Context;
use serde::{Deserialize, Serialize};

use rusty_maelstrom::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum EchoPayload {
    Echo {echo: String},
    EchoOk {echo: String},
}

fn main() -> anyhow::Result<()> {
    let mut node = Node::<EchoNode>::new(EchoNode{})?;

    node.run()?;

    Ok(())
}

struct EchoNode { }

impl NodeType for EchoNode {
    type Payload = EchoPayload;

    fn step(&mut self, input: Events<Self::Payload>, output: &mut StdoutLock) -> anyhow::Result<()> {
        if let Events::Message(input) = input {
            let mut resp = input.to_reply();
            if let EchoPayload::Echo { echo } = resp.body.payload {
                resp.body.payload = EchoPayload::EchoOk { echo };
                resp.send(output).context("Send respond message echo")?;
            }
        }
        Ok(())
    }

    fn init(&mut self, _init: InitPayload) { }
}

