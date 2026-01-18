use std::io::{BufRead, StdoutLock, Write};

use anyhow::{anyhow, Context};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message<BodyType> {
    pub src: String,
    pub dest: String,
    pub body: Body<BodyType>,
}

impl<BodyType> Message<BodyType> {
    pub fn to_reply(self) -> Self {
        Self {
            src: self.dest,
            dest: self.src,
            body: Body {
                msg_id: self.body.msg_id.map(|id| id + 1),
                in_reply_to: self.body.msg_id,
                payload: self.body.payload,
            },
        }
    }

    pub fn send(&self, output: &mut impl Write) -> anyhow::Result<()>
    where
        BodyType: Serialize,
    {
        serde_json::to_writer(&mut *output, self).context("Error serialize Message to ouput")?;
        output
            .write(b"\n")
            .context("Error in msg.send() writing '\n'")?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Body<BodyType> {
    pub msg_id: Option<usize>,
    pub in_reply_to: Option<usize>,
    #[serde(flatten)]
    pub payload: BodyType,
}

pub struct Node<InnerNode: NodeType> {
    node: InnerNode,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Init {
    Init(InitPayload),
    InitOk,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitPayload {
    pub node_id: String,
    pub node_ids: Vec<String>,
}

pub enum Events<Payload> {
    Message(Message<Payload>),
    // Heartbeat is used to trigger sync's
    Heartbeat,
    EOF,
}

impl<P: NodeType> Node<P> {
    pub fn new(node: P) -> anyhow::Result<Self> {
        let mut stdin = std::io::stdin().lock().lines();
        let mut stdout = std::io::stdout().lock();

        let init_input: Message<Init> = serde_json::from_str(
            &stdin
                .next()
                .expect("Ecpext an init message")
                .context("Init message failed")?,
        )
        .context("Error in init message deserialization")?;

        let mut out = Self { node };

        if let Init::Init(init) = init_input.body.payload.clone() {
            out.node.init(init);
            let mut reply = init_input.to_reply();
            reply.body.payload = Init::InitOk;
            reply.send(&mut stdout)?;
        }

        Ok(out)
    }

    pub fn run(&mut self) -> anyhow::Result<()> {
        let (sender, receiver) = std::sync::mpsc::channel();
        let mut stdout = std::io::stdout().lock();

        let sender_clone = sender.clone();
        let receiving_loop_join_handle = std::thread::spawn(move || {
            let stdin = std::io::stdin().lock();

            for line in stdin.lines() {
                let lines = line.context("Maelstrom input from stdin could not be read")?;

                let input: Message<P::Payload> = serde_json::from_str(&lines)
                    .context("Error converting Maelstrom input to Json format in run")?;

                if sender.send(Events::Message(input)).is_err() {
                    return Err(anyhow!("Failed to send input"));
                }
            }

            if sender.send(Events::EOF).is_err() {
                return Err(anyhow!("Failed to send input"));
            }

            Ok(())
        });

        let heartbeat_join_handle = std::thread::spawn(move || {
            loop {
                std::thread::sleep(std::time::Duration::from_millis(500));
                if sender_clone.send(Events::Heartbeat).is_err() {
                    return Err(anyhow!("Failed to send Heartbeat"));
                }
            }
        });


        for input in receiver {
            self.node
                .step(input, &mut stdout)
                .context("Node step failed")?;
        }

        receiving_loop_join_handle
            .join()
            .expect("stdin thread panicked")
            .context("stdin thread error")?;

        heartbeat_join_handle
            .join()
            .expect("heartbead thread panicked")
            .context("heartbeat thread error")?;
        Ok(())
    }

}

pub trait NodeType {
    type Payload: std::fmt::Debug + DeserializeOwned + Send + 'static;
    fn step(&mut self, input: Events<Self::Payload>, output: &mut StdoutLock) -> anyhow::Result<()>;
    fn init(&mut self, init: InitPayload);
}
