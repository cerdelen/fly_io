use async_trait::async_trait;
use maelstrom::protocol::Message;
use maelstrom::{done, Node, Result, Runtime};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone, Default)]
struct Handler {
    s: Arc<Mutex<State>>,
}

#[derive(Clone, Default)]
struct State {
    m: HashMap<usize, usize>,
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        let msg: Result<Request> = req.body.as_obj();
        match msg {
            Ok(Request::Init {}) => Ok(()),
            Ok(Request::Txn { mut txn }) => {
                let mut s = self.s.lock().await;
                for (op, key, val) in &mut txn {
                    match op {
                        OP::R => {
                            *val = s.m.get(key).cloned();
                        }
                        OP::W => {
                            if let Some(val) = val {
                                s.m.insert(*key, *val);
                            }
                        }
                    }
                }
                runtime.reply(req, Response::TxnOk { txn }).await
            }
            _ => done(runtime, req),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Request {
    Init {},
    Txn {
        txn: Vec<(OP, usize, Option<usize>)>,
    },
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
enum OP {
    R,
    W,
}

#[derive(Serialize, Deserialize)]
#[allow(clippy::enum_variant_names)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Response {
    TxnOk {
        txn: Vec<(OP, usize, Option<usize>)>,
    },
}

async fn async_main() -> Result<()> {
    let runtime = Runtime::new();
    let handler = Arc::new(Handler::default());
    runtime.with_handler(handler).run().await
}

fn main() -> Result<()> {
    Runtime::init(async_main())
}
