use async_trait::async_trait;
use maelstrom::kv::{lin_kv, seq_kv, Storage, KV};
use maelstrom::protocol::Message;
use maelstrom::{done, Node, Result, Runtime};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio_context::context::Context;

#[derive(Clone)]
struct Handler {
    seq_kv: Storage,
    lin_kv: Storage,
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        let msg: Result<Request> = req.body.as_obj();
        match msg {
            Ok(Request::Init {}) => Ok(()),
            Ok(Request::Txn { mut txn }) => {
                for (op, key, val) in &mut txn {
                    match op {
                        OP::R => {
                            let (ctx, _handler) = Context::new();
                            if let Ok(read_value) =
                                self.lin_kv.get::<usize>(ctx, key.to_string()).await
                            {
                                *val = Some(read_value);
                            }
                        }
                        OP::W => {
                            if let Some(val) = val {
                                let (ctx, _handler) = Context::new();
                                let _ = self.seq_kv.put(ctx, key.to_string(), val).await;
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
    let handler = Arc::new(Handler {
        seq_kv: seq_kv(runtime.clone()),
        lin_kv: lin_kv(runtime.clone()),
    });
    runtime.with_handler(handler).run().await
}

fn main() -> Result<()> {
    Runtime::init(async_main())
}
