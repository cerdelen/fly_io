use async_trait::async_trait;
use maelstrom::protocol::Message;
use maelstrom::{done, Node, Result, Runtime};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio_context::context::Context;

use maelstrom::kv::{lin_kv, seq_kv, Storage, KV};

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
            Ok(Request::Send { key, msg }) => {
                let offset = loop {
                    let (ctx, _handler) = Context::new();
                    let msgs: Vec<usize> =
                        self.lin_kv.get(ctx, key.clone()).await.unwrap_or_default();
                    let offset = msgs.len();
                    let mut msgs_updated: Vec<usize> = msgs.clone();
                    msgs_updated.push(msg);
                    let (ctx, _handler) = Context::new();
                    if self
                        .lin_kv
                        .cas(ctx, key.clone(), msgs, msgs_updated, true)
                        .await
                        .is_ok()
                    {
                        break offset;
                    }
                };
                runtime.reply(req, Response::SendOk { offset }).await
            }
            Ok(Request::Poll { offsets }) => {
                let mut out_msgs = HashMap::with_capacity(offsets.len());
                for (key, offset) in offsets {
                    let (ctx, _handler) = Context::new();
                    if let Ok(msgs) = self.lin_kv.get::<Vec<usize>>(ctx, key.clone()).await {
                        let mut idx = offset;
                        let mut out = Vec::with_capacity(msgs.len() - idx);
                        if idx < msgs.len() {
                            for msg in msgs[offset..].iter() {
                                out.push((idx, *msg));
                                idx += 1;
                            }
                        }
                        out_msgs.insert(key, out);
                    }
                }
                runtime
                    .reply(req, Response::PollOk { msgs: out_msgs })
                    .await
            }
            Ok(Request::CommitOffsets { offsets }) => {
                for (key, offset) in offsets {
                    let (ctx, _handler) = Context::new();
                    let _ = self.seq_kv.put(ctx, key, offset).await;
                }
                runtime.reply(req, Response::CommitOffsetsOk {}).await
            }
            Ok(Request::ListCommittedOffsets { keys }) => {
                let mut offsets = HashMap::with_capacity(keys.len());

                for key in keys {
                    let (ctx, _handler) = Context::new();
                    if let Ok(offset) = self.seq_kv.get::<usize>(ctx, key.clone()).await {
                        offsets.insert(key, offset);
                    };
                }
                runtime
                    .reply(req, Response::ListCommittedOffsetsOk { offsets })
                    .await
            }
            _ => done(runtime, req),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Request {
    Init {},
    Send { key: String, msg: usize },
    Poll { offsets: HashMap<String, usize> },
    CommitOffsets { offsets: HashMap<String, usize> },
    ListCommittedOffsets { keys: Vec<String> },
}

#[derive(Serialize, Deserialize)]
#[allow(clippy::enum_variant_names)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Response {
    SendOk {
        offset: usize,
    },
    PollOk {
        msgs: HashMap<String, Vec<(usize, usize)>>,
    },
    CommitOffsetsOk {},
    ListCommittedOffsetsOk {
        offsets: HashMap<String, usize>,
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
