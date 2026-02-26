use async_trait::async_trait;
use maelstrom::protocol::Message;
use maelstrom::{done, Node, Result, Runtime};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
struct Handler {
    msgs: Arc<RwLock<HashMap<String, Vec<usize>>>>,
    offsets: Arc<RwLock<HashMap<String, usize>>>,
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        let msg: Result<Request> = req.body.as_obj();
        match msg {
            Ok(Request::Init {}) => Ok(()),
            Ok(Request::Send { key, msg }) => {
                let offset = match self.msgs.write().await.entry(key) {
                    std::collections::hash_map::Entry::Occupied(mut occupied_entry) => {
                        let msgs = occupied_entry.get_mut();
                        msgs.push(msg);
                        msgs.len() - 1
                    }
                    std::collections::hash_map::Entry::Vacant(vacant_entry) => {
                        vacant_entry.insert(vec![msg]);
                        0
                    }
                };
                runtime.reply(req, Request::SendOk { offset }).await
            }
            Ok(Request::Poll { offsets }) => {
                let mut out_msgs = HashMap::with_capacity(offsets.len());
                let msg_guard = self.msgs.read().await;
                for start_offset in &offsets {
                    let mut idx = *start_offset.1;
                    if let Some(msgs) = msg_guard.get(start_offset.0) {
                        let mut out = Vec::with_capacity(msgs.len() - idx);
                        if idx < msgs.len() {
                            for msg in msgs[idx..].iter() {
                                out.push((idx, *msg));
                                idx += 1;
                            }
                        }
                        out_msgs.insert(start_offset.0.clone(), out);
                    }
                }

                runtime.reply(req, Request::PollOk { msgs: out_msgs }).await
            }
            Ok(Request::CommitOffsets { offsets }) => {
                let mut guard = self.offsets.write().await;
                for offset in offsets {
                    guard.insert(offset.0, offset.1);
                }
                runtime.reply(req, Request::CommitOffsetsOk {}).await
            }
            Ok(Request::ListCommittedOffsets { keys }) => {
                let guard = self.offsets.write().await;
                let mut offsets = HashMap::with_capacity(keys.len());

                for key in keys {
                    if let Some(offset) = guard.get(&key) {
                        offsets.insert(key, *offset);
                    };
                }
                runtime
                    .reply(req, Request::ListCommittedOffsetsOk { offsets })
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
    Send {
        key: String,
        msg: usize,
    },
    SendOk {
        offset: usize,
    },
    Poll {
        offsets: HashMap<String, usize>,
    },
    PollOk {
        msgs: HashMap<String, Vec<(usize, usize)>>,
    },
    CommitOffsets {
        offsets: HashMap<String, usize>,
    },
    CommitOffsetsOk {},
    ListCommittedOffsets {
        keys: Vec<String>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<String, usize>,
    },
}

async fn async_main() -> Result<()> {
    let runtime = Runtime::new();
    let handler = Arc::new(Handler {
        msgs: Arc::new(RwLock::new(HashMap::new())),
        offsets: Arc::new(RwLock::new(HashMap::new())),
    });
    runtime.with_handler(handler).run().await
}

fn main() -> Result<()> {
    Runtime::init(async_main())
}
