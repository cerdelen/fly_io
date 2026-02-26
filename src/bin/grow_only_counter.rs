use async_trait::async_trait;
use maelstrom::protocol::Message;
use maelstrom::{Node, Result, Runtime, done};
use serde::{Deserialize, Serialize};
use tokio_context::context::Context;
use std::sync::Arc;


use maelstrom::kv::{KV, Storage, seq_kv};


#[derive(Clone)]
struct Handler {
    kv: Storage,
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        let kv_key = String::from("GrowOnly");
        let msg: Result<Request> = req.body.as_obj();
        match msg {
            Ok(Request::Init {} ) => {
                let (ctx, _handler) = Context::new();
                self.kv.cas::<usize>(ctx, kv_key, 0, 0, true).await
            },
            Ok(Request::Read {} ) => {
                let (ctx, _handler) = Context::new();

                // Write a random value to the kv to enforce updating stale values for this node
                let random_number =  rand::random::<u64>();
                let _ = self.kv.put::<usize>(ctx, String::from("rand"), random_number as usize).await;
                let (ctx, _handler) = Context::new();
                let value = self.kv.get::<usize>(ctx, kv_key).await?;
                runtime.reply(req, Request::ReadOk { value }).await
            },
            Ok(Request::Add{ delta })  => {
                loop {
                    let (ctx, _handler) = Context::new();
                    let value = self.kv.get::<usize>(ctx, kv_key.clone()).await?;
                    let (ctx, _handler) = Context::new();
                    if self.kv.cas::<usize>(ctx, kv_key.clone(), value, value + delta, false).await.is_ok() {
                        break
                    }
                }
                runtime.reply(req, Request::AddOk{}).await
            },
            _ => done(runtime, req),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Request {
    Init {},
    Read {},
    ReadOk { value: usize },
    Add { delta: usize },
    AddOk {},
}


async fn async_main() -> Result<()> {
    let runtime = Runtime::new();
    let handler = Arc::new(Handler { kv: seq_kv(runtime.clone())});
    runtime.with_handler(handler).run().await
}

fn main() -> Result<()> {
    Runtime::init(async_main())
}

