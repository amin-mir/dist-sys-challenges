use std::sync::Arc;

use async_trait::async_trait;
use maelstrom::kv::{seq_kv, Storage, KV};
use maelstrom::protocol::Message;
use maelstrom::{done, Node, Result, Runtime};
use serde::{Deserialize, Serialize};
use tokio_context::context::Context;

fn main() -> Result<()> {
    Runtime::init(async {
        let runtime = Runtime::new();
        let handler = Arc::new(Handler::new(runtime.clone()));
        runtime.with_handler(handler).run().await
    })
}

struct Handler {
    storage: Storage,
}

impl Handler {
    fn new(runtime: Runtime) -> Handler {
        Handler {
            storage: seq_kv(runtime),
        }
    }
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        let (ctx, mut ctx_handler) = Context::new();
        let gocounter: Result<GrowOnlyCounter> = req.body.as_obj();

        match gocounter {
            Ok(GrowOnlyCounter::Add { delta }) => {
                loop {
                    let old_val = self
                        .storage
                        .get(ctx_handler.spawn_ctx(), "counter".to_string())
                        .await
                        .unwrap_or(0);

                    let new_val = old_val + delta;

                    let cas_res = self
                        .storage
                        .cas(
                            ctx_handler.spawn_ctx(),
                            "counter".to_string(),
                            old_val,
                            new_val,
                            true,
                        )
                        .await;

                    if cas_res.is_ok() {
                        break;
                    }
                }

                runtime.reply(req, GrowOnlyCounter::AddOk).await
            }
            Ok(GrowOnlyCounter::Read) => {
                let val: i64 = match self.storage.get(ctx, "counter".to_string()).await {
                    Ok(val) => val,
                    Err(_) => 0,
                };
                runtime
                    .reply(req, GrowOnlyCounter::ReadOk { value: val })
                    .await
            }
            _ => done(runtime, req),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum GrowOnlyCounter {
    Add { delta: i64 },
    AddOk,
    Read,
    ReadOk { value: i64 },
}
