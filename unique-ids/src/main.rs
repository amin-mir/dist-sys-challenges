use std::sync::Arc;
use std::sync::atomic::{Ordering, AtomicUsize};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::json;
use async_trait::async_trait;
use maelstrom::protocol::Message;
use maelstrom::{done, Node, Result, Runtime};

pub(crate) fn main() -> Result<()> {
    Runtime::init(async {
        let handler = Arc::new(Handler::default());
        Runtime::new().with_handler(handler).run().await
    })
}

#[derive(Default)]
struct Handler {
    counter: AtomicUsize,
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        if req.get_type() == "generate" {
            let id = self.generate_id(&runtime);
            let mut body = req.body.clone().with_type("generate_ok");
            body.extra.insert("id".to_string(), json!(id));
            return runtime.reply(req, body).await;
        }
        
        done(runtime, req)
    }
}

impl Handler {
    fn generate_id(&self, runtime: &Runtime) -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis();

        let node_id = runtime.node_id();
        let seq = self.counter.fetch_add(1, Ordering::Relaxed).to_string();
        format!("{}-{}-{}", now, node_id, seq)
    }
}
