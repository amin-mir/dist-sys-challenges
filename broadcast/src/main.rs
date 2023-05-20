use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use maelstrom::protocol::Message;
use maelstrom::{done, Node, Result, Runtime};
use serde::{Deserialize, Serialize};

pub(crate) fn main() -> Result<()> {
    Runtime::init(async {
        let handler = Arc::new(Handler::default());
        Runtime::new().with_handler(handler).run().await
    })
}

#[derive(Default)]
struct Handler {
    // TODO: make this data structure lock-free.
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    data: Vec<i64>,
    topology: HashMap<String, Vec<String>>,
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        let broadcast_msg: Result<BroadcastMessage> = req.body.as_obj();
        match broadcast_msg {
            Ok(BroadcastMessage::Broadcast { message }) => {
                self.inner.lock().unwrap().data.push(message);

                for node in runtime.neighbours() {
                    let msg = BroadcastMessage::InternalBroadcast { message };
                    runtime.call_async(node, msg);
                }

                runtime.reply(req, BroadcastMessage::BroadcastOk).await
            }
            Ok(BroadcastMessage::InternalBroadcast { message }) => {
                self.inner.lock().unwrap().data.push(message);
                runtime.reply(req, BroadcastMessage::InterBroadcastOk).await
            }
            Ok(BroadcastMessage::Read) => {
                let data = self.inner.lock().unwrap().data.clone();
                runtime
                    .reply(req, BroadcastMessage::ReadOk { messages: data })
                    .await
            }
            Ok(BroadcastMessage::Topology { topology }) => {
                self.inner.lock().unwrap().topology = topology;
                runtime.reply(req, BroadcastMessage::TopologyOk).await
            }
            _ => done(runtime, req),
        }
    }
}

impl Handler {}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum BroadcastMessage {
    Broadcast {
        message: i64,
    },
    BroadcastOk,
    InternalBroadcast {
        message: i64,
    },
    InterBroadcastOk,
    Read,
    ReadOk {
        messages: Vec<i64>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
}
