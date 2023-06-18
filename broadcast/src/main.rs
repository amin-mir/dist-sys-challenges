use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use crossbeam_queue::SegQueue;
use maelstrom::protocol::Message;
use maelstrom::{done, Node, Result, Runtime};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio::time::{self, Duration};
use tokio_context::context::Context;

pub(crate) fn main() -> Result<()> {
    eprintln!("##### Starting application");

    Runtime::init(async {
        let runtime = Runtime::new();

        let handler = Arc::new(Handler::new(runtime.clone()));
        handler.retry_failed_calls().await;

        runtime.with_handler(handler).run().await
    })
}

#[derive(Default)]
struct Handler {
    // TODO: make this data structure lock-free.
    runtime: Runtime,
    inner: Mutex<Inner>,
    failed_calls: Arc<SegQueue<(String, BroadcastMessage)>>,
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
                self.inner.lock().await.data.push(message);

                let failed_calls = self.failed_calls.clone();
                let rt = runtime.clone();

                let handle = runtime.spawn(async move {
                    eprintln!(
                        "###### neighbors ={:?}, for node = {}",
                        rt.neighbours().collect::<Vec<_>>(),
                        rt.node_id(),
                    );
                    for node in rt.neighbours() {
                        let (ctx, _handle) = Context::new();
                        let msg = BroadcastMessage::InternalBroadcast { message };
                        if rt.call(ctx, node, msg.clone()).await.is_err() {
                            failed_calls.push((node.to_string(), msg));
                        }
                    }
                });

                runtime.reply(req, BroadcastMessage::BroadcastOk).await?;
                handle.await?;
                Ok(())
            }
            Ok(BroadcastMessage::InternalBroadcast { message }) => {
                self.inner.lock().await.data.push(message);
                runtime.reply(req, BroadcastMessage::InterBroadcastOk).await
            }
            Ok(BroadcastMessage::Read) => {
                let data = self.inner.lock().await.data.clone();
                runtime
                    .reply(req, BroadcastMessage::ReadOk { messages: data })
                    .await
            }
            Ok(BroadcastMessage::Topology { topology }) => {
                self.inner.lock().await.topology = topology;
                eprintln!(
                    "###### topology ={:?}, for node = {}",
                    self.inner.lock().await.topology,
                    runtime.node_id(),
                );
                runtime.reply(req, BroadcastMessage::TopologyOk).await
            }
            _ => done(runtime, req),
        }
    }
}

impl Handler {
    fn new(runtime: Runtime) -> Self {
        Self {
            runtime,
            failed_calls: Arc::new(SegQueue::new()),
            ..Default::default()
        }
    }

    async fn retry_failed_calls(self: &Arc<Self>) {
        let this = self.clone();

        self.runtime.spawn(async move {
            loop {
                time::sleep(Duration::from_millis(100)).await;
                eprintln!(
                    "###### retrying failed calls, len before = {}",
                    this.failed_calls.len(),
                );

                let len = this.failed_calls.len();

                for _ in 0..len {
                    let (ctx, _handle) = Context::new();
                    let (node, msg) = this.failed_calls.pop().unwrap();

                    if this
                        .runtime
                        .call(ctx, node.clone(), msg.clone())
                        .await
                        .is_err()
                    {
                        this.failed_calls.push((node.clone(), msg));
                    }
                }

                // eprintln!(
                //     "###### retired the failed calls, len after = {}",
                //     this.failed_calls.len(),
                // );
            }
        });
    }
}

#[derive(Serialize, Deserialize, Clone)]
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
