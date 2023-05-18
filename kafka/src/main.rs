use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use maelstrom::protocol::Message;
use maelstrom::{done, Node, Result, Runtime};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

pub(crate) fn main() -> Result<()> {
    Runtime::init(async {
        let handler = Arc::new(Handler::default());
        Runtime::new().with_handler(handler).run().await
    })
}

#[derive(Default)]
struct Handler {
    logs: RwLock<HashMap<String, Vec<i64>>>,
    committed_offsets: RwLock<HashMap<String, i64>>,
}

impl Handler {
    async fn append_to_log(&self, key: String, msg: i64) -> i64 {
        let mut logs = self.logs.write().await;
        let log = logs.entry(key).or_insert_with(Vec::new);
        log.push(msg);
        (log.len() - 1) as i64
    }
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        let kafka_message: Result<KafkaMessage> = req.body.as_obj();

        match kafka_message {
            Ok(KafkaMessage::Send { key, msg }) => {
                let offset = self.append_to_log(key, msg).await;
                let send_ok = KafkaMessage::SendOk { offset };
                runtime.reply(req, send_ok).await
            }
            Ok(KafkaMessage::Poll { offsets }) => {
                let mut msgs = HashMap::new();
                for (key, offset) in offsets {
                    let logs = self.logs.read().await;

                    let log = match logs.get(&key) {
                        Some(log) => log,
                        None => continue,
                    };

                    // TODO: first find the offset by binary search to avoid filtering.
                    let log_msgs = log
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| *i >= offset as usize)
                        .map(|(i, msg)| [i as i64, *msg])
                        .collect();
                    msgs.insert(key, log_msgs);
                }
                let poll_ok = KafkaMessage::PollOk { msgs };
                runtime.reply(req, poll_ok).await
            }
            Ok(KafkaMessage::CommitOffsets { offsets }) => {
                let mut committed_offsets = self.committed_offsets.write().await;
                for (key, offset) in offsets {
                    committed_offsets.insert(key, offset);
                }
                runtime.reply(req, KafkaMessage::CommitOffsetsOk).await
            }
            Ok(KafkaMessage::ListCommittedOffsets { keys }) => {
                let committed_offsets = self.committed_offsets.read().await;
                let offsets = keys
                    .iter()
                    .filter_map(|key| {
                        committed_offsets
                            .get(key)
                            .map(|offset| (key.clone(), *offset))
                    })
                    .collect();
                runtime
                    .reply(req, KafkaMessage::ListCommittedOffsetsOk { offsets })
                    .await
            }
            _ => done(runtime, req),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum KafkaMessage {
    Send {
        key: String,
        msg: i64,
    },
    SendOk {
        offset: i64,
    },
    Poll {
        offsets: HashMap<String, i64>,
    },
    PollOk {
        msgs: HashMap<String, Vec<[i64; 2]>>,
    },
    CommitOffsets {
        offsets: HashMap<String, i64>,
    },
    CommitOffsetsOk,
    ListCommittedOffsets {
        keys: Vec<String>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<String, i64>,
    },
}
