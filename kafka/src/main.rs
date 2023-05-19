use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use maelstrom::kv::{lin_kv, Storage, KV};
use maelstrom::protocol::Message;
use maelstrom::{done, Node, Result, Runtime};
use serde::{Deserialize, Serialize};
use tokio_context::context::{Context, Handle as CtxHandle};

pub(crate) fn main() -> Result<()> {
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
            storage: lin_kv(runtime),
        }
    }
}

impl Handler {
    async fn get_next_offset(&self, ctx_handle: &mut CtxHandle, log_name: String) -> i64 {
        let next_offset_key = get_next_offset_key(log_name.clone());
        loop {
            let get_res = self
                .storage
                .get(ctx_handle.spawn_ctx(), next_offset_key.clone())
                .await;

            let old_offset = match get_res {
                Ok(Some(offset)) => offset,
                _ => -1,
            };
            let new_offset = old_offset + 1;

            let set_res = self
                .storage
                .cas(
                    ctx_handle.spawn_ctx(),
                    next_offset_key.clone(),
                    old_offset,
                    new_offset,
                    true,
                )
                .await;

            if set_res.is_ok() {
                return new_offset;
            }
        }
    }

    async fn append_to_log(
        &self,
        ctx_handle: &mut CtxHandle,
        key: String,
        offset: i64,
        msg: i64,
    ) -> Result<()> {
        let log_key = get_log_msg_key(key, offset);

        // No one should have access to the offset that is passed here to us.
        // get_next_offset makes sure of that, so here we can simply put the message.
        self.storage.put(ctx_handle.spawn_ctx(), log_key, msg).await
    }

    async fn get_log_msgs_at_offset(
        &self,
        ctx_handle: &mut CtxHandle,
        log_name: String,
        offset: i64,
    ) -> Option<Vec<[i64; 2]>> {
        let mut msgs = Vec::new();
        let mut current_offset = offset;
        loop {
            let log_key = get_log_msg_key(log_name.clone(), current_offset);
            let get_res = self.storage.get(ctx_handle.spawn_ctx(), log_key).await;

            match get_res {
                Ok(Some(msg)) => {
                    msgs.push([current_offset, msg]);
                    current_offset += 1;
                }
                _ => break,
            }
        }

        if msgs.is_empty() {
            None
        } else {
            Some(msgs)
        }
    }

    async fn commit_offsets(&self, ctx_handle: &mut CtxHandle, log_name: String, offset: i64) {
        let committed_offset_key = get_log_committed_offset_key(log_name);

        loop {
            let get_res = self
                .storage
                .get(ctx_handle.spawn_ctx(), committed_offset_key.clone())
                .await;

            let current_offset = match get_res {
                Ok(Some(offset)) => offset,
                _ => 0,
            };

            if current_offset >= offset {
                return;
            }

            let set_res = self
                .storage
                .cas(
                    ctx_handle.spawn_ctx(),
                    committed_offset_key.clone(),
                    current_offset,
                    offset,
                    true,
                )
                .await;

            if set_res.is_ok() {
                return;
            }
        }
    }

    async fn get_committed_offset(
        &self,
        ctx_handle: &mut CtxHandle,
        log_name: String,
    ) -> Option<i64> {
        let committed_offset_key = get_log_committed_offset_key(log_name);

        let get_res = self
            .storage
            .get(ctx_handle.spawn_ctx(), committed_offset_key)
            .await;

        get_res.ok()
    }
}

fn get_next_offset_key(log: String) -> String {
    format!("log.{}.next_offset", log)
}

fn get_log_msg_key(log: String, offset: i64) -> String {
    format!("log.{}.msg.{}", log, offset)
}

fn get_log_committed_offset_key(log: String) -> String {
    format!("log.{}.committed_offset", log)
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        let kafka_message: Result<KafkaMessage> = req.body.as_obj();

        match kafka_message {
            Ok(KafkaMessage::Send { key, msg }) => {
                let (_ctx, mut ctx_handle) = Context::new();

                let offset = self.get_next_offset(&mut ctx_handle, key.clone()).await;
                self.append_to_log(&mut ctx_handle, key, offset, msg)
                    .await?;
                let send_ok = KafkaMessage::SendOk { offset };
                runtime.reply(req, send_ok).await
            }
            Ok(KafkaMessage::Poll { offsets }) => {
                let (_ctx, mut ctx_handle) = Context::new();

                let mut msgs = HashMap::new();

                for (key, offset) in offsets {
                    let log_msgs = self
                        .get_log_msgs_at_offset(&mut ctx_handle, key.clone(), offset)
                        .await;

                    if let Some(log_msgs) = log_msgs {
                        msgs.insert(key, log_msgs);
                    }
                }

                let poll_ok = KafkaMessage::PollOk { msgs };
                runtime.reply(req, poll_ok).await
            }
            Ok(KafkaMessage::CommitOffsets { offsets }) => {
                let (_ctx, mut ctx_handle) = Context::new();

                for (log_name, offset) in offsets {
                    self.commit_offsets(&mut ctx_handle, log_name, offset).await;
                }
                runtime.reply(req, KafkaMessage::CommitOffsetsOk).await
            }
            Ok(KafkaMessage::ListCommittedOffsets { keys }) => {
                let (_ctx, mut ctx_handle) = Context::new();

                let mut offsets = HashMap::new();

                for key in keys {
                    let committed_offset = self
                        .get_committed_offset(&mut ctx_handle, key.clone())
                        .await;

                    if let Some(committed_offset) = committed_offset {
                        offsets.insert(key.clone(), committed_offset);
                    }
                }

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
