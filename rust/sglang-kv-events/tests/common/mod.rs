//! Shared by the socket-level tests.
#![allow(dead_code)]

use std::time::Duration;

use bytes::Bytes;
use sglang_kv_events::publisher::END_SEQ;
use sglang_kv_events::subscriber::SubscriberConfig;
use sglang_kv_events::wire::{
    BlockStored, KvCacheEvent, KvEventBatch, StorageMedium, TokenIds, encode_event_batch,
};
use tokio::time::timeout;
use zeromq::{PubSocket, RouterSocket, Socket, SocketRecv, SocketSend, ZmqMessage};

pub const WAIT: Duration = Duration::from_secs(10);

/// An encoded batch that stores the one block `hash`.
pub fn payload(hash: u64) -> Bytes {
    let stored = BlockStored {
        block_hashes: vec![hash as i64],
        parent_block_hash: None,
        token_ids: TokenIds::Unigram(vec![hash as i64]),
        block_size: 1,
        lora_id: None,
        medium: Some(StorageMedium::Gpu),
        cache_salt: None,
        session_id: None,
        lora_name: None,
        mm_routing_spans: None,
    };
    Bytes::from(encode_event_batch(&KvEventBatch {
        ts: 1.0,
        events: vec![KvCacheEvent::BlockStored(stored)],
        attn_dp_rank: Some(0),
    }))
}

/// A publisher that only sends live what the test tells it to, and replays
/// whatever the test has put in its ring. Batch `seq` carries `payload(seq)`.
pub struct WithholdingPublisher {
    live: PubSocket,
    replay: RouterSocket,
    pub ring: Vec<u64>,
    pub config: SubscriberConfig,
}

impl WithholdingPublisher {
    pub async fn bind() -> Self {
        let mut live = PubSocket::new();
        let endpoint = live.bind("tcp://127.0.0.1:0").await.unwrap().to_string();
        let mut replay = RouterSocket::new();
        let replay_endpoint = replay.bind("tcp://127.0.0.1:0").await.unwrap().to_string();
        let config = SubscriberConfig {
            endpoint,
            replay_endpoint,
            topic: "kv".to_string(),
        };
        Self {
            live,
            replay,
            ring: Vec::new(),
            config,
        }
    }

    pub async fn send_live(&mut self, seq: u64) {
        let frames = vec![
            Bytes::from_static(b"kv"),
            Bytes::copy_from_slice(&seq.to_be_bytes()),
            payload(seq),
        ];
        self.live.send(frames.try_into().unwrap()).await.unwrap();
    }

    /// Answer the next replay request from the ring; returns its start.
    pub async fn serve_replay(&mut self) -> u64 {
        let request = timeout(WAIT, self.replay.recv()).await;
        let request = request.expect("no replay request").unwrap().into_vec();
        let start = u64::from_be_bytes(request[2].as_ref().try_into().unwrap());
        let reply = |topic: &'static [u8], seq: [u8; 8], payload: Bytes| {
            let frames = vec![
                request[0].clone(),
                Bytes::new(),
                Bytes::from_static(topic),
                Bytes::copy_from_slice(&seq),
                payload,
            ];
            ZmqMessage::try_from(frames).unwrap()
        };
        for seq in self.ring.iter().filter(|seq| **seq >= start) {
            let message = reply(b"kv", seq.to_be_bytes(), payload(*seq));
            self.replay.send(message).await.unwrap();
        }
        let end = reply(b"", END_SEQ, Bytes::new());
        self.replay.send(end).await.unwrap();
        start
    }
}
