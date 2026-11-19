//! Follow one rank's `sglang.kv.v1.KvEventService` stream and print what it
//! says about the cache.
//!
//! ```text
//! cargo run -p sglang-kv-events --features relay --example kv_tail -- \
//!     http://127.0.0.1:30001 [snapshot|live|from-start]
//! ```
//!
//! `snapshot` (the default) loads the whole tree and then follows; `live`
//! follows without the state before it; `from-start` resumes from sequence 0
//! and fails if the server no longer holds it. Every line ends with the
//! number of blocks and a digest of the view built so far, so two clients
//! that started differently can be compared at the same cursor.

use std::collections::BTreeMap;
use std::hash::{DefaultHasher, Hash, Hasher};

use sglang_kv_v1_types as pb;
use sglang_kv_v1_types::kv_cache_event::Event;
use sglang_kv_v1_types::kv_event_service_client::KvEventServiceClient;
use sglang_kv_v1_types::kv_stream_item::Item;

/// The consumer's view: for each block, its parent and the tiers holding it.
#[derive(Default, Hash)]
struct View {
    blocks: BTreeMap<i64, (Option<i64>, u8)>,
}

impl View {
    fn store(&mut self, stored: &pb::KvBlocksStored) {
        let tiers = stored.tiers.iter().fold(0u8, |mask, tier| mask | 1 << tier);
        let mut parent = stored.parent_block_hash;
        for hash in &stored.block_hashes {
            self.blocks.entry(*hash).or_insert((parent, 0)).1 |= tiers;
            parent = Some(*hash);
        }
    }

    fn remove(&mut self, removed: &pb::KvBlocksRemoved) {
        for hash in &removed.block_hashes {
            if let Some((_, tiers)) = self.blocks.get_mut(hash) {
                *tiers &= !(1 << removed.tier);
                if *tiers == 0 {
                    self.blocks.remove(hash);
                }
            }
        }
    }

    fn apply(&mut self, batch: &pb::KvEventBatch) {
        for event in &batch.events {
            match &event.event {
                Some(Event::Stored(stored)) => self.store(stored),
                Some(Event::Removed(removed)) => self.remove(removed),
                Some(Event::Cleared(_)) => self.blocks.clear(),
                None => {}
            }
        }
    }

    fn summary(&self) -> String {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        format!(
            "blocks={} digest={:016x}",
            self.blocks.len(),
            hasher.finish()
        )
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let endpoint = args
        .next()
        .ok_or("usage: kv_tail <grpc url> [snapshot|live|from-start]")?;
    let mode = args.next().unwrap_or_else(|| "snapshot".to_string());

    let mut client = KvEventServiceClient::connect(endpoint)
        .await?
        .max_decoding_message_size(64 * 1024 * 1024);
    let info = client
        .get_kv_stream_info(pb::GetKvStreamInfoRequest {})
        .await?;
    let info = info.into_inner();
    println!(
        "stream publisher_id={} dp_rank={} block_size={} history=[{}, {}) blocks={} complete={} \
         reseed_supported={}",
        info.publisher_id,
        info.dp_rank,
        info.block_size,
        info.oldest_seq,
        info.next_seq,
        info.blocks,
        info.complete,
        info.reseed_supported
    );

    let (resume, on_miss) = match mode.as_str() {
        "snapshot" => (None, pb::KvCursorMiss::Snapshot),
        "live" => (None, pb::KvCursorMiss::LiveOnly),
        "from-start" => {
            let cursor = pb::KvCursor {
                publisher_id: info.publisher_id.clone(),
                next_seq: 0,
            };
            (Some(cursor), pb::KvCursorMiss::Fail)
        }
        other => return Err(format!("unknown mode {other:?}").into()),
    };
    let request = pb::SubscribeKvEventsRequest {
        resume,
        on_miss: on_miss as i32,
        max_chunk_bytes: 0,
    };
    let mut stream = client.subscribe_kv_events(request).await?.into_inner();

    let mut view = View::default();
    // A snapshot replaces the view only once it is complete.
    let mut staged = View::default();
    while let Some(item) = stream.message().await? {
        match item.item {
            Some(Item::Start(start)) => {
                println!("start mode={:?} next_seq={}", start.mode(), start.next_seq);
            }
            Some(Item::Snapshot(chunk)) => {
                for stored in &chunk.stored {
                    staged.store(stored);
                }
                println!("snapshot chunk={} runs={}", chunk.index, chunk.stored.len());
            }
            Some(Item::SnapshotEnd(end)) => {
                view = std::mem::take(&mut staged);
                println!(
                    "snapshot end chunks={} next_seq={} {}",
                    end.chunks,
                    end.next_seq,
                    view.summary()
                );
            }
            Some(Item::Batch(batch)) => {
                view.apply(&batch);
                println!(
                    "batch seq={} events={} next_seq={} {}",
                    batch.seq,
                    batch.events.len(),
                    batch.seq + 1,
                    view.summary()
                );
            }
            Some(Item::Heartbeat(heartbeat)) => {
                println!("idle next_seq={} {}", heartbeat.next_seq, view.summary());
            }
            None => {}
        }
    }
    Ok(())
}
