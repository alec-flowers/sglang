//! The wire batch as the public `sglang.kv.v1` message.

use sglang_kv_v1_types as pb;

use crate::wire;

/// An event without a medium is about device memory, the recorder's default.
pub fn tier(medium: Option<wire::StorageMedium>) -> pb::KvTier {
    match medium {
        None | Some(wire::StorageMedium::Gpu) => pb::KvTier::Device,
        Some(wire::StorageMedium::CpuPinned) => pb::KvTier::Host,
        Some(wire::StorageMedium::Disk) => pb::KvTier::Disk,
        Some(wire::StorageMedium::External) => pb::KvTier::External,
    }
}

pub fn mm_spans(spans: &[wire::MmRoutingSpan]) -> pb::KvMmSpans {
    let span = |span: &wire::MmRoutingSpan| pb::KvMmSpan {
        start: span.start,
        length: span.length,
        label: span.label.to_string(),
    };
    pb::KvMmSpans {
        spans: spans.iter().map(span).collect(),
    }
}

pub fn batch(seq: u64, batch: wire::KvEventBatch) -> pb::KvEventBatch {
    pb::KvEventBatch {
        seq,
        timestamp: batch.ts,
        events: batch.events.into_iter().map(event).collect(),
    }
}

fn event(event: wire::KvCacheEvent) -> pb::KvCacheEvent {
    use pb::kv_cache_event::Event;
    let event = match event {
        wire::KvCacheEvent::BlockStored(stored) => Event::Stored(blocks_stored(stored)),
        wire::KvCacheEvent::BlockRemoved(removed) => Event::Removed(pb::KvBlocksRemoved {
            block_hashes: removed.block_hashes,
            tier: tier(removed.medium) as i32,
        }),
        wire::KvCacheEvent::AllBlocksCleared => Event::Cleared(pb::KvAllBlocksCleared {}),
    };
    pb::KvCacheEvent { event: Some(event) }
}

fn blocks_stored(stored: wire::BlockStored) -> pb::KvBlocksStored {
    let (token_ids, bigram) = match stored.token_ids {
        wire::TokenIds::Unigram(tokens) => (tokens, false),
        wire::TokenIds::Bigram(pairs) => (flatten(&pairs), true),
    };
    let spans = stored.mm_routing_spans.unwrap_or_default();
    pb::KvBlocksStored {
        block_hashes: stored.block_hashes,
        parent_block_hash: stored.parent_block_hash,
        token_ids,
        block_size: stored.block_size,
        tiers: vec![tier(stored.medium) as i32],
        cache_salt: stored.cache_salt.map(|salt| salt.to_string()),
        lora_name: stored.lora_name.map(|name| name.to_string()),
        session_id: stored.session_id.map(|id| id.to_string()),
        mm_routing_spans: spans.iter().map(|block| mm_spans(block)).collect(),
        bigram,
    }
}

/// Bigram positions as the public message carries them: two ids each.
pub fn flatten(pairs: &[(i64, i64)]) -> Vec<i64> {
    pairs.iter().flat_map(|(a, b)| [*a, *b]).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stored_blocks_keep_every_published_field() {
        let stored = wire::BlockStored {
            block_hashes: vec![-7, 8],
            parent_block_hash: Some(-1),
            token_ids: wire::TokenIds::Bigram(vec![(1, 2), (2, 1 << 40)]),
            block_size: 1,
            lora_id: None,
            medium: Some(wire::StorageMedium::CpuPinned),
            cache_salt: Some("salt".into()),
            session_id: Some("session".into()),
            lora_name: Some("adapter".into()),
            mm_routing_spans: Some(vec![
                vec![wire::MmRoutingSpan {
                    start: 0,
                    length: 1,
                    label: "image".into(),
                }],
                vec![],
            ]),
        };
        let wire_batch = wire::KvEventBatch {
            ts: 2.5,
            events: vec![wire::KvCacheEvent::BlockStored(stored)],
            attn_dp_rank: Some(0),
        };
        let batch = batch(9, wire_batch);
        assert_eq!((batch.seq, batch.timestamp), (9, 2.5));
        let Some(pb::kv_cache_event::Event::Stored(stored)) = batch.events[0].event.clone() else {
            panic!("expected a stored event");
        };
        let expected = pb::KvBlocksStored {
            block_hashes: vec![-7, 8],
            parent_block_hash: Some(-1),
            // A bigram position is two ids.
            token_ids: vec![1, 2, 2, 1 << 40],
            bigram: true,
            block_size: 1,
            tiers: vec![pb::KvTier::Host as i32],
            cache_salt: Some("salt".to_string()),
            lora_name: Some("adapter".to_string()),
            session_id: Some("session".to_string()),
            mm_routing_spans: vec![
                pb::KvMmSpans {
                    spans: vec![pb::KvMmSpan {
                        start: 0,
                        length: 1,
                        label: "image".to_string(),
                    }],
                },
                pb::KvMmSpans { spans: vec![] },
            ],
        };
        assert_eq!(stored, expected);
    }
}
