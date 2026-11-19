//! Cross-language parity: each case in `testdata/wire_golden.json` holds a
//! batch and the bytes msgspec encodes it to. The Rust codec must produce the
//! same bytes and decode them back; `test_kv_events.py` checks the msgspec
//! side, so a schema change on either side fails one of the two.

use serde_json::Value;
use sglang_kv_events::wire::{
    BlockRemoved, BlockStored, KvCacheEvent, KvEventBatch, MmRoutingSpan, StorageMedium, TokenIds,
    decode_event_batch, encode_event_batch,
};

const FIXTURES: &str = include_str!("../testdata/wire_golden.json");

fn ints(value: &Value) -> Vec<i64> {
    let values = value.as_array().expect("an array of integers");
    values.iter().map(|v| v.as_i64().expect("i64")).collect()
}

fn token_ids(value: &Value) -> TokenIds {
    let cells = value.as_array().expect("token_ids array");
    if cells.first().is_some_and(Value::is_array) {
        let pairs = cells.iter().map(|cell| {
            let pair = ints(cell);
            (pair[0], pair[1])
        });
        return TokenIds::Bigram(pairs.collect());
    }
    TokenIds::Unigram(ints(value))
}

fn medium(fields: &Value) -> Option<StorageMedium> {
    fields.get("medium").map(|medium| match medium.as_str() {
        Some("GPU") => StorageMedium::Gpu,
        Some("CPU_PINNED") => StorageMedium::CpuPinned,
        Some("DISK") => StorageMedium::Disk,
        Some("EXTERNAL") => StorageMedium::External,
        other => panic!("unknown medium {other:?}"),
    })
}

fn text(fields: &Value, key: &str) -> Option<std::sync::Arc<str>> {
    fields.get(key).map(|v| v.as_str().expect("string").into())
}

fn spans(fields: &Value) -> Option<Vec<Vec<MmRoutingSpan>>> {
    let blocks = fields.get("mm_routing_spans")?.as_array().expect("blocks");
    let block_spans = |block: &Value| {
        let spans = block.as_array().expect("spans").iter();
        spans
            .map(|span| MmRoutingSpan {
                start: span[0].as_u64().expect("start") as u32,
                length: span[1].as_u64().expect("length") as u32,
                label: span[2].as_str().expect("label").into(),
            })
            .collect()
    };
    Some(blocks.iter().map(block_spans).collect())
}

fn event(fields: &Value) -> KvCacheEvent {
    match fields["type"].as_str().expect("type") {
        "BlockStored" => KvCacheEvent::BlockStored(BlockStored {
            block_hashes: ints(&fields["block_hashes"]),
            parent_block_hash: fields["parent_block_hash"].as_i64(),
            token_ids: token_ids(&fields["token_ids"]),
            block_size: fields["block_size"].as_u64().expect("block_size") as u32,
            lora_id: fields["lora_id"].as_i64(),
            medium: medium(fields),
            cache_salt: text(fields, "cache_salt"),
            session_id: text(fields, "session_id"),
            lora_name: text(fields, "lora_name"),
            mm_routing_spans: spans(fields),
        }),
        "BlockRemoved" => KvCacheEvent::BlockRemoved(BlockRemoved {
            block_hashes: ints(&fields["block_hashes"]),
            medium: medium(fields),
        }),
        "AllBlocksCleared" => KvCacheEvent::AllBlocksCleared,
        other => panic!("unknown event type {other}"),
    }
}

fn batch(value: &Value) -> KvEventBatch {
    KvEventBatch {
        ts: value["ts"].as_f64().expect("ts"),
        events: value["events"]
            .as_array()
            .expect("events")
            .iter()
            .map(event)
            .collect(),
        attn_dp_rank: value["attn_dp_rank"].as_u64().map(|rank| rank as u32),
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn unhex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).expect("hex"))
        .collect()
}

#[test]
fn codec_matches_msgspec_bytes() {
    let fixtures: Value = serde_json::from_str(FIXTURES).unwrap();
    let cases = fixtures["cases"].as_array().expect("cases array");
    assert!(!cases.is_empty());
    for case in cases {
        let name = case["name"].as_str().expect("name");
        let batch = batch(&case["batch"]);
        let golden = case["msgpack_hex"].as_str().expect("msgpack_hex");
        assert_eq!(hex(&encode_event_batch(&batch)), golden, "{name}: encode");
        assert_eq!(
            decode_event_batch(&unhex(golden)).unwrap(),
            batch,
            "{name}: decode"
        );
    }
}
