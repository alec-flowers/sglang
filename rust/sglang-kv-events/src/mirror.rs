//! A relay's copy of one rank's tree, folded from its event stream, and the
//! snapshot built from it.
//!
//! A block is present in a set of tiers: a store adds its tier, a remove
//! takes it away, and the block is gone when no tier holds it. This is the
//! view any consumer of the stream builds, so a snapshot of it followed by
//! the batches after the cut leaves a consumer exactly where one that
//! followed from the first batch is.

use std::collections::HashMap;
use std::sync::Arc;

use prost::Message;
use sglang_kv_v1_types as pb;

use crate::convert;
use crate::wire;

// Arbitrary. A snapshot run is cut here so that a chunk can stay small and a
// consumer applies it in bounded steps.
const MAX_RUN_BLOCKS: usize = 1024;
const MAX_RUN_TOKENS: usize = 16 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
enum Tokens {
    /// The common case of one token per block, kept off the heap.
    One(i64),
    Many(Box<[i64]>),
}

impl Tokens {
    fn new(tokens: &[i64]) -> Self {
        match tokens {
            [token] => Tokens::One(*token),
            tokens => Tokens::Many(tokens.into()),
        }
    }

    fn as_slice(&self) -> &[i64] {
        match self {
            Tokens::One(token) => std::slice::from_ref(token),
            Tokens::Many(tokens) => tokens,
        }
    }
}

/// What a store says about its blocks besides their place in the tree.
#[derive(Debug, PartialEq, Eq)]
struct Attributes {
    cache_salt: Option<Arc<str>>,
    lora_name: Option<Arc<str>>,
}

#[derive(Clone, Debug)]
struct Block {
    parent: Option<i64>,
    /// Bit `t` is set while tier `t` (a `KvTier` value) holds the block.
    tiers: u8,
    /// Two ids per position when `bigram`.
    tokens: Tokens,
    bigram: bool,
    /// Shared by the blocks of one store; `None` when it carries neither.
    attributes: Option<Arc<Attributes>>,
}

#[derive(Clone, Default)]
pub struct Mirror {
    blocks: HashMap<i64, Block>,
    /// Routing spans of the few blocks that have labeled multimodal tokens.
    spans: HashMap<i64, Arc<[wire::MmRoutingSpan]>>,
}

fn tier_bit(medium: Option<wire::StorageMedium>) -> u8 {
    1 << (convert::tier(medium) as u8)
}

impl Mirror {
    /// Blocks in the tree, each counted once whatever tiers hold it.
    pub fn len(&self) -> usize {
        self.blocks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }

    pub fn fold(&mut self, batch: &wire::KvEventBatch) {
        for event in &batch.events {
            match event {
                wire::KvCacheEvent::BlockStored(stored) => self.store(stored),
                wire::KvCacheEvent::BlockRemoved(removed) => {
                    let tier = tier_bit(removed.medium);
                    for hash in &removed.block_hashes {
                        self.remove(*hash, tier);
                    }
                }
                wire::KvCacheEvent::AllBlocksCleared => {
                    self.blocks.clear();
                    self.spans.clear();
                }
            }
        }
    }

    fn store(&mut self, stored: &wire::BlockStored) {
        let tier = tier_bit(stored.medium);
        let attributes = (stored.cache_salt.is_some() || stored.lora_name.is_some()).then(|| {
            Arc::new(Attributes {
                cache_salt: stored.cache_salt.clone(),
                lora_name: stored.lora_name.clone(),
            })
        });
        let flat;
        let (tokens, ids_per_block, bigram): (&[i64], usize, bool) = match &stored.token_ids {
            wire::TokenIds::Unigram(tokens) => (tokens, stored.block_size as usize, false),
            wire::TokenIds::Bigram(pairs) => {
                flat = convert::flatten(pairs);
                (&flat, 2 * stored.block_size as usize, true)
            }
        };
        let mut block_tokens = tokens.chunks(ids_per_block.max(1));
        let mut parent = stored.parent_block_hash;
        for (index, hash) in stored.block_hashes.iter().enumerate() {
            let tokens = block_tokens.next().unwrap_or_default();
            // A block stored again, in another tier or by another request,
            // keeps what the first store said about it.
            let block = self.blocks.entry(*hash).or_insert_with(|| Block {
                parent,
                tiers: 0,
                tokens: Tokens::new(tokens),
                bigram,
                attributes: attributes.clone(),
            });
            block.tiers |= tier;
            let spans = stored
                .mm_routing_spans
                .as_ref()
                .and_then(|spans| spans.get(index));
            if let Some(spans) = spans.filter(|spans| !spans.is_empty()) {
                self.spans
                    .entry(*hash)
                    .or_insert_with(|| spans.as_slice().into());
            }
            parent = Some(*hash);
        }
    }

    fn remove(&mut self, hash: i64, tier: u8) {
        if let Some(block) = self.blocks.get_mut(&hash) {
            block.tiers &= !tier;
            if block.tiers == 0 {
                self.blocks.remove(&hash);
                self.spans.remove(&hash);
            }
        }
    }

    /// The tree as stores that rebuild it in an empty index: a block always
    /// comes after its parent, and a chain of blocks that differ only in
    /// their tokens is one store.
    pub fn export(&self) -> Vec<pb::KvBlocksStored> {
        let mut children: HashMap<i64, Vec<i64>> = HashMap::new();
        let mut roots = Vec::new();
        for (hash, block) in &self.blocks {
            match block.parent {
                Some(parent) if self.blocks.contains_key(&parent) => {
                    children.entry(parent).or_default().push(*hash);
                }
                // At the root, or below a block that is gone from every tier.
                _ => roots.push(*hash),
            }
        }
        // Sorted, so that the same tree always exports the same snapshot.
        roots.sort_unstable_by(|a, b| b.cmp(a));
        for siblings in children.values_mut() {
            siblings.sort_unstable_by(|a, b| b.cmp(a));
        }

        let mut runs = Vec::new();
        let mut exported = 0;
        let mut starts = roots;
        while let Some(start) = starts.pop() {
            let mut run = Run::new(&self.blocks[&start]);
            let mut hash = start;
            loop {
                run.push(hash, &self.blocks[&hash], self.spans.get(&hash));
                let next = children.get(&hash).map(Vec::as_slice).unwrap_or_default();
                match next {
                    [only] if run.continues_with(&self.blocks[only]) => hash = *only,
                    next => {
                        starts.extend_from_slice(next);
                        break;
                    }
                }
            }
            exported += run.stored.block_hashes.len();
            runs.push(run.finish());
        }
        if exported != self.blocks.len() {
            // Only a parent cycle hides blocks from the walk above, and the
            // tree has none; do not let one pass as a complete snapshot.
            tracing::error!(
                exported,
                blocks = self.blocks.len(),
                "KV snapshot does not cover the mirrored tree"
            );
        }
        runs
    }
}

struct Run<'a> {
    stored: pb::KvBlocksStored,
    tiers: u8,
    attributes: &'a Option<Arc<Attributes>>,
    ids_per_block: usize,
    has_spans: bool,
}

impl<'a> Run<'a> {
    fn new(first: &'a Block) -> Self {
        let attributes = first.attributes.as_deref();
        let tiers = (0..8i32).filter(|tier| first.tiers >> tier & 1 == 1);
        Self {
            stored: pb::KvBlocksStored {
                parent_block_hash: first.parent,
                tiers: tiers.collect(),
                cache_salt: attributes.and_then(|a| a.cache_salt.as_deref().map(str::to_string)),
                lora_name: attributes.and_then(|a| a.lora_name.as_deref().map(str::to_string)),
                block_size: (first.tokens.as_slice().len() / if first.bigram { 2 } else { 1 })
                    as u32,
                bigram: first.bigram,
                ..Default::default()
            },
            tiers: first.tiers,
            attributes: &first.attributes,
            ids_per_block: first.tokens.as_slice().len(),
            has_spans: false,
        }
    }

    fn continues_with(&self, block: &Block) -> bool {
        block.tiers == self.tiers
            && block.attributes == *self.attributes
            && block.tokens.as_slice().len() == self.ids_per_block
            && block.bigram == self.stored.bigram
            && self.stored.block_hashes.len() < MAX_RUN_BLOCKS
            && self.stored.token_ids.len() < MAX_RUN_TOKENS
    }

    fn push(&mut self, hash: i64, block: &Block, spans: Option<&Arc<[wire::MmRoutingSpan]>>) {
        if let Some(spans) = spans {
            // Per-block entries are positional: catch up on the blocks before.
            let blocks_so_far = self.stored.block_hashes.len();
            self.stored
                .mm_routing_spans
                .resize_with(blocks_so_far, Default::default);
            self.stored.mm_routing_spans.push(convert::mm_spans(spans));
            self.has_spans = true;
        } else if self.has_spans {
            self.stored.mm_routing_spans.push(Default::default());
        }
        self.stored.block_hashes.push(hash);
        self.stored
            .token_ids
            .extend_from_slice(block.tokens.as_slice());
    }

    fn finish(self) -> pb::KvBlocksStored {
        self.stored
    }
}

// What a KvStreamItem adds around its chunk's stores: the item's tag and
// length, and the chunk's index field.
const CHUNK_ENVELOPE_BYTES: usize = 12;

/// Group a snapshot's stores into chunks whose KvStreamItem encodes to at most
/// `max_chunk_bytes`; a store larger than that is a chunk of its own.
pub fn chunks(
    stores: &[pb::KvBlocksStored],
    max_chunk_bytes: usize,
) -> impl Iterator<Item = pb::KvSnapshotChunk> + '_ {
    let mut rest = stores;
    let mut index = 0;
    std::iter::from_fn(move || {
        let mut bytes = CHUNK_ENVELOPE_BYTES;
        let mut fits = |store: &pb::KvBlocksStored| {
            let first = bytes == CHUNK_ENVELOPE_BYTES;
            let body = store.encoded_len();
            // As a repeated field: a tag, the body's length, the body.
            bytes += 1 + prost::length_delimiter_len(body) + body;
            first || bytes <= max_chunk_bytes
        };
        let take = rest.iter().take_while(|store| fits(store)).count();
        if take == 0 {
            return None;
        }
        let (stored, remaining) = rest.split_at(take);
        rest = remaining;
        index += 1;
        Some(pb::KvSnapshotChunk {
            index: index - 1,
            stored: stored.to_vec(),
        })
    })
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;

    /// What a consumer that applies every event one block at a time ends up
    /// with: for each block its parent, tokens, attributes and tiers.
    type View = BTreeMap<i64, (Option<i64>, Vec<i64>, Option<String>, BTreeSet<i32>)>;

    fn apply_store(view: &mut View, stored: &pb::KvBlocksStored) {
        let ids_per_block = stored.token_ids.len() / stored.block_hashes.len();
        let mut parent = stored.parent_block_hash;
        for (index, hash) in stored.block_hashes.iter().enumerate() {
            let tokens = &stored.token_ids[index * ids_per_block..][..ids_per_block];
            let salt = stored.cache_salt.clone();
            let block =
                view.entry(*hash)
                    .or_insert((parent, tokens.to_vec(), salt, BTreeSet::new()));
            block.3.extend(&stored.tiers);
            parent = Some(*hash);
        }
    }

    fn apply(view: &mut View, batch: &wire::KvEventBatch) {
        use pb::kv_cache_event::Event;
        for event in convert::batch(0, batch.clone()).events {
            match event.event.unwrap() {
                Event::Stored(stored) => apply_store(view, &stored),
                Event::Removed(removed) => {
                    for hash in &removed.block_hashes {
                        let gone = view.get_mut(hash).is_some_and(|block| {
                            block.3.remove(&removed.tier);
                            block.3.is_empty()
                        });
                        if gone {
                            view.remove(hash);
                        }
                    }
                }
                Event::Cleared(_) => view.clear(),
            }
        }
    }

    /// A deterministic stream of stores along shared prefixes, removes in
    /// either tier and the occasional clear.
    struct Stream {
        state: u64,
        next_hash: i64,
        chains: Vec<Vec<i64>>,
    }

    impl Stream {
        fn random(&mut self, bound: u64) -> u64 {
            // Knuth's MMIX constants; any full-period LCG does.
            self.state = self
                .state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (self.state >> 33) % bound
        }

        fn medium(&mut self) -> Option<wire::StorageMedium> {
            [wire::StorageMedium::Gpu, wire::StorageMedium::CpuPinned]
                .get(self.random(2) as usize)
                .copied()
        }

        fn store(&mut self) -> wire::KvCacheEvent {
            // Extend a prefix of an existing chain, or start at the root.
            let mut chain = match self.random_chain() {
                Some(chain) => {
                    let keep = self.random(chain.len() as u64 + 1) as usize;
                    chain[..keep].to_vec()
                }
                None => Vec::new(),
            };
            let parent_block_hash = chain.last().copied();
            let block_hashes: Vec<i64> = (0..1 + self.random(4))
                .map(|_| {
                    self.next_hash += 1;
                    self.next_hash
                })
                .collect();
            chain.extend(&block_hashes);
            self.chains.push(chain);
            self.stored(block_hashes, parent_block_hash)
        }

        fn random_chain(&mut self) -> Option<Vec<i64>> {
            if self.chains.is_empty() {
                return None;
            }
            let index = self.random(self.chains.len() as u64) as usize;
            Some(self.chains[index].clone())
        }

        /// The same blocks again, in whichever tier comes up.
        fn restore(&mut self) -> Option<wire::KvCacheEvent> {
            let chain = self.random_chain()?;
            let split = self.random(chain.len() as u64) as usize;
            let parent_block_hash = split.checked_sub(1).map(|index| chain[index]);
            let block_hashes = chain[split..].to_vec();
            Some(self.stored(block_hashes, parent_block_hash))
        }

        fn stored(
            &mut self,
            block_hashes: Vec<i64>,
            parent_block_hash: Option<i64>,
        ) -> wire::KvCacheEvent {
            // Two tokens per block, a function of the hash as in the real tree.
            let tokens = block_hashes.iter().flat_map(|hash| [*hash, hash << 40]);
            wire::KvCacheEvent::BlockStored(wire::BlockStored {
                token_ids: wire::TokenIds::Unigram(tokens.collect()),
                block_size: 2,
                lora_id: None,
                medium: self.medium(),
                // Salted when the chain's root is: an attribute of the whole chain.
                cache_salt: (block_hashes[0] % 3 == 0).then(|| "salt".into()),
                session_id: None,
                lora_name: None,
                mm_routing_spans: None,
                block_hashes,
                parent_block_hash,
            })
        }

        fn remove(&mut self) -> wire::KvCacheEvent {
            let block_hashes = (0..1 + self.random(6))
                .map(|_| 1 + self.random(self.next_hash.max(1) as u64) as i64)
                .collect();
            wire::KvCacheEvent::BlockRemoved(wire::BlockRemoved {
                block_hashes,
                medium: self.medium(),
            })
        }

        fn batch(&mut self) -> wire::KvEventBatch {
            let events = (0..1 + self.random(3)).filter_map(|_| match self.random(100) {
                0 => Some(wire::KvCacheEvent::AllBlocksCleared),
                1..40 => Some(self.remove()),
                40..55 => self.restore(),
                _ => Some(self.store()),
            });
            wire::KvEventBatch {
                ts: 0.0,
                events: events.collect(),
                attn_dp_rank: Some(0),
            }
        }
    }

    #[test]
    fn snapshot_leaves_a_consumer_where_the_whole_stream_would() {
        for seed in 0..20 {
            let mut stream = Stream {
                state: seed,
                next_hash: 0,
                chains: Vec::new(),
            };
            let mut mirror = Mirror::default();
            let mut followed = View::new();
            for step in 0..300 {
                let batch = stream.batch();
                mirror.fold(&batch);
                apply(&mut followed, &batch);
                if step % 7 != 0 {
                    continue;
                }
                let mut loaded = View::new();
                for stored in mirror.export() {
                    let ids = stored.block_hashes.len() * stored.block_size as usize;
                    assert_eq!((stored.token_ids.len(), stored.block_size), (ids, 2));
                    // A consumer links a block to a parent it already holds.
                    if let Some(parent) = stored.parent_block_hash {
                        assert_eq!(followed.contains_key(&parent), loaded.contains_key(&parent));
                    }
                    apply_store(&mut loaded, &stored);
                }
                assert_eq!(loaded, followed, "seed {seed} step {step}");
                assert_eq!(mirror.len(), followed.len());
            }
        }
    }

    #[test]
    fn chunks_respect_the_byte_limit_and_keep_every_store_in_order() {
        let stored = |blocks: i64| pb::KvBlocksStored {
            block_hashes: (0..blocks).collect(),
            token_ids: (0..blocks).collect(),
            block_size: 1,
            ..Default::default()
        };
        let stores = [stored(10), stored(10), stored(200), stored(1), stored(1)];
        let item_len = |chunk: &pb::KvSnapshotChunk| {
            let item = pb::kv_stream_item::Item::Snapshot(chunk.clone());
            pb::KvStreamItem { item: Some(item) }.encoded_len()
        };
        // Room for the first two stores, each framed by a tag and a length byte.
        let limit = CHUNK_ENVELOPE_BYTES + 2 * (2 + stores[0].encoded_len());
        let chunks: Vec<_> = chunks(&stores, limit).collect();
        // The 200-block store exceeds the limit and travels alone.
        let sizes: Vec<_> = chunks.iter().map(|chunk| chunk.stored.len()).collect();
        assert_eq!(sizes, [2, 1, 2]);
        // The limit bounds the message a client receives, not the stores in it.
        assert!(item_len(&chunks[0]) <= limit && item_len(&chunks[2]) <= limit);
        assert_eq!(
            chunks.iter().map(|chunk| chunk.index).collect::<Vec<_>>(),
            [0, 1, 2]
        );
        let rejoined: Vec<_> = chunks.into_iter().flat_map(|chunk| chunk.stored).collect();
        assert_eq!(rejoined, stores);
    }
}
