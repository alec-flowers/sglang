//! Wire codec for SGLang's KV cache event batches.
//!
//! Byte-compatible with the msgspec encoding of `KVEventBatch` in
//! `python/sglang/srt/disaggregation/kv_events.py`:
//!
//! * the batch is a positional msgpack array `[ts, events, attn_dp_rank]`;
//! * each event is a map whose first key `type` carries the class name and
//!   whose other keys are field names in declaration order;
//! * a field with a `None` default is omitted when it is `None`.
//!
//! `testdata/wire_golden.json` pins the bytes against msgspec.

use std::fmt;
use std::sync::Arc;

/// `[ts, events, attn_dp_rank]`.
#[derive(Debug, Clone, PartialEq)]
pub struct KvEventBatch {
    /// Publisher wall clock, seconds since the epoch.
    pub ts: f64,
    pub events: Vec<KvCacheEvent>,
    pub attn_dp_rank: Option<u32>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum KvCacheEvent {
    BlockStored(BlockStored),
    BlockRemoved(BlockRemoved),
    AllBlocksCleared,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BlockStored {
    pub block_hashes: Vec<i64>,
    /// Parent of `block_hashes[0]`; block `i`'s parent is block `i - 1`.
    pub parent_block_hash: Option<i64>,
    /// `block_size` tokens per block, concatenated over `block_hashes`.
    pub token_ids: TokenIds,
    pub block_size: u32,
    pub lora_id: Option<i64>,
    pub medium: Option<StorageMedium>,
    pub cache_salt: Option<Arc<str>>,
    pub session_id: Option<Arc<str>>,
    pub lora_name: Option<Arc<str>>,
    /// One entry per block, aligned with `block_hashes`.
    pub mm_routing_spans: Option<Vec<Vec<MmRoutingSpan>>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BlockRemoved {
    pub block_hashes: Vec<i64>,
    pub medium: Option<StorageMedium>,
}

/// Token ids are i64: multimodal pad values exceed `u32::MAX`.
#[derive(Debug, Clone, PartialEq)]
pub enum TokenIds {
    Unigram(Vec<i64>),
    /// Bigram (EAGLE) keys: one `[t_i, t_{i+1}]` pair per position.
    Bigram(Vec<(i64, i64)>),
}

impl TokenIds {
    pub fn len(&self) -> usize {
        match self {
            TokenIds::Unigram(tokens) => tokens.len(),
            TokenIds::Bigram(pairs) => pairs.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A run of multimodal tokens inside one block that share a routing label.
#[derive(Debug, Clone, PartialEq)]
pub struct MmRoutingSpan {
    pub start: u32,
    pub length: u32,
    pub label: Arc<str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StorageMedium {
    Gpu,
    CpuPinned,
    Disk,
    External,
}

impl StorageMedium {
    /// The value of Python's `StorageMedium` enum member.
    pub fn as_str(self) -> &'static str {
        match self {
            StorageMedium::Gpu => "GPU",
            StorageMedium::CpuPinned => "CPU_PINNED",
            StorageMedium::Disk => "DISK",
            StorageMedium::External => "EXTERNAL",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "GPU" => StorageMedium::Gpu,
            "CPU_PINNED" => StorageMedium::CpuPinned,
            "DISK" => StorageMedium::Disk,
            "EXTERNAL" => StorageMedium::External,
            _ => return None,
        })
    }
}

// ===== Encode =====

pub fn encode_event_batch(batch: &KvEventBatch) -> Vec<u8> {
    let mut out = Vec::new();
    encode_event_batch_into(batch, &mut out);
    out
}

pub fn encode_event_batch_into(batch: &KvEventBatch, out: &mut Vec<u8>) {
    write_array_len(out, 3);
    write_f64(out, batch.ts);
    write_array_len(out, batch.events.len());
    for event in &batch.events {
        encode_event(event, out);
    }
    write_optional_int(out, batch.attn_dp_rank.map(i64::from));
}

fn encode_event(event: &KvCacheEvent, out: &mut Vec<u8>) {
    match event {
        KvCacheEvent::BlockStored(stored) => encode_block_stored(stored, out),
        KvCacheEvent::BlockRemoved(removed) => {
            write_map_len(out, 2 + usize::from(removed.medium.is_some()));
            write_tag(out, "BlockRemoved");
            write_str(out, "block_hashes");
            write_int_array(out, &removed.block_hashes);
            write_medium(out, removed.medium);
        }
        KvCacheEvent::AllBlocksCleared => {
            write_map_len(out, 1);
            write_tag(out, "AllBlocksCleared");
        }
    }
}

fn encode_block_stored(stored: &BlockStored, out: &mut Vec<u8>) {
    let omittable = [
        stored.medium.is_some(),
        stored.cache_salt.is_some(),
        stored.session_id.is_some(),
        stored.lora_name.is_some(),
        stored.mm_routing_spans.is_some(),
    ];
    // `type` plus the five fields that have no default and are always sent.
    write_map_len(out, 6 + omittable.iter().filter(|set| **set).count());
    write_tag(out, "BlockStored");
    write_str(out, "block_hashes");
    write_int_array(out, &stored.block_hashes);
    write_str(out, "parent_block_hash");
    write_optional_int(out, stored.parent_block_hash);
    write_str(out, "token_ids");
    match &stored.token_ids {
        TokenIds::Unigram(tokens) => write_int_array(out, tokens),
        TokenIds::Bigram(pairs) => {
            write_array_len(out, pairs.len());
            for (first, second) in pairs {
                write_array_len(out, 2);
                write_int(out, *first);
                write_int(out, *second);
            }
        }
    }
    write_str(out, "block_size");
    write_int(out, i64::from(stored.block_size));
    write_str(out, "lora_id");
    write_optional_int(out, stored.lora_id);
    write_medium(out, stored.medium);
    write_optional_str_field(out, "cache_salt", stored.cache_salt.as_deref());
    write_optional_str_field(out, "session_id", stored.session_id.as_deref());
    write_optional_str_field(out, "lora_name", stored.lora_name.as_deref());
    if let Some(blocks) = &stored.mm_routing_spans {
        write_str(out, "mm_routing_spans");
        write_array_len(out, blocks.len());
        for spans in blocks {
            write_array_len(out, spans.len());
            for span in spans {
                write_array_len(out, 3);
                write_int(out, i64::from(span.start));
                write_int(out, i64::from(span.length));
                write_str(out, &span.label);
            }
        }
    }
}

fn write_tag(out: &mut Vec<u8>, class_name: &str) {
    write_str(out, "type");
    write_str(out, class_name);
}

fn write_medium(out: &mut Vec<u8>, medium: Option<StorageMedium>) {
    write_optional_str_field(out, "medium", medium.map(StorageMedium::as_str));
}

fn write_optional_str_field(out: &mut Vec<u8>, key: &str, value: Option<&str>) {
    if let Some(value) = value {
        write_str(out, key);
        write_str(out, value);
    }
}

fn write_optional_int(out: &mut Vec<u8>, value: Option<i64>) {
    match value {
        Some(value) => write_int(out, value),
        None => out.push(0xc0),
    }
}

fn write_int_array(out: &mut Vec<u8>, values: &[i64]) {
    write_array_len(out, values.len());
    for value in values {
        write_int(out, *value);
    }
}

/// msgspec picks the shortest form, and an unsigned one for values >= 0.
fn write_int(out: &mut Vec<u8>, value: i64) {
    if value >= 0 {
        let value = value as u64;
        if value < 0x80 {
            out.push(value as u8);
        } else if value <= u64::from(u8::MAX) {
            out.push(0xcc);
            out.push(value as u8);
        } else if value <= u64::from(u16::MAX) {
            out.push(0xcd);
            out.extend_from_slice(&(value as u16).to_be_bytes());
        } else if value <= u64::from(u32::MAX) {
            out.push(0xce);
            out.extend_from_slice(&(value as u32).to_be_bytes());
        } else {
            out.push(0xcf);
            out.extend_from_slice(&value.to_be_bytes());
        }
    } else if value >= -32 {
        out.push(value as i8 as u8);
    } else if value >= i64::from(i8::MIN) {
        out.push(0xd0);
        out.push(value as i8 as u8);
    } else if value >= i64::from(i16::MIN) {
        out.push(0xd1);
        out.extend_from_slice(&(value as i16).to_be_bytes());
    } else if value >= i64::from(i32::MIN) {
        out.push(0xd2);
        out.extend_from_slice(&(value as i32).to_be_bytes());
    } else {
        out.push(0xd3);
        out.extend_from_slice(&value.to_be_bytes());
    }
}

fn write_f64(out: &mut Vec<u8>, value: f64) {
    out.push(0xcb);
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_str(out: &mut Vec<u8>, value: &str) {
    let len = value.len();
    if len < 32 {
        out.push(0xa0 | len as u8);
    } else if len <= usize::from(u8::MAX) {
        out.push(0xd9);
        out.push(len as u8);
    } else if len <= usize::from(u16::MAX) {
        out.push(0xda);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        out.push(0xdb);
        out.extend_from_slice(&wire_len(len).to_be_bytes());
    }
    out.extend_from_slice(value.as_bytes());
}

fn write_array_len(out: &mut Vec<u8>, len: usize) {
    if len < 16 {
        out.push(0x90 | len as u8);
    } else if len <= usize::from(u16::MAX) {
        out.push(0xdc);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        out.push(0xdd);
        out.extend_from_slice(&wire_len(len).to_be_bytes());
    }
}

fn write_map_len(out: &mut Vec<u8>, len: usize) {
    // Events have at most 11 keys, so only the fixmap form occurs.
    assert!(len < 16, "KV event map with {len} keys");
    out.push(0x80 | len as u8);
}

fn wire_len(len: usize) -> u32 {
    u32::try_from(len).expect("msgpack lengths are 32-bit")
}

// ===== Decode =====

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    Truncated,
    /// A value of another msgpack type was found where `expected` belongs.
    Unexpected {
        expected: &'static str,
        marker: u8,
    },
    IntOutOfRange,
    InvalidUtf8,
    MissingField(&'static str),
    UnknownEventType(String),
    UnknownMedium(String),
    /// A skipped value nests deeper than `MAX_SKIP_DEPTH`.
    TooDeep,
    TrailingBytes,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecodeError::Truncated => f.write_str("KV event batch is truncated"),
            DecodeError::Unexpected { expected, marker } => {
                write!(f, "expected {expected}, found msgpack marker {marker:#04x}")
            }
            DecodeError::IntOutOfRange => f.write_str("integer out of range"),
            DecodeError::InvalidUtf8 => f.write_str("string is not UTF-8"),
            DecodeError::MissingField(field) => write!(f, "missing field {field}"),
            DecodeError::UnknownEventType(tag) => write!(f, "unknown KV event type {tag:?}"),
            DecodeError::UnknownMedium(medium) => write!(f, "unknown storage medium {medium:?}"),
            DecodeError::TooDeep => f.write_str("skipped value is nested too deeply"),
            DecodeError::TrailingBytes => f.write_str("bytes follow the KV event batch"),
        }
    }
}

impl std::error::Error for DecodeError {}

/// Decode one payload frame. Unknown event keys and extra trailing batch
/// elements are skipped, so a publisher may add optional fields; an unknown
/// event type or storage medium fails the batch.
pub fn decode_event_batch(bytes: &[u8]) -> Result<KvEventBatch, DecodeError> {
    let mut reader = Reader { buf: bytes };
    let fields = reader.array_len()?;
    if fields < 2 {
        return Err(DecodeError::MissingField("events"));
    }
    let ts = reader.float()?;
    let num_events = reader.array_len()?;
    let mut events = reader.vec_for(num_events);
    for _ in 0..num_events {
        events.push(decode_event(&mut reader)?);
    }
    let attn_dp_rank = if fields >= 3 {
        reader.optional(Reader::uint32)?
    } else {
        None
    };
    for _ in 3..fields {
        reader.skip(0)?;
    }
    if !reader.buf.is_empty() {
        return Err(DecodeError::TrailingBytes);
    }
    Ok(KvEventBatch {
        ts,
        events,
        attn_dp_rank,
    })
}

fn decode_event(reader: &mut Reader<'_>) -> Result<KvCacheEvent, DecodeError> {
    let mut tag = None;
    let mut block_hashes = None;
    let mut parent_block_hash = None;
    let mut token_ids = None;
    let mut block_size = None;
    let mut lora_id = None;
    let mut medium = None;
    let mut cache_salt = None;
    let mut session_id = None;
    let mut lora_name = None;
    let mut mm_routing_spans = None;
    for _ in 0..reader.map_len()? {
        match reader.str()? {
            "type" => tag = Some(reader.str()?),
            "block_hashes" => block_hashes = Some(reader.int_array()?),
            "parent_block_hash" => parent_block_hash = reader.optional(Reader::int)?,
            "token_ids" => token_ids = Some(reader.token_ids()?),
            "block_size" => block_size = Some(reader.uint32()?),
            "lora_id" => lora_id = reader.optional(Reader::int)?,
            "medium" => medium = reader.optional(Reader::medium)?,
            "cache_salt" => cache_salt = reader.optional(Reader::arc_str)?,
            "session_id" => session_id = reader.optional(Reader::arc_str)?,
            "lora_name" => lora_name = reader.optional(Reader::arc_str)?,
            "mm_routing_spans" => mm_routing_spans = reader.optional(Reader::mm_routing_spans)?,
            _ => reader.skip(0)?,
        }
    }
    match tag.ok_or(DecodeError::MissingField("type"))? {
        "BlockStored" => Ok(KvCacheEvent::BlockStored(BlockStored {
            block_hashes: block_hashes.ok_or(DecodeError::MissingField("block_hashes"))?,
            parent_block_hash,
            token_ids: token_ids.ok_or(DecodeError::MissingField("token_ids"))?,
            block_size: block_size.ok_or(DecodeError::MissingField("block_size"))?,
            lora_id,
            medium,
            cache_salt,
            session_id,
            lora_name,
            mm_routing_spans,
        })),
        "BlockRemoved" => Ok(KvCacheEvent::BlockRemoved(BlockRemoved {
            block_hashes: block_hashes.ok_or(DecodeError::MissingField("block_hashes"))?,
            medium,
        })),
        "AllBlocksCleared" => Ok(KvCacheEvent::AllBlocksCleared),
        other => Err(DecodeError::UnknownEventType(other.to_string())),
    }
}

// Arbitrary; far deeper than any real batch.
const MAX_SKIP_DEPTH: usize = 32;
// Arbitrary. What a length prefix may reserve before any element is decoded.
const MAX_PREALLOC_BYTES: usize = 1 << 20;

struct Reader<'a> {
    buf: &'a [u8],
}

impl<'a> Reader<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8], DecodeError> {
        if self.buf.len() < len {
            return Err(DecodeError::Truncated);
        }
        let (head, tail) = self.buf.split_at(len);
        self.buf = tail;
        Ok(head)
    }

    fn byte(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }

    fn be<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        Ok(self.take(N)?.try_into().expect("take returned N bytes"))
    }

    /// An empty `Vec` for `len` elements announced on the wire. A corrupt
    /// prefix cannot reserve more than the bytes left (an element takes at
    /// least one) or `MAX_PREALLOC_BYTES`; the vector grows as elements decode.
    fn vec_for<T>(&self, len: usize) -> Vec<T> {
        let per_element = std::mem::size_of::<T>().max(1);
        Vec::with_capacity(
            len.min(self.buf.len())
                .min(MAX_PREALLOC_BYTES / per_element),
        )
    }

    fn optional<T>(
        &mut self,
        read: impl FnOnce(&mut Self) -> Result<T, DecodeError>,
    ) -> Result<Option<T>, DecodeError> {
        if self.buf.first() == Some(&0xc0) {
            self.buf = &self.buf[1..];
            return Ok(None);
        }
        read(self).map(Some)
    }

    fn int(&mut self) -> Result<i64, DecodeError> {
        let marker = self.byte()?;
        Ok(match marker {
            0x00..=0x7f => i64::from(marker),
            0xe0..=0xff => i64::from(marker as i8),
            0xcc => i64::from(self.byte()?),
            0xcd => i64::from(u16::from_be_bytes(self.be()?)),
            0xce => i64::from(u32::from_be_bytes(self.be()?)),
            0xcf => i64::try_from(u64::from_be_bytes(self.be()?))
                .map_err(|_| DecodeError::IntOutOfRange)?,
            0xd0 => i64::from(self.byte()? as i8),
            0xd1 => i64::from(i16::from_be_bytes(self.be()?)),
            0xd2 => i64::from(i32::from_be_bytes(self.be()?)),
            0xd3 => i64::from_be_bytes(self.be()?),
            _ => {
                return Err(DecodeError::Unexpected {
                    expected: "an integer",
                    marker,
                });
            }
        })
    }

    fn uint32(&mut self) -> Result<u32, DecodeError> {
        u32::try_from(self.int()?).map_err(|_| DecodeError::IntOutOfRange)
    }

    fn float(&mut self) -> Result<f64, DecodeError> {
        match self.buf.first() {
            Some(0xcb) => {
                self.buf = &self.buf[1..];
                Ok(f64::from_be_bytes(self.be()?))
            }
            Some(0xca) => {
                self.buf = &self.buf[1..];
                Ok(f64::from(f32::from_be_bytes(self.be()?)))
            }
            _ => Ok(self.int()? as f64),
        }
    }

    fn str(&mut self) -> Result<&'a str, DecodeError> {
        let marker = self.byte()?;
        let len = match marker {
            0xa0..=0xbf => usize::from(marker & 0x1f),
            0xd9 => usize::from(self.byte()?),
            0xda => usize::from(u16::from_be_bytes(self.be()?)),
            0xdb => u32::from_be_bytes(self.be()?) as usize,
            _ => {
                return Err(DecodeError::Unexpected {
                    expected: "a string",
                    marker,
                });
            }
        };
        std::str::from_utf8(self.take(len)?).map_err(|_| DecodeError::InvalidUtf8)
    }

    fn arc_str(&mut self) -> Result<Arc<str>, DecodeError> {
        self.str().map(Arc::from)
    }

    fn medium(&mut self) -> Result<StorageMedium, DecodeError> {
        let value = self.str()?;
        StorageMedium::parse(value).ok_or_else(|| DecodeError::UnknownMedium(value.to_string()))
    }

    fn array_len(&mut self) -> Result<usize, DecodeError> {
        let marker = self.byte()?;
        Ok(match marker {
            0x90..=0x9f => usize::from(marker & 0x0f),
            0xdc => usize::from(u16::from_be_bytes(self.be()?)),
            0xdd => u32::from_be_bytes(self.be()?) as usize,
            _ => {
                return Err(DecodeError::Unexpected {
                    expected: "an array",
                    marker,
                });
            }
        })
    }

    fn map_len(&mut self) -> Result<usize, DecodeError> {
        let marker = self.byte()?;
        Ok(match marker {
            0x80..=0x8f => usize::from(marker & 0x0f),
            0xde => usize::from(u16::from_be_bytes(self.be()?)),
            0xdf => u32::from_be_bytes(self.be()?) as usize,
            _ => {
                return Err(DecodeError::Unexpected {
                    expected: "a map",
                    marker,
                });
            }
        })
    }

    fn int_array(&mut self) -> Result<Vec<i64>, DecodeError> {
        let len = self.array_len()?;
        let mut values = self.vec_for(len);
        for _ in 0..len {
            values.push(self.int()?);
        }
        Ok(values)
    }

    fn token_ids(&mut self) -> Result<TokenIds, DecodeError> {
        let len = self.array_len()?;
        let is_pair = matches!(self.buf.first(), Some(0x90..=0x9f | 0xdc | 0xdd));
        if len == 0 || !is_pair {
            let mut tokens = self.vec_for(len);
            for _ in 0..len {
                tokens.push(self.int()?);
            }
            return Ok(TokenIds::Unigram(tokens));
        }
        let mut pairs = self.vec_for(len);
        for _ in 0..len {
            let marker = *self.buf.first().ok_or(DecodeError::Truncated)?;
            if self.array_len()? != 2 {
                return Err(DecodeError::Unexpected {
                    expected: "a bigram token pair",
                    marker,
                });
            }
            pairs.push((self.int()?, self.int()?));
        }
        Ok(TokenIds::Bigram(pairs))
    }

    fn mm_routing_spans(&mut self) -> Result<Vec<Vec<MmRoutingSpan>>, DecodeError> {
        let num_blocks = self.array_len()?;
        let mut blocks = self.vec_for(num_blocks);
        for _ in 0..num_blocks {
            let num_spans = self.array_len()?;
            let mut spans = self.vec_for(num_spans);
            for _ in 0..num_spans {
                let marker = *self.buf.first().ok_or(DecodeError::Truncated)?;
                if self.array_len()? != 3 {
                    return Err(DecodeError::Unexpected {
                        expected: "a [start, length, label] span",
                        marker,
                    });
                }
                spans.push(MmRoutingSpan {
                    start: self.uint32()?,
                    length: self.uint32()?,
                    label: self.arc_str()?,
                });
            }
            blocks.push(spans);
        }
        Ok(blocks)
    }

    /// Skip one value of any type.
    fn skip(&mut self, depth: usize) -> Result<(), DecodeError> {
        if depth > MAX_SKIP_DEPTH {
            return Err(DecodeError::TooDeep);
        }
        let marker = self.byte()?;
        let (payload, children) = match marker {
            0x00..=0x7f | 0xe0..=0xff | 0xc0 | 0xc2 | 0xc3 => (0, 0),
            0x80..=0x8f => (0, 2 * usize::from(marker & 0x0f)),
            0x90..=0x9f => (0, usize::from(marker & 0x0f)),
            0xa0..=0xbf => (usize::from(marker & 0x1f), 0),
            0xc4 | 0xd9 => (usize::from(self.byte()?), 0),
            0xc5 | 0xda => (usize::from(u16::from_be_bytes(self.be()?)), 0),
            0xc6 | 0xdb => (u32::from_be_bytes(self.be()?) as usize, 0),
            // ext 8/16/32: a length, then a type byte, then the data.
            0xc7 => (usize::from(self.byte()?) + 1, 0),
            0xc8 => (usize::from(u16::from_be_bytes(self.be()?)) + 1, 0),
            0xc9 => (u32::from_be_bytes(self.be()?) as usize + 1, 0),
            0xca | 0xce | 0xd2 => (4, 0),
            0xcb | 0xcf | 0xd3 => (8, 0),
            0xcc | 0xd0 => (1, 0),
            0xcd | 0xd1 => (2, 0),
            // fixext 1/2/4/8/16: a type byte, then the data.
            0xd4 => (2, 0),
            0xd5 => (3, 0),
            0xd6 => (5, 0),
            0xd7 => (9, 0),
            0xd8 => (17, 0),
            0xdc => (0, usize::from(u16::from_be_bytes(self.be()?))),
            0xdd => (0, u32::from_be_bytes(self.be()?) as usize),
            0xde => (0, 2 * usize::from(u16::from_be_bytes(self.be()?))),
            0xdf => (0, 2 * u32::from_be_bytes(self.be()?) as usize),
            0xc1 => {
                return Err(DecodeError::Unexpected {
                    expected: "a msgpack value",
                    marker,
                });
            }
        };
        self.take(payload)?;
        for _ in 0..children {
            self.skip(depth + 1)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stored(tokens: TokenIds) -> KvEventBatch {
        KvEventBatch {
            ts: 1.5,
            events: vec![KvCacheEvent::BlockStored(BlockStored {
                block_hashes: vec![7],
                parent_block_hash: None,
                token_ids: tokens,
                block_size: 2,
                lora_id: None,
                medium: Some(StorageMedium::Gpu),
                cache_salt: None,
                session_id: None,
                lora_name: None,
                mm_routing_spans: None,
            })],
            attn_dp_rank: Some(0),
        }
    }

    #[test]
    fn unknown_keys_and_trailing_batch_elements_are_skipped() {
        // [1.5, [{"type": "BlockRemoved", "future": {"a": [1, nil]},
        //   "block_hashes": [7]}], 0, <bin 2 bytes>]
        let mut bytes = vec![0x94, 0xcb];
        bytes.extend_from_slice(&1.5f64.to_be_bytes());
        bytes.extend_from_slice(&[0x91, 0x83]);
        write_tag(&mut bytes, "BlockRemoved");
        write_str(&mut bytes, "future");
        bytes.extend_from_slice(&[0x81, 0xa1, b'a', 0x92, 0x01, 0xc0]);
        write_str(&mut bytes, "block_hashes");
        bytes.extend_from_slice(&[0x91, 0x07]);
        bytes.extend_from_slice(&[0x00, 0xc4, 0x02, 0xaa, 0xbb]);

        let batch = decode_event_batch(&bytes).unwrap();
        assert_eq!(
            batch.events,
            vec![KvCacheEvent::BlockRemoved(BlockRemoved {
                block_hashes: vec![7],
                medium: None,
            })]
        );
        assert_eq!(batch.attn_dp_rank, Some(0));
    }

    #[test]
    fn every_truncation_is_an_error() {
        for batch in [
            stored(TokenIds::Unigram(vec![1, 1 << 40])),
            stored(TokenIds::Bigram(vec![(1, 2), (2, 3)])),
        ] {
            let bytes = encode_event_batch(&batch);
            for len in 0..bytes.len() {
                assert!(decode_event_batch(&bytes[..len]).is_err(), "prefix {len}");
            }
        }
    }

    #[test]
    fn corrupt_length_prefix_reserves_a_bounded_amount() {
        // An array32 header announcing 4G events, with a megabyte of junk
        // after it: an element-count bound alone would reserve ~170 MB here.
        let mut bytes = vec![0x93, 0xcb];
        bytes.extend_from_slice(&0f64.to_be_bytes());
        bytes.extend_from_slice(&[0xdd, 0xff, 0xff, 0xff, 0xff]);
        bytes.resize(bytes.len() + (1 << 20), 0xc1);
        let reader = Reader { buf: &bytes };
        let events: Vec<KvCacheEvent> = reader.vec_for(u32::MAX as usize);
        assert!(events.capacity() * std::mem::size_of::<KvCacheEvent>() <= MAX_PREALLOC_BYTES);
        assert!(decode_event_batch(&bytes).is_err());
    }
}
