//! One rank's relay: reads the publisher's stream, keeps a history of it and
//! fans it out to consumers.
//!
//! A single task owns all of the state, so classifying a cursor, cutting its
//! backlog and registering its live queue happen in one step: a consumer sees
//! every batch exactly once, in order.

use std::sync::{Arc, Mutex, OnceLock};
use std::thread::JoinHandle;

use bytes::Bytes;
use sglang_kv_v1_types as pb;
use tokio::sync::{mpsc, oneshot};

use crate::convert;
use crate::history::History;
use crate::subscriber::{StreamEvent, SubscriberConfig, run_subscriber};
use crate::wire;

#[derive(Debug, Clone)]
pub struct RelayConfig {
    pub source: SubscriberConfig,
    pub dp_rank: u32,
    pub block_size: u32,
    pub hash_format: pb::KvHashFormat,
    pub history_max_batches: usize,
    pub history_max_bytes: usize,
    /// Live batches queued for one consumer before it is dropped as too slow.
    pub consumer_queue: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum RelayError {
    #[error("the KV event relay is not running")]
    Unavailable,
    #[error("cursor {next_seq} is outside the retained batches [{oldest_seq}, {window_end})")]
    OutOfRange {
        next_seq: u64,
        oldest_seq: u64,
        window_end: u64,
    },
    #[error("the cursor belongs to publisher {cursor:?}; the stream is now {current:?}")]
    WrongPublisher { cursor: String, current: String },
    #[error("this server does not serve KV snapshots")]
    NoSnapshot,
    #[error("on_miss FAIL needs a cursor to resume from")]
    NoCursor,
}

/// Why a consumer's live feed ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndReason {
    /// History was lost; the stream continues under a new publisher id.
    DataLoss,
    /// The consumer's queue overflowed.
    Lagged,
    /// The relay stopped.
    Unavailable,
}

pub type Batch = Arc<pb::KvEventBatch>;

/// A consumer's place in the stream, cut atomically with the live queue.
pub struct Admission {
    pub mode: pb::KvStreamMode,
    pub info: pb::KvStreamInfo,
    /// The first batch `backlog`, then `live`, delivers.
    pub next_seq: u64,
    pub backlog: Vec<Batch>,
    pub live: LiveFeed,
}

pub struct LiveFeed {
    batches: mpsc::Receiver<Batch>,
    end: Arc<OnceLock<EndReason>>,
}

impl LiveFeed {
    pub async fn recv(&mut self) -> Result<Batch, EndReason> {
        match self.batches.recv().await {
            Some(batch) => Ok(batch),
            None => Err(*self.end.get().unwrap_or(&EndReason::Unavailable)),
        }
    }
}

enum Command {
    Info(oneshot::Sender<pb::KvStreamInfo>),
    Subscribe {
        resume: Option<pb::KvCursor>,
        on_miss: pb::KvCursorMiss,
        reply: oneshot::Sender<Result<Admission, RelayError>>,
    },
    Events {
        from: pb::KvCursor,
        end_seq: Option<u64>,
        reply: oneshot::Sender<Result<(pb::KvStreamInfo, Vec<Batch>), RelayError>>,
    },
}

/// The relay thread. Dropping it stops the relay.
pub struct Relay {
    handle: RelayHandle,
    stop: Mutex<Option<(oneshot::Sender<()>, JoinHandle<()>)>>,
}

#[derive(Clone)]
pub struct RelayHandle {
    commands: mpsc::Sender<Command>,
}

impl Relay {
    pub fn start(config: RelayConfig) -> std::io::Result<Self> {
        let (commands, command_rx) = mpsc::channel(64);
        let (stop_tx, stop_rx) = oneshot::channel();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let thread = std::thread::Builder::new()
            .name("kv-relay".to_string())
            .spawn(move || {
                runtime.block_on(async {
                    tokio::select! {
                        () = run(config, command_rx) => {}
                        _ = stop_rx => {}
                    }
                });
            })?;
        Ok(Self {
            handle: RelayHandle { commands },
            stop: Mutex::new(Some((stop_tx, thread))),
        })
    }

    pub fn handle(&self) -> RelayHandle {
        self.handle.clone()
    }

    /// Stop the relay; every live feed ends with `EndReason::Unavailable`.
    pub fn shutdown(&self) {
        let stop = self.stop.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some((stop, thread)) = stop {
            let _ = stop.send(());
            let _ = thread.join();
        }
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl RelayHandle {
    pub async fn info(&self) -> Result<pb::KvStreamInfo, RelayError> {
        let (reply, response) = oneshot::channel();
        self.request(Command::Info(reply), response).await
    }

    pub async fn subscribe(
        &self,
        resume: Option<pb::KvCursor>,
        on_miss: pb::KvCursorMiss,
    ) -> Result<Admission, RelayError> {
        let (reply, response) = oneshot::channel();
        let command = Command::Subscribe {
            resume,
            on_miss,
            reply,
        };
        self.request(command, response).await?
    }

    /// The retained batches `[from.next_seq, end_seq)`.
    pub async fn events(
        &self,
        from: pb::KvCursor,
        end_seq: Option<u64>,
    ) -> Result<(pb::KvStreamInfo, Vec<Batch>), RelayError> {
        let (reply, response) = oneshot::channel();
        let command = Command::Events {
            from,
            end_seq,
            reply,
        };
        self.request(command, response).await?
    }

    async fn request<T>(
        &self,
        command: Command,
        response: oneshot::Receiver<T>,
    ) -> Result<T, RelayError> {
        self.commands
            .send(command)
            .await
            .map_err(|_| RelayError::Unavailable)?;
        response.await.map_err(|_| RelayError::Unavailable)
    }
}

struct Consumer {
    batches: mpsc::Sender<Batch>,
    end: Arc<OnceLock<EndReason>>,
}

struct State {
    config: RelayConfig,
    publisher_id: String,
    /// False once any batch of this publisher id was not seen.
    complete: bool,
    history: History<Batch>,
    consumers: Vec<Consumer>,
}

async fn run(config: RelayConfig, mut commands: mpsc::Receiver<Command>) {
    let (event_tx, mut events) = mpsc::channel(1024);
    let source = config.source.clone();
    let reader = tokio::spawn(async move {
        if let Err(error) = run_subscriber(source, event_tx).await {
            tracing::error!(%error, "KV event relay lost its publisher");
        }
    });
    let mut state = State {
        history: History::new(config.history_max_batches, config.history_max_bytes),
        config,
        publisher_id: new_publisher_id(),
        complete: true,
        consumers: Vec::new(),
    };
    loop {
        tokio::select! {
            event = events.recv() => match event {
                Some(StreamEvent::Batch { seq, payload }) => state.on_batch(seq, payload),
                Some(StreamEvent::Lost { resumed_at, .. }) => state.start_new_epoch(resumed_at),
                // The reader failed: the consumers would wait forever.
                None => break,
            },
            command = commands.recv() => match command {
                Some(command) => state.handle(command),
                None => break,
            },
        }
    }
    reader.abort();
}

fn new_publisher_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

impl State {
    fn on_batch(&mut self, seq: u64, payload: Bytes) {
        let batch = match wire::decode_event_batch(&payload) {
            Ok(batch) => batch,
            Err(error) => {
                // Skipping it would silently diverge from the engine.
                tracing::error!(%error, seq, "undecodable KV event batch");
                self.start_new_epoch(seq + 1);
                return;
            }
        };
        let batch = Arc::new(convert::batch(seq, batch));
        self.history.push(batch.clone(), payload.len());
        self.consumers.retain(|consumer| {
            use mpsc::error::TrySendError;
            match consumer.batches.try_send(batch.clone()) {
                Ok(()) => true,
                Err(TrySendError::Full(_)) => {
                    let _ = consumer.end.set(EndReason::Lagged);
                    false
                }
                Err(TrySendError::Closed(_)) => false,
            }
        });
    }

    /// Batches before `next_seq` are gone: nobody can resume across the hole.
    fn start_new_epoch(&mut self, next_seq: u64) {
        for consumer in self.consumers.drain(..) {
            let _ = consumer.end.set(EndReason::DataLoss);
        }
        self.publisher_id = new_publisher_id();
        self.complete = false;
        self.history.reset(next_seq);
    }

    fn info(&self) -> pb::KvStreamInfo {
        pb::KvStreamInfo {
            publisher_id: self.publisher_id.clone(),
            dp_rank: self.config.dp_rank,
            block_size: self.config.block_size,
            hash_format: self.config.hash_format as i32,
            oldest_seq: self.history.oldest_seq(),
            next_seq: self.history.next_seq(),
            blocks: 0,
            complete: self.complete,
            reseed_supported: false,
        }
    }

    fn handle(&mut self, command: Command) {
        match command {
            Command::Info(reply) => {
                let _ = reply.send(self.info());
            }
            Command::Subscribe {
                resume,
                on_miss,
                reply,
            } => {
                let _ = reply.send(self.subscribe(resume, on_miss));
            }
            Command::Events {
                from,
                end_seq,
                reply,
            } => {
                let batches = self.check_cursor(&from).map(|()| {
                    let end = end_seq.unwrap_or(u64::MAX);
                    let batches = self.history.range(from.next_seq, end);
                    (self.info(), batches.cloned().collect())
                });
                let _ = reply.send(batches);
            }
        }
    }

    fn check_cursor(&self, cursor: &pb::KvCursor) -> Result<(), RelayError> {
        if cursor.publisher_id != self.publisher_id {
            return Err(RelayError::WrongPublisher {
                cursor: cursor.publisher_id.clone(),
                current: self.publisher_id.clone(),
            });
        }
        if !self.history.covers(cursor.next_seq) {
            return Err(RelayError::OutOfRange {
                next_seq: cursor.next_seq,
                oldest_seq: self.history.oldest_seq(),
                window_end: self.history.next_seq(),
            });
        }
        Ok(())
    }

    fn subscribe(
        &mut self,
        resume: Option<pb::KvCursor>,
        on_miss: pb::KvCursorMiss,
    ) -> Result<Admission, RelayError> {
        let resumable = resume.as_ref().map(|cursor| self.check_cursor(cursor));
        let (mode, next_seq) = match (resumable, on_miss) {
            (Some(Ok(())), _) => (
                pb::KvStreamMode::Events,
                resume.map_or(0, |cursor| cursor.next_seq),
            ),
            (Some(Err(error)), pb::KvCursorMiss::Fail) => return Err(error),
            (None, pb::KvCursorMiss::Fail) => return Err(RelayError::NoCursor),
            (_, pb::KvCursorMiss::LiveOnly) => {
                (pb::KvStreamMode::LiveOnly, self.history.next_seq())
            }
            _ => return Err(RelayError::NoSnapshot),
        };
        let backlog = self.history.range(next_seq, u64::MAX).cloned().collect();
        let (batches, live) = mpsc::channel(self.config.consumer_queue.max(1));
        let end = Arc::new(OnceLock::new());
        self.consumers.push(Consumer {
            batches,
            end: end.clone(),
        });
        Ok(Admission {
            mode,
            info: self.info(),
            next_seq,
            backlog,
            live: LiveFeed { batches: live, end },
        })
    }
}
