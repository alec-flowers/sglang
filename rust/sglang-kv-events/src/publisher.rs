//! PUB + ROUTER publisher for encoded KV event batches: the Rust counterpart
//! of `ZmqEventPublisher` in `python/sglang/srt/disaggregation/kv_events.py`.
//!
//! Live batches go out as `[topic, seq (u64 BE), payload]`. A replay request
//! `[identity, "", start_seq (u64 BE)]` is answered with
//! `[identity, "", topic, seq, payload]` for every retained batch from
//! `start_seq` on, then `[identity, "", "", END_SEQ, ""]`.

use std::collections::VecDeque;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{Semaphore, mpsc, oneshot};
use zeromq::{PubSocket, RouterSocket, Socket, SocketRecv, SocketSend, ZmqMessage};

/// The sequence frame of the message that ends a replay: -1 as i64 BE.
pub const END_SEQ: [u8; 8] = [0xff; 8];

// Same grace period as ZmqEventPublisher.SHUTDOWN_TIMEOUT.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);
// Arbitrary. How long a subscriber or a replay requester may hold up a send
// before it is dropped; zeromq has no per-peer high-water mark to do that.
const SEND_TIMEOUT: Duration = Duration::from_secs(1);
// Arbitrary. A replay request that finds this many waiting is dropped, and
// the requester asks again.
const REPLAY_BACKLOG: usize = 64;

#[derive(Debug, Clone)]
pub struct PublisherConfig {
    /// `tcp://host:port` or `ipc://path`; always bound, never connected.
    pub endpoint: String,
    pub replay_endpoint: Option<String>,
    pub topic: String,
    /// Batches retained for replay.
    pub buffer_steps: usize,
    /// Batches queued between `publish` and the publisher thread; 0 for no limit.
    pub max_queue_size: usize,
    /// Batches queued for the PUB socket, shared by all subscribers; 0 for no limit.
    pub hwm: usize,
}

/// 0 means no limit, as for `queue.Queue(maxsize=0)` and ZMQ_SNDHWM.
fn queue_capacity(limit: usize) -> usize {
    match limit {
        0 => Semaphore::MAX_PERMITS,
        limit => limit.min(Semaphore::MAX_PERMITS),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PublisherError {
    #[error("unsupported KV event endpoint {0:?}: expected tcp://host:port or ipc://path")]
    UnsupportedEndpoint(String),
    #[error("cannot bind KV event socket {endpoint}: {reason}")]
    Bind { endpoint: String, reason: String },
    #[error("cannot start the KV event publisher thread: {0}")]
    Thread(#[from] std::io::Error),
    #[error("publisher is closed")]
    Closed,
}

type SequencedBatch = (u64, Bytes);

/// Sequence assignment and the hand-off queue. A batch gets its sequence
/// number before the queue is tried, so a dropped batch is a visible gap.
struct Ingress {
    next_seq: u64,
    tx: Option<mpsc::Sender<SequencedBatch>>,
}

impl Ingress {
    fn publish(&mut self, payload: Bytes, dropped: &AtomicU64) -> Result<u64, PublisherError> {
        let tx = self.tx.as_ref().ok_or(PublisherError::Closed)?;
        let seq = self.next_seq;
        self.next_seq += 1;
        match tx.try_send((seq, payload)) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                dropped.fetch_add(1, Ordering::Relaxed);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => return Err(PublisherError::Closed),
        }
        Ok(seq)
    }
}

#[derive(Default)]
struct Counters {
    /// Batches that found the hand-off queue full: sequence gaps for everyone.
    dropped: AtomicU64,
    /// Batches that found the PUB queue full: they are only in the replay ring.
    live_dropped: AtomicU64,
}

pub struct Publisher {
    ingress: Mutex<Ingress>,
    counters: Arc<Counters>,
    worker: Mutex<Option<Worker>>,
    local_pub_endpoint: String,
    local_replay_endpoint: Option<String>,
}

struct Worker {
    thread: JoinHandle<()>,
    /// Fires when the thread has drained its queue and closed its sockets.
    done: std::sync::mpsc::Receiver<()>,
    abort: oneshot::Sender<()>,
}

struct Bound {
    pub_endpoint: String,
    replay_endpoint: Option<String>,
}

impl Publisher {
    /// Bind the sockets and start the publisher thread.
    pub fn start(config: PublisherConfig) -> Result<Self, PublisherError> {
        let (tx, rx) = mpsc::channel(queue_capacity(config.max_queue_size));
        let counters = Arc::new(Counters::default());
        let worker_counters = counters.clone();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let (abort_tx, abort_rx) = oneshot::channel();
        let thread = std::thread::Builder::new()
            .name("kv-events-pub".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build();
                match runtime {
                    Ok(runtime) => runtime.block_on(async {
                        tokio::select! {
                            () = run(config, rx, ready_tx, worker_counters) => {}
                            _ = abort_rx => {}
                        }
                    }),
                    Err(error) => {
                        let _ = ready_tx.send(Err(PublisherError::Thread(error)));
                    }
                }
                let _ = done_tx.send(());
            })?;
        let bound = match ready_rx.recv() {
            Ok(Ok(bound)) => bound,
            Ok(Err(error)) => {
                let _ = thread.join();
                return Err(error);
            }
            Err(_) => {
                let _ = thread.join();
                return Err(PublisherError::Closed);
            }
        };
        Ok(Self {
            ingress: Mutex::new(Ingress {
                next_seq: 0,
                tx: Some(tx),
            }),
            counters,
            worker: Mutex::new(Some(Worker {
                thread,
                done: done_rx,
                abort: abort_tx,
            })),
            local_pub_endpoint: bound.pub_endpoint,
            local_replay_endpoint: bound.replay_endpoint,
        })
    }

    /// Queue one encoded batch and return its sequence number. Never blocks:
    /// a batch that finds the queue full is dropped and counted.
    pub fn publish(&self, payload: Bytes) -> Result<u64, PublisherError> {
        let mut ingress = self.ingress.lock().unwrap_or_else(|e| e.into_inner());
        ingress.publish(payload, &self.counters.dropped)
    }

    /// Batches dropped because the hand-off queue was full.
    pub fn dropped_batches(&self) -> u64 {
        self.counters.dropped.load(Ordering::Relaxed)
    }

    /// Batches that were not sent live because the PUB queue was full. They
    /// are in the replay ring, where a subscriber that sees the gap finds them.
    pub fn live_dropped_batches(&self) -> u64 {
        self.counters.live_dropped.load(Ordering::Relaxed)
    }

    /// The bound PUB endpoint, with the real port for a `:0` bind.
    pub fn local_pub_endpoint(&self) -> &str {
        &self.local_pub_endpoint
    }

    pub fn local_replay_endpoint(&self) -> Option<&str> {
        self.local_replay_endpoint.as_deref()
    }

    /// Publish what is queued, then close the sockets. Idempotent.
    pub fn shutdown(&self) {
        self.ingress
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .tx
            .take();
        let worker = self.worker.lock().unwrap_or_else(|e| e.into_inner()).take();
        let Some(worker) = worker else { return };
        if worker.done.recv_timeout(SHUTDOWN_TIMEOUT).is_err() {
            let _ = worker.abort.send(());
            // A thread that never reaches an await point cannot be stopped;
            // leave it behind rather than hang the caller.
            if worker.done.recv_timeout(SHUTDOWN_TIMEOUT).is_err() {
                return;
            }
        }
        let _ = worker.thread.join();
    }
}

impl Drop for Publisher {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// The retained batches, oldest first, with strictly increasing sequence numbers.
struct Ring {
    entries: VecDeque<SequencedBatch>,
    capacity: usize,
}

impl Ring {
    fn push(&mut self, seq: u64, payload: Bytes) {
        if self.capacity == 0 {
            return;
        }
        if self.entries.len() == self.capacity {
            self.entries.pop_front();
        }
        self.entries.push_back((seq, payload));
    }

    fn since(&self, start_seq: u64) -> impl Iterator<Item = &SequencedBatch> {
        let first = self.entries.partition_point(|(seq, _)| *seq < start_seq);
        self.entries.range(first..)
    }
}

struct ReplayRequest {
    identity: Bytes,
    start_seq: u64,
}

/// The batches one requester is owed, cut from the ring when it asked.
struct Replay {
    identity: Bytes,
    batches: Vec<SequencedBatch>,
}

/// Removes the bound ipc socket files however `run` ends. zeromq only does so
/// from its accept task, which an early return or an abort never polls again.
#[derive(Default)]
struct IpcFiles(Vec<String>);

impl Drop for IpcFiles {
    fn drop(&mut self) {
        for path in &self.0 {
            let _ = std::fs::remove_file(path);
        }
    }
}

async fn run(
    config: PublisherConfig,
    mut batches: mpsc::Receiver<SequencedBatch>,
    ready: std::sync::mpsc::Sender<Result<Bound, PublisherError>>,
    counters: Arc<Counters>,
) {
    let mut ipc_files = IpcFiles::default();
    let mut pub_socket = PubSocket::new();
    let pub_endpoint = match bind(&mut pub_socket, &config.endpoint, &mut ipc_files).await {
        Ok(endpoint) => endpoint,
        Err(error) => {
            let _ = ready.send(Err(error));
            return;
        }
    };
    let mut router = None;
    let mut replay_endpoint = None;
    if let Some(endpoint) = &config.replay_endpoint {
        let mut socket = RouterSocket::new();
        match bind(&mut socket, endpoint, &mut ipc_files).await {
            Ok(endpoint) => replay_endpoint = Some(endpoint),
            Err(error) => {
                let _ = ready.send(Err(error));
                return;
            }
        }
        router = Some(socket);
    }
    let _ = ready.send(Ok(Bound {
        pub_endpoint,
        replay_endpoint,
    }));

    let topic = Bytes::from(config.topic.into_bytes());
    let (live_tx, live_rx) = mpsc::channel(queue_capacity(config.hwm));
    let live = tokio::spawn(send_live(pub_socket, live_rx));
    let (request_tx, mut requests) = mpsc::channel(REPLAY_BACKLOG);
    let (replay_tx, replay_rx) = mpsc::channel(REPLAY_BACKLOG);
    let replay = router
        .map(|router| tokio::spawn(serve_replays(router, request_tx, replay_rx, topic.clone())));

    let mut ring = Ring {
        entries: VecDeque::new(),
        capacity: config.buffer_steps,
    };
    loop {
        // Batches first: a replay then covers every batch published before
        // the request was sent, which the router's gap repair relies on.
        tokio::select! {
            biased;
            batch = batches.recv() => {
                let Some((seq, payload)) = batch else { break };
                ring.push(seq, payload.clone());
                let message = frames([
                    topic.clone(),
                    Bytes::copy_from_slice(&seq.to_be_bytes()),
                    payload,
                ]);
                if live_tx.try_send(message).is_err() {
                    counters.live_dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
            Some(ReplayRequest { identity, start_seq }) = requests.recv() => {
                let batches = ring.since(start_seq).cloned().collect();
                // Past the backlog the request is dropped, as a reply past
                // libzmq's ROUTER high-water mark is.
                let _ = replay_tx.try_send(Replay { identity, batches });
            }
        }
    }

    drop(live_tx);
    if let Ok(mut pub_socket) = live.await {
        pub_socket.unbind_all().await;
    }
    if let Some(replay) = replay {
        replay.abort();
    }
}

fn frames<const N: usize>(frames: [Bytes; N]) -> ZmqMessage {
    ZmqMessage::try_from(Vec::from(frames)).expect("a message with at least one frame")
}

async fn send_live(mut socket: PubSocket, mut live: mpsc::Receiver<ZmqMessage>) -> PubSocket {
    while let Some(message) = live.recv().await {
        match tokio::time::timeout(SEND_TIMEOUT, socket.send(message)).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => tracing::warn!(%error, "KV event publish failed"),
            // zeromq writes to the subscribers in turn and cannot say which
            // one stalled, so drop them all: they reconnect, see a sequence
            // gap and repair it through replay.
            Err(_stalled) => {
                tracing::warn!("a KV event subscriber stopped reading; dropping the subscribers");
                socket.backend().shutdown();
            }
        }
    }
    socket
}

/// One task owns the whole ROUTER socket. Its receive side removes a peer
/// that disconnected, and blocks the thread to do so if a send to that peer is
/// suspended, so the two must never be polled concurrently.
async fn serve_replays(
    mut socket: RouterSocket,
    requests: mpsc::Sender<ReplayRequest>,
    mut replays: mpsc::Receiver<Replay>,
    topic: Bytes,
) {
    loop {
        tokio::select! {
            message = socket.recv() => {
                let Ok(message) = message else {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                };
                let Some(request) = replay_request(message) else {
                    tracing::warn!("invalid KV event replay request");
                    continue;
                };
                if requests.send(request).await.is_err() {
                    return;
                }
            }
            Some(replay) = replays.recv() => send_replay(&mut socket, replay, &topic).await,
        }
    }
}

fn replay_request(message: ZmqMessage) -> Option<ReplayRequest> {
    match message.into_vec().as_slice() {
        [identity, delimiter, start_seq] if delimiter.is_empty() => Some(ReplayRequest {
            identity: identity.clone(),
            start_seq: u64::from_be_bytes(start_seq.as_ref().try_into().ok()?),
        }),
        _ => None,
    }
}

/// A requester that is gone or not reading loses the rest of its replay.
async fn send_replay(socket: &mut RouterSocket, replay: Replay, topic: &Bytes) {
    let reply = |topic: Bytes, seq: [u8; 8], payload: Bytes| {
        let identity = replay.identity.clone();
        let seq = Bytes::copy_from_slice(&seq);
        frames([identity, Bytes::new(), topic, seq, payload])
    };
    let batches = replay.batches.iter();
    let batches =
        batches.map(|(seq, payload)| reply(topic.clone(), seq.to_be_bytes(), payload.clone()));
    let end = reply(Bytes::new(), END_SEQ, Bytes::new());
    for message in batches.chain(std::iter::once(end)) {
        let sent = tokio::time::timeout(SEND_TIMEOUT, socket.send(message)).await;
        if !matches!(sent, Ok(Ok(()))) {
            return;
        }
    }
}

async fn bind(
    socket: &mut impl Socket,
    endpoint: &str,
    ipc_files: &mut IpcFiles,
) -> Result<String, PublisherError> {
    let address = bind_address(endpoint)?;
    if let Some(path) = address.strip_prefix("ipc://") {
        remove_stale_socket(Path::new(path));
    }
    match socket.bind(&address).await {
        Ok(bound) => {
            let bound = bound.to_string();
            if let Some(path) = bound.strip_prefix("ipc://") {
                ipc_files.0.push(path.to_string());
            }
            Ok(bound)
        }
        Err(error) => Err(PublisherError::Bind {
            endpoint: endpoint.to_string(),
            reason: error.to_string(),
        }),
    }
}

/// zeromq takes the host literally, so spell out the wildcards libzmq accepts.
fn bind_address(endpoint: &str) -> Result<String, PublisherError> {
    let unsupported = || PublisherError::UnsupportedEndpoint(endpoint.to_string());
    if let Some(address) = endpoint.strip_prefix("tcp://") {
        let (host, port) = address.rsplit_once(':').ok_or_else(unsupported)?;
        let host = match host {
            "*" => "0.0.0.0",
            "::" => "[::]",
            host => host,
        };
        return Ok(format!("tcp://{host}:{port}"));
    }
    match endpoint.strip_prefix("ipc://") {
        // An abstract-namespace name has no filesystem path to bind.
        Some(path) if !path.is_empty() && !path.starts_with('@') => Ok(endpoint.to_string()),
        _ => Err(unsupported()),
    }
}

/// A socket file left by a previous process would fail the bind, as it does
/// not for libzmq, which unlinks it first.
fn remove_stale_socket(path: &Path) {
    use std::os::unix::fs::FileTypeExt;
    if std::fs::symlink_metadata(path).is_ok_and(|meta| meta.file_type().is_socket()) {
        let _ = std::fs::remove_file(path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_queue_drops_the_batch_and_leaves_a_sequence_gap() {
        let (tx, mut rx) = mpsc::channel(1);
        let mut ingress = Ingress {
            next_seq: 0,
            tx: Some(tx),
        };
        let dropped = AtomicU64::new(0);
        let seqs: Vec<u64> = (0..3)
            .map(|_| ingress.publish(Bytes::new(), &dropped).unwrap())
            .collect();
        assert_eq!(seqs, [0, 1, 2]);
        assert_eq!(dropped.load(Ordering::Relaxed), 2);
        assert_eq!(rx.try_recv().unwrap().0, 0);
        // The next batch keeps counting past the dropped ones.
        assert_eq!(ingress.publish(Bytes::new(), &dropped).unwrap(), 3);
        assert_eq!(rx.try_recv().unwrap().0, 3);
    }

    #[test]
    fn zero_queue_limit_means_unlimited() {
        // queue.Queue(maxsize=0) and ZMQ_SNDHWM=0 are unbounded; a one-slot
        // queue here would drop almost every batch of such a config.
        assert_eq!(queue_capacity(0), Semaphore::MAX_PERMITS);
        assert_eq!(queue_capacity(7), 7);
    }

    #[test]
    fn ring_replays_from_the_requested_sequence_and_evicts_the_oldest() {
        let mut ring = Ring {
            entries: VecDeque::new(),
            capacity: 3,
        };
        for seq in [0, 1, 3, 4, 6] {
            ring.push(seq, Bytes::new());
        }
        let seqs = |start| ring.since(start).map(|(seq, _)| *seq).collect::<Vec<_>>();
        assert_eq!(seqs(0), [3, 4, 6]);
        // A start inside a gap resumes at the next retained batch.
        assert_eq!(seqs(5), [6]);
        assert_eq!(seqs(7), [0u64; 0]);
    }

    #[test]
    fn wildcard_hosts_are_spelled_out() {
        assert_eq!(bind_address("tcp://*:5557").unwrap(), "tcp://0.0.0.0:5557");
        assert_eq!(bind_address("tcp://:::5557").unwrap(), "tcp://[::]:5557");
        assert_eq!(
            bind_address("tcp://127.0.0.1:5557").unwrap(),
            "tcp://127.0.0.1:5557"
        );
        assert!(bind_address("inproc://kv").is_err());
        assert!(bind_address("ipc://@abstract").is_err());
    }
}
