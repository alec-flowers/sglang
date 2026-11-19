//! A gap-free reader of one publisher's stream: SUB for live batches, DEALER
//! for replay.
//!
//! PUB/SUB may lose batches (before the subscription arrives, or when the
//! publisher's live queue is full). The reader asks the publisher's replay
//! socket for whatever it is missing: everything at start, the hole when a
//! live batch arrives out of sequence, and the tail when the stream goes
//! quiet. A hole the replay ring can no longer fill is reported as
//! [`StreamEvent::Lost`].

use std::collections::BTreeMap;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::time::{Instant, MissedTickBehavior};
use zeromq::{DealerSocket, Socket, SocketRecv, SocketSend, SubSocket, ZmqMessage};

use crate::publisher::{END_SEQ, RESYNC_REQUEST};

// How long a quiet stream goes unchecked for a lost final batch. Arbitrary;
// it bounds how stale a snapshot can be after the engine goes idle.
const TAIL_PROBE_INTERVAL: Duration = Duration::from_secs(1);
// Arbitrary; a replay of a full ring takes well under a second in-process.
const REPLAY_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct SubscriberConfig {
    pub endpoint: String,
    pub replay_endpoint: String,
    pub topic: String,
    /// After a loss, ask the publisher to publish its whole tree again, at
    /// most this often. None never asks.
    pub resync_min_interval: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamEvent {
    /// The next batch in sequence.
    Batch { seq: u64, payload: Bytes },
    /// Batches `[expected, resumed_at)` are gone for good; the next batch is
    /// `resumed_at`.
    Lost { expected: u64, resumed_at: u64 },
}

#[derive(Debug, thiserror::Error)]
pub enum SubscriberError {
    #[error("cannot connect to the KV event publisher at {endpoint}: {reason}")]
    Connect { endpoint: String, reason: String },
    #[error("the KV event replay socket failed: {0}")]
    Replay(String),
}

/// Puts live and replayed batches back into one sequence.
#[derive(Default)]
struct Sequencer {
    next: u64,
    /// Live batches that arrived ahead of `next`.
    ahead: BTreeMap<u64, Bytes>,
    replay: Option<Replay>,
}

struct Replay {
    /// The first batch that was waiting in `ahead` when the request was sent.
    /// Every earlier batch had been published by then, so a replay that ends
    /// without reaching it proves the batches in between are gone.
    proves_lost_before: Option<u64>,
    requested_at: Instant,
}

impl Sequencer {
    /// Feed a live batch. Returns whether a replay must be requested.
    fn live(&mut self, seq: u64, payload: Bytes, out: &mut Vec<StreamEvent>) -> bool {
        if seq < self.next {
            return false;
        }
        if seq > self.next {
            self.ahead.insert(seq, payload);
            return self.replay.is_none();
        }
        self.emit(seq, payload, out);
        false
    }

    fn replayed(&mut self, seq: u64, payload: Bytes, out: &mut Vec<StreamEvent>) {
        if seq < self.next {
            return;
        }
        // A replay sends every retained batch in order, so skipping ahead
        // means the ring no longer holds what lies between.
        if seq > self.next {
            self.skip_to(seq, out);
        }
        self.emit(seq, payload, out);
    }

    /// The end marker arrived. Returns whether another replay is needed.
    fn replay_ended(&mut self, out: &mut Vec<StreamEvent>) -> bool {
        let Some(replay) = self.replay.take() else {
            return false;
        };
        if let Some(seq) = replay.proves_lost_before
            && self.next < seq
        {
            self.skip_to(seq, out);
            self.drain_ahead(out);
        }
        // A hole that opened while this replay was in flight.
        !self.ahead.is_empty()
    }

    fn start_replay(&mut self) -> u64 {
        let proves_lost_before = match &self.replay {
            // A repeated request: the reply to the first one may still be on
            // its way, and its end marker proves no more than it did then.
            Some(unanswered) => unanswered.proves_lost_before,
            None => self.ahead.keys().next().copied(),
        };
        self.replay = Some(Replay {
            proves_lost_before,
            requested_at: Instant::now(),
        });
        self.next
    }

    fn skip_to(&mut self, seq: u64, out: &mut Vec<StreamEvent>) {
        out.push(StreamEvent::Lost {
            expected: self.next,
            resumed_at: seq,
        });
        self.next = seq;
    }

    fn emit(&mut self, seq: u64, payload: Bytes, out: &mut Vec<StreamEvent>) {
        out.push(StreamEvent::Batch { seq, payload });
        self.next = seq + 1;
        self.drain_ahead(out);
    }

    fn drain_ahead(&mut self, out: &mut Vec<StreamEvent>) {
        while let Some(entry) = self.ahead.first_entry() {
            let seq = *entry.key();
            if seq > self.next {
                break;
            }
            let payload = entry.remove();
            if seq == self.next {
                out.push(StreamEvent::Batch { seq, payload });
                self.next += 1;
            }
        }
    }
}

/// Read the stream until `events` is closed.
pub async fn run_subscriber(
    config: SubscriberConfig,
    events: mpsc::Sender<StreamEvent>,
) -> Result<(), SubscriberError> {
    let connect_error = |endpoint: &str, error: zeromq::ZmqError| SubscriberError::Connect {
        endpoint: endpoint.to_string(),
        reason: error.to_string(),
    };
    let mut live = SubSocket::new();
    live.connect(&config.endpoint)
        .await
        .map_err(|e| connect_error(&config.endpoint, e))?;
    live.subscribe(&config.topic)
        .await
        .map_err(|e| connect_error(&config.endpoint, e))?;
    let mut replay = DealerSocket::new();
    replay
        .connect(&config.replay_endpoint)
        .await
        .map_err(|e| connect_error(&config.replay_endpoint, e))?;

    let mut sequencer = Sequencer::default();
    // Batches published before the subscription took effect.
    request_replay(&mut replay, &mut sequencer).await?;
    let mut probe = tokio::time::interval(TAIL_PROBE_INTERVAL);
    probe.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut quiet = true;
    let mut out = Vec::new();
    let mut resync_wanted = false;
    let mut last_resync: Option<Instant> = None;
    loop {
        let mut needs_replay = false;
        tokio::select! {
            message = live.recv() => match message {
                Ok(message) => {
                    if let [_topic, seq, payload] = message.into_vec().as_slice()
                        && let Some(seq) = sequence_number(seq)
                    {
                        quiet = false;
                        needs_replay = sequencer.live(seq, payload.clone(), &mut out);
                    }
                }
                // The publisher went away; the SUB socket reconnects by itself.
                Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
            },
            message = replay.recv() => {
                let message = message.map_err(|e| SubscriberError::Replay(e.to_string()))?;
                if let [_delimiter, _topic, seq, payload] = message.into_vec().as_slice() {
                    if seq.as_ref() == END_SEQ {
                        needs_replay = sequencer.replay_ended(&mut out);
                    } else if let Some(seq) = sequence_number(seq) {
                        sequencer.replayed(seq, payload.clone(), &mut out);
                    }
                }
            }
            _ = probe.tick() => {
                needs_replay = match &sequencer.replay {
                    // The request or its end marker was lost; ask again.
                    Some(replay) => replay.requested_at.elapsed() > REPLAY_TIMEOUT,
                    // The final batch before a quiet period may have been dropped.
                    None => quiet,
                };
                quiet = true;
            }
            () = events.closed() => return Ok(()),
        }
        if needs_replay {
            request_replay(&mut replay, &mut sequencer).await?;
        }
        for event in out.drain(..) {
            resync_wanted |= matches!(event, StreamEvent::Lost { .. });
            if events.send(event).await.is_err() {
                return Ok(());
            }
        }
        // A loss inside the interval waits for it; the probe tick retries.
        if resync_wanted
            && let Some(interval) = config.resync_min_interval
            && last_resync.is_none_or(|at| at.elapsed() >= interval)
        {
            let request = vec![Bytes::new(), Bytes::from_static(RESYNC_REQUEST)];
            let request = ZmqMessage::try_from(request).expect("two frames");
            let sent = replay.send(request).await;
            sent.map_err(|e| SubscriberError::Replay(e.to_string()))?;
            resync_wanted = false;
            last_resync = Some(Instant::now());
        }
    }
}

async fn request_replay(
    socket: &mut DealerSocket,
    sequencer: &mut Sequencer,
) -> Result<(), SubscriberError> {
    let start_seq = sequencer.start_replay();
    let request = vec![
        Bytes::new(),
        Bytes::copy_from_slice(&start_seq.to_be_bytes()),
    ];
    let request = ZmqMessage::try_from(request).expect("two frames");
    socket
        .send(request)
        .await
        .map_err(|e| SubscriberError::Replay(e.to_string()))
}

fn sequence_number(frame: &Bytes) -> Option<u64> {
    <[u8; 8]>::try_from(frame.as_ref())
        .ok()
        .map(u64::from_be_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload(seq: u64) -> Bytes {
        Bytes::copy_from_slice(&seq.to_be_bytes())
    }

    fn batch(seq: u64) -> StreamEvent {
        StreamEvent::Batch {
            seq,
            payload: payload(seq),
        }
    }

    #[tokio::test]
    async fn replay_fills_a_hole_and_delivers_each_batch_once() {
        let mut sequencer = Sequencer::default();
        let mut out = Vec::new();
        assert!(!sequencer.live(0, payload(0), &mut out));
        // 1 and 2 were dropped; 3 reveals the hole.
        assert!(sequencer.live(3, payload(3), &mut out));
        assert_eq!(sequencer.start_replay(), 1);
        // More live batches arrive while the replay is in flight.
        assert!(!sequencer.live(4, payload(4), &mut out));
        for seq in 1..=4 {
            sequencer.replayed(seq, payload(seq), &mut out);
        }
        assert!(!sequencer.replay_ended(&mut out));
        assert_eq!(out, (0..=4).map(batch).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn hole_the_ring_cannot_fill_is_reported_as_lost() {
        let mut sequencer = Sequencer::default();
        let mut out = Vec::new();
        sequencer.live(0, payload(0), &mut out);
        assert!(sequencer.live(5, payload(5), &mut out));
        sequencer.start_replay();
        // The ring only holds 3 and later.
        for seq in 3..=5 {
            sequencer.replayed(seq, payload(seq), &mut out);
        }
        assert!(!sequencer.replay_ended(&mut out));
        let lost = StreamEvent::Lost {
            expected: 1,
            resumed_at: 3,
        };
        assert_eq!(out, [batch(0), lost, batch(3), batch(4), batch(5)]);

        // A ring that kept nothing at all: the waiting batch marks the resume point.
        out.clear();
        assert!(sequencer.live(9, payload(9), &mut out));
        sequencer.start_replay();
        assert!(!sequencer.replay_ended(&mut out));
        let lost = StreamEvent::Lost {
            expected: 6,
            resumed_at: 9,
        };
        assert_eq!(out, [lost, batch(9)]);
    }

    #[tokio::test]
    async fn late_end_of_a_repeated_request_proves_nothing_about_newer_holes() {
        let mut sequencer = Sequencer::default();
        let mut out = Vec::new();
        sequencer.live(0, payload(0), &mut out);
        assert!(sequencer.live(2, payload(2), &mut out));
        sequencer.start_replay();
        // The reply is slow: 1 and 2 arrive, then a newer hole (3) opens and
        // the request is repeated before the first reply's end marker.
        sequencer.replayed(1, payload(1), &mut out);
        sequencer.replayed(2, payload(2), &mut out);
        assert!(!sequencer.live(4, payload(4), &mut out));
        assert_eq!(sequencer.start_replay(), 3);
        // That end marker was sent before 3 was published.
        assert!(sequencer.replay_ended(&mut out));
        assert_eq!(out, (0..=2).map(batch).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn hole_opened_during_a_replay_gets_its_own_replay() {
        let mut sequencer = Sequencer::default();
        let mut out = Vec::new();
        sequencer.live(0, payload(0), &mut out);
        assert!(sequencer.live(2, payload(2), &mut out));
        sequencer.start_replay();
        // 3 is dropped after the request was sent, so this replay says nothing
        // about it: it must not be declared lost.
        assert!(!sequencer.live(4, payload(4), &mut out));
        sequencer.replayed(1, payload(1), &mut out);
        sequencer.replayed(2, payload(2), &mut out);
        assert!(sequencer.replay_ended(&mut out));
        assert_eq!(sequencer.start_replay(), 3);
        sequencer.replayed(3, payload(3), &mut out);
        sequencer.replayed(4, payload(4), &mut out);
        assert!(!sequencer.replay_ended(&mut out));
        assert_eq!(out, (0..=4).map(batch).collect::<Vec<_>>());
    }
}
