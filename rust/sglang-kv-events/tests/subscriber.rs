//! The subscriber against a real publisher: what it must deliver when
//! PUB/SUB drops batches.

use std::time::Duration;

use bytes::Bytes;
use sglang_kv_events::publisher::{END_SEQ, Publisher, PublisherConfig};
use sglang_kv_events::subscriber::{StreamEvent, SubscriberConfig, run_subscriber};
use tokio::sync::mpsc;
use tokio::time::timeout;
use zeromq::{PubSocket, RouterSocket, Socket, SocketRecv, SocketSend, ZmqMessage};

fn publisher(buffer_steps: usize) -> Publisher {
    Publisher::start(PublisherConfig {
        endpoint: "tcp://127.0.0.1:0".to_string(),
        replay_endpoint: Some("tcp://127.0.0.1:0".to_string()),
        topic: "kv".to_string(),
        buffer_steps,
        max_queue_size: 4096,
        hwm: 4096,
    })
    .unwrap()
}

fn subscribe(publisher: &Publisher) -> mpsc::Receiver<StreamEvent> {
    let config = SubscriberConfig {
        endpoint: publisher.local_pub_endpoint().to_string(),
        replay_endpoint: publisher.local_replay_endpoint().unwrap().to_string(),
        topic: "kv".to_string(),
    };
    let (tx, rx) = mpsc::channel(4096);
    tokio::spawn(async move { run_subscriber(config, tx).await.unwrap() });
    rx
}

fn publish(publisher: &Publisher, count: u64) {
    for _ in 0..count {
        publisher.publish(Bytes::from_static(b"batch")).unwrap();
    }
}

async fn next(events: &mut mpsc::Receiver<StreamEvent>) -> StreamEvent {
    let event = timeout(Duration::from_secs(10), events.recv()).await;
    event.expect("no event").expect("subscriber stopped")
}

async fn expect_batches(events: &mut mpsc::Receiver<StreamEvent>, seqs: std::ops::Range<u64>) {
    for seq in seqs {
        let payload = Bytes::from_static(b"batch");
        assert_eq!(next(events).await, StreamEvent::Batch { seq, payload });
    }
}

#[tokio::test]
async fn batches_from_before_the_subscription_arrive_first_and_once() {
    let publisher = publisher(1024);
    publish(&publisher, 5);
    let mut events = subscribe(&publisher);
    expect_batches(&mut events, 0..5).await;
    publish(&publisher, 5);
    expect_batches(&mut events, 5..10).await;
}

/// A publisher that only sends live what the test tells it to, and replays
/// whatever the test has put in its ring.
struct WithholdingPublisher {
    live: PubSocket,
    replay: RouterSocket,
    ring: Vec<u64>,
    config: SubscriberConfig,
}

impl WithholdingPublisher {
    async fn bind() -> Self {
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

    async fn send_live(&mut self, seq: u64) {
        let frames = vec![
            Bytes::from_static(b"kv"),
            Bytes::copy_from_slice(&seq.to_be_bytes()),
            Bytes::from_static(b"batch"),
        ];
        self.live.send(frames.try_into().unwrap()).await.unwrap();
    }

    /// Answer the next replay request from the ring; returns its start.
    async fn serve_replay(&mut self) -> u64 {
        let request = timeout(Duration::from_secs(10), self.replay.recv()).await;
        let request = request.expect("no replay request").unwrap().into_vec();
        let start = u64::from_be_bytes(request[2].as_ref().try_into().unwrap());
        let reply = |topic: &'static [u8], seq: [u8; 8], payload: &'static [u8]| {
            let frames = vec![
                request[0].clone(),
                Bytes::new(),
                Bytes::from_static(topic),
                Bytes::copy_from_slice(&seq),
                Bytes::from_static(payload),
            ];
            ZmqMessage::try_from(frames).unwrap()
        };
        for seq in self.ring.iter().filter(|seq| **seq >= start) {
            let message = reply(b"kv", seq.to_be_bytes(), b"batch");
            self.replay.send(message).await.unwrap();
        }
        self.replay.send(reply(b"", END_SEQ, b"")).await.unwrap();
        start
    }
}

#[tokio::test]
async fn withheld_live_batches_are_recovered_including_the_last() {
    let mut publisher = WithholdingPublisher::bind().await;
    publisher.ring.push(0);
    let (tx, mut events) = mpsc::channel(64);
    tokio::spawn(run_subscriber(publisher.config.clone(), tx));
    assert_eq!(publisher.serve_replay().await, 0);
    expect_batches(&mut events, 0..1).await;

    // 1 and 2 never go out live. Until the subscription reaches the
    // publisher, 3 does not either, and the tail probe finds all three.
    publisher.ring.extend([1, 2, 3]);
    publisher.send_live(3).await;
    assert_eq!(publisher.serve_replay().await, 1);
    expect_batches(&mut events, 1..4).await;

    // Nothing follows 4 and 5 to reveal that they were dropped.
    publisher.ring.extend([4, 5]);
    while publisher.serve_replay().await != 4 {}
    expect_batches(&mut events, 4..6).await;
}

#[tokio::test]
async fn batches_evicted_from_the_ring_are_reported_lost() {
    let publisher = publisher(2);
    publish(&publisher, 10);
    let mut events = subscribe(&publisher);
    let lost = StreamEvent::Lost {
        expected: 0,
        resumed_at: 8,
    };
    assert_eq!(next(&mut events).await, lost);
    expect_batches(&mut events, 8..10).await;
}
