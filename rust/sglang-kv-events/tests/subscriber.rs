//! The subscriber against a real publisher: what it must deliver when
//! PUB/SUB drops batches.

mod common;

use std::time::{Duration, Instant};

use sglang_kv_events::publisher::{Publisher, PublisherConfig};
use sglang_kv_events::subscriber::{StreamEvent, SubscriberConfig, run_subscriber};
use tokio::sync::mpsc;
use tokio::time::timeout;

use common::{WAIT, WithholdingPublisher, payload};

fn publisher(buffer_steps: usize) -> Publisher {
    Publisher::start(PublisherConfig {
        endpoint: "tcp://127.0.0.1:0".to_string(),
        replay_endpoint: Some("tcp://127.0.0.1:0".to_string()),
        topic: "kv".to_string(),
        buffer_steps,
        max_queue_size: 4096,
        hwm: 4096,
        accept_resync: false,
    })
    .unwrap()
}

fn subscribe(publisher: &Publisher) -> mpsc::Receiver<StreamEvent> {
    let config = SubscriberConfig {
        endpoint: publisher.local_pub_endpoint().to_string(),
        replay_endpoint: publisher.local_replay_endpoint().unwrap().to_string(),
        topic: "kv".to_string(),
        resync_min_interval: None,
    };
    let (tx, rx) = mpsc::channel(4096);
    tokio::spawn(async move { run_subscriber(config, tx).await.unwrap() });
    rx
}

fn publish(publisher: &Publisher, seqs: std::ops::Range<u64>) {
    for seq in seqs {
        assert_eq!(publisher.publish(payload(seq)).unwrap(), seq);
    }
}

async fn next(events: &mut mpsc::Receiver<StreamEvent>) -> StreamEvent {
    let event = timeout(WAIT, events.recv()).await;
    event.expect("no event").expect("subscriber stopped")
}

async fn expect_batches(events: &mut mpsc::Receiver<StreamEvent>, seqs: std::ops::Range<u64>) {
    for seq in seqs {
        let payload = payload(seq);
        assert_eq!(next(events).await, StreamEvent::Batch { seq, payload });
    }
}

#[tokio::test]
async fn batches_from_before_the_subscription_arrive_first_and_once() {
    let publisher = publisher(1024);
    publish(&publisher, 0..5);
    let mut events = subscribe(&publisher);
    expect_batches(&mut events, 0..5).await;
    publish(&publisher, 5..10);
    expect_batches(&mut events, 5..10).await;
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
    publish(&publisher, 0..10);
    let mut events = subscribe(&publisher);
    let lost = StreamEvent::Lost {
        expected: 0,
        resumed_at: 8,
    };
    assert_eq!(next(&mut events).await, lost);
    expect_batches(&mut events, 8..10).await;
}

#[tokio::test]
async fn losses_ask_for_a_resync_at_most_once_per_interval() {
    let interval = Duration::from_secs(4);
    let mut publisher = WithholdingPublisher::bind().await;
    publisher.ring.push(0);
    let config = SubscriberConfig {
        resync_min_interval: Some(interval),
        ..publisher.config.clone()
    };
    let (tx, mut events) = mpsc::channel(64);
    tokio::spawn(run_subscriber(config, tx));
    assert_eq!(publisher.serve_replay().await, 0);
    expect_batches(&mut events, 0..1).await;

    // Two holes the ring cannot fill, well inside one interval.
    let first_loss = Instant::now();
    for (expected, resumed_at) in [(1, 5), (6, 9)] {
        publisher.ring = vec![resumed_at];
        publisher.send_live(resumed_at).await;
        while publisher.serve_replay().await != expected {}
        let lost = StreamEvent::Lost {
            expected,
            resumed_at,
        };
        assert_eq!(next(&mut events).await, lost);
        expect_batches(&mut events, resumed_at..resumed_at + 1).await;
    }
    while publisher.resyncs < 2 {
        publisher.serve_replay().await;
    }
    // The second request waited for the interval instead of following its loss.
    assert!(first_loss.elapsed() >= interval);
}
