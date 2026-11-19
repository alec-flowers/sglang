//! The publisher's wire contract, exercised over real sockets.

use std::time::Duration;

use bytes::Bytes;
use sglang_kv_events::publisher::{END_SEQ, Publisher, PublisherConfig, RESYNC_REQUEST};
use tokio::time::timeout;
use zeromq::{DealerSocket, Socket, SocketRecv, SocketSend, SubSocket, ZmqMessage};

const WAIT: Duration = Duration::from_secs(10);

fn config(endpoint: &str, replay_endpoint: Option<&str>) -> PublisherConfig {
    PublisherConfig {
        endpoint: endpoint.to_string(),
        replay_endpoint: replay_endpoint.map(str::to_string),
        topic: "kv@model".to_string(),
        buffer_steps: 2,
        max_queue_size: 1024,
        hwm: 1024,
        accept_resync: false,
    }
}

async fn recv(socket: &mut impl SocketRecv) -> Vec<Bytes> {
    let message = timeout(WAIT, socket.recv()).await.expect("no message");
    message.expect("recv").into_vec()
}

/// Subscribe, then publish until one batch arrives: PUB drops batches sent
/// before the subscription reaches it.
async fn joined_subscriber(publisher: &Publisher) -> SubSocket {
    let mut subscriber = SubSocket::new();
    subscriber
        .connect(publisher.local_pub_endpoint())
        .await
        .unwrap();
    subscriber.subscribe("").await.unwrap();
    let joined = async {
        loop {
            publisher.publish(Bytes::from_static(b"probe")).unwrap();
            if timeout(Duration::from_millis(50), subscriber.recv())
                .await
                .is_ok()
            {
                return;
            }
        }
    };
    timeout(WAIT, joined)
        .await
        .expect("no batch reached the subscriber");
    subscriber
}

#[tokio::test]
async fn live_batches_carry_topic_sequence_and_payload() {
    let publisher = Publisher::start(config("tcp://127.0.0.1:0", None)).unwrap();
    let mut subscriber = joined_subscriber(&publisher).await;

    let seq = publisher.publish(Bytes::from_static(b"payload")).unwrap();
    loop {
        let frames = recv(&mut subscriber).await;
        assert_eq!(frames.len(), 3);
        assert_eq!(frames[0].as_ref(), b"kv@model");
        if frames[2].as_ref() == b"payload" {
            assert_eq!(frames[1].as_ref(), seq.to_be_bytes());
            break;
        }
    }
}

#[tokio::test]
async fn replay_serves_the_retained_batches_then_the_end_marker() {
    let publisher =
        Publisher::start(config("tcp://127.0.0.1:0", Some("tcp://127.0.0.1:0"))).unwrap();
    for payload in ["a", "b", "c", "d"] {
        publisher.publish(Bytes::from(payload)).unwrap();
    }

    let mut dealer = DealerSocket::new();
    dealer
        .connect(publisher.local_replay_endpoint().unwrap())
        .await
        .unwrap();
    // A request without the 8-byte sequence frame is ignored, not fatal.
    dealer.send(ZmqMessage::from("bogus")).await.unwrap();
    let request = vec![Bytes::new(), Bytes::copy_from_slice(&0u64.to_be_bytes())];
    dealer
        .send(ZmqMessage::try_from(request).unwrap())
        .await
        .unwrap();

    // buffer_steps is 2, so only the last two of the four batches remain.
    for (seq, payload) in [(2u64, "c"), (3, "d")] {
        let frames = recv(&mut dealer).await;
        let expected: [&[u8]; 4] = [b"", b"kv@model", &seq.to_be_bytes(), payload.as_bytes()];
        assert_eq!(frames, expected);
    }
    let expected: [&[u8]; 4] = [b"", b"", &END_SEQ, b""];
    assert_eq!(recv(&mut dealer).await, expected);
}

#[tokio::test]
async fn resync_request_reaches_only_a_publisher_that_accepts_one() {
    for accept_resync in [true, false] {
        let publisher = Publisher::start(PublisherConfig {
            accept_resync,
            ..config("tcp://127.0.0.1:0", Some("tcp://127.0.0.1:0"))
        })
        .unwrap();
        let mut dealer = DealerSocket::new();
        dealer
            .connect(publisher.local_replay_endpoint().unwrap())
            .await
            .unwrap();
        let resync = vec![Bytes::new(), Bytes::from_static(RESYNC_REQUEST)];
        dealer
            .send(ZmqMessage::try_from(resync).unwrap())
            .await
            .unwrap();
        // Requests are handled in order: this reply follows the resync request.
        let replay = vec![Bytes::new(), Bytes::copy_from_slice(&0u64.to_be_bytes())];
        dealer
            .send(ZmqMessage::try_from(replay).unwrap())
            .await
            .unwrap();
        let expected: [&[u8]; 4] = [b"", b"", &END_SEQ, b""];
        assert_eq!(recv(&mut dealer).await, expected);

        assert_eq!(publisher.take_resync_request(), accept_resync);
        assert!(!publisher.take_resync_request());
    }
}

#[tokio::test]
async fn shutdown_publishes_the_queue_and_removes_the_ipc_socket() {
    let path = std::env::temp_dir().join(format!("kv-events-{}.sock", std::process::id()));
    let publisher = Publisher::start(config(&format!("ipc://{}", path.display()), None)).unwrap();
    assert!(path.exists());
    let mut subscriber = joined_subscriber(&publisher).await;

    let last = (0..100)
        .map(|_| publisher.publish(Bytes::from_static(b"queued")).unwrap())
        .last()
        .unwrap();
    publisher.shutdown();
    assert!(!path.exists());
    assert!(publisher.publish(Bytes::new()).is_err());

    loop {
        let frames = recv(&mut subscriber).await;
        if frames[1].as_ref() == last.to_be_bytes() {
            break;
        }
    }
}

/// Enough 64 KiB batches that an unread copy of them cannot sit in socket buffers.
fn publish_bulk(publisher: &Publisher) -> u64 {
    let batch = Bytes::from(vec![0u8; 64 * 1024]);
    let seqs = (0..512).map(|_| publisher.publish(batch.clone()).unwrap());
    seqs.last().unwrap()
}

// Every test here runs on a current-thread runtime. In a multi_thread test the
// body runs outside the scheduler, where a zeromq socket spins for good once
// tokio's task budget runs out.
#[tokio::test]
async fn replay_requester_that_leaves_mid_reply_does_not_stall_the_publisher() {
    // A requester that stops reading and then disconnects used to leave the
    // publisher thread blocked for good: no live batches, no replays, and a
    // shutdown that never returned.
    let mut config = config("tcp://127.0.0.1:0", Some("tcp://127.0.0.1:0"));
    config.buffer_steps = 1024;
    let publisher = std::sync::Arc::new(Publisher::start(config).unwrap());
    let last = publish_bulk(&publisher);
    let replay_request = |start: u64| {
        let frames = vec![Bytes::new(), Bytes::copy_from_slice(&start.to_be_bytes())];
        ZmqMessage::try_from(frames).unwrap()
    };

    let mut stalled = DealerSocket::new();
    stalled
        .connect(publisher.local_replay_endpoint().unwrap())
        .await
        .unwrap();
    stalled.send(replay_request(0)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    drop(stalled);

    let mut dealer = DealerSocket::new();
    dealer
        .connect(publisher.local_replay_endpoint().unwrap())
        .await
        .unwrap();
    dealer.send(replay_request(last)).await.unwrap();
    assert_eq!(recv(&mut dealer).await[2].as_ref(), last.to_be_bytes());
    assert_eq!(recv(&mut dealer).await[2].as_ref(), END_SEQ);

    let shutdown = tokio::task::spawn_blocking(move || publisher.shutdown());
    timeout(WAIT, shutdown)
        .await
        .expect("shutdown hung")
        .unwrap();
}

#[tokio::test]
async fn subscriber_that_stops_reading_does_not_starve_the_others() {
    // The PUB socket writes to subscribers in turn, so one that stops reading
    // used to hold up every other subscriber indefinitely.
    let publisher = Publisher::start(config("tcp://127.0.0.1:0", None)).unwrap();
    let _stalled = joined_subscriber(&publisher).await;
    let mut healthy = joined_subscriber(&publisher).await;
    let last = publish_bulk(&publisher);

    let caught_up = async {
        loop {
            // Keep the stream moving: a subscriber that was dropped with the
            // stalled one only sees batches published after it reconnects.
            publisher.publish(Bytes::from_static(b"tick")).unwrap();
            let frames = timeout(Duration::from_millis(50), healthy.recv()).await;
            if let Ok(Ok(frames)) = frames
                && u64::from_be_bytes(frames.get(1).unwrap().as_ref().try_into().unwrap()) > last
            {
                return;
            }
        }
    };
    timeout(WAIT, caught_up)
        .await
        .expect("the healthy subscriber starved");
}
