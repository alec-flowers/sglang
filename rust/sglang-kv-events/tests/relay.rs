//! The relay and its gRPC service, end to end: a publisher on one side, a
//! tonic client on the other.

mod common;

use std::time::Duration;

use sglang_kv_events::publisher::{Publisher, PublisherConfig};
use sglang_kv_events::relay::{EndReason, Relay, RelayConfig};
use sglang_kv_events::service::KvEventService;
use sglang_kv_events::subscriber::SubscriberConfig;
use sglang_kv_v1_types as pb;
use sglang_kv_v1_types::kv_event_service_client::KvEventServiceClient;
use sglang_kv_v1_types::kv_stream_item::Item;
use tokio::time::timeout;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Channel;
use tonic::{Code, Streaming};

use common::{WAIT, WithholdingPublisher, payload};

type Client = KvEventServiceClient<Channel>;

fn publisher() -> Publisher {
    Publisher::start(PublisherConfig {
        endpoint: "tcp://127.0.0.1:0".to_string(),
        replay_endpoint: Some("tcp://127.0.0.1:0".to_string()),
        topic: "kv".to_string(),
        buffer_steps: 1024,
        max_queue_size: 1024,
        hwm: 1024,
    })
    .unwrap()
}

fn source(publisher: &Publisher) -> SubscriberConfig {
    SubscriberConfig {
        endpoint: publisher.local_pub_endpoint().to_string(),
        replay_endpoint: publisher.local_replay_endpoint().unwrap().to_string(),
        topic: "kv".to_string(),
    }
}

fn relay(source: SubscriberConfig, history_max_batches: usize, consumer_queue: usize) -> Relay {
    Relay::start(RelayConfig {
        source,
        dp_rank: 3,
        block_size: 1,
        hash_format: pb::KvHashFormat::Dynamo,
        history_max_batches,
        history_max_bytes: 1 << 20,
        consumer_queue,
    })
    .unwrap()
}

async fn serve(relay: &Relay) -> Client {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let service = KvEventService::new(relay.handle());
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    // A deadline on every RPC, so a regression fails the test instead of
    // hanging it.
    let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{address}"));
    let channel = endpoint.unwrap().timeout(3 * WAIT).connect().await.unwrap();
    Client::new(channel)
}

fn publish(publisher: &Publisher, seqs: std::ops::Range<u64>) {
    for seq in seqs {
        assert_eq!(publisher.publish(payload(seq)).unwrap(), seq);
    }
}

/// The stream info once the relay has taken in every batch before `next_seq`.
async fn info_at(client: &mut Client, next_seq: u64) -> pb::KvStreamInfo {
    let poll = async {
        loop {
            let info = client.get_kv_stream_info(pb::GetKvStreamInfoRequest {});
            let info = info.await.unwrap().into_inner();
            if info.next_seq == next_seq {
                return info;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    timeout(WAIT, poll).await.expect("relay did not catch up")
}

fn subscription(
    resume: Option<(&str, u64)>,
    on_miss: pb::KvCursorMiss,
) -> pb::SubscribeKvEventsRequest {
    pb::SubscribeKvEventsRequest {
        resume: resume.map(|(publisher_id, next_seq)| pb::KvCursor {
            publisher_id: publisher_id.to_string(),
            next_seq,
        }),
        on_miss: on_miss as i32,
        max_chunk_bytes: 0,
    }
}

async fn next(stream: &mut Streaming<pb::KvStreamItem>) -> Result<Item, Code> {
    let item = timeout(WAIT, stream.message()).await.expect("no item");
    match item {
        Ok(item) => Ok(item.expect("stream ended").item.expect("empty item")),
        Err(status) => Err(status.code()),
    }
}

async fn expect_start(
    stream: &mut Streaming<pb::KvStreamItem>,
    mode: pb::KvStreamMode,
    next_seq: u64,
) -> pb::KvStreamInfo {
    let Ok(Item::Start(start)) = next(stream).await else {
        panic!("expected a start item");
    };
    assert_eq!((start.mode(), start.next_seq), (mode, next_seq));
    start.info.expect("stream info")
}

/// The next item that is not a heartbeat; an idle second is not an event.
async fn next_event(stream: &mut Streaming<pb::KvStreamItem>) -> Result<Item, Code> {
    loop {
        match next(stream).await {
            Ok(Item::Heartbeat(_)) => {}
            event => return event,
        }
    }
}

async fn expect_batches(stream: &mut Streaming<pb::KvStreamItem>, seqs: std::ops::Range<u64>) {
    for seq in seqs {
        let Ok(Item::Batch(batch)) = next_event(stream).await else {
            panic!("expected batch {seq}");
        };
        assert_eq!(batch.seq, seq);
        // Batch `seq` stores the block whose hash is `seq`.
        let Some(pb::kv_cache_event::Event::Stored(stored)) = &batch.events[0].event else {
            panic!("expected a stored event");
        };
        assert_eq!(stored.block_hashes, [seq as i64]);
    }
}

#[tokio::test]
async fn consumer_resumes_from_its_cursor_without_a_gap_or_a_repeat() {
    let publisher = publisher();
    let relay = relay(source(&publisher), 1024, 1024);
    let mut client = serve(&relay).await;
    publish(&publisher, 0..5);
    let info = info_at(&mut client, 5).await;
    assert_eq!((info.dp_rank, info.oldest_seq, info.complete), (3, 0, true));

    let request = subscription(Some((&info.publisher_id, 2)), pb::KvCursorMiss::Fail);
    let mut stream = client
        .subscribe_kv_events(request)
        .await
        .unwrap()
        .into_inner();
    let start = expect_start(&mut stream, pb::KvStreamMode::Events, 2).await;
    assert_eq!(start.publisher_id, info.publisher_id);
    // The backlog, then batches published after the subscription.
    expect_batches(&mut stream, 2..5).await;
    publish(&publisher, 5..7);
    expect_batches(&mut stream, 5..7).await;
    // An idle stream reports the cursor it has reached.
    assert_eq!(
        next(&mut stream).await,
        Ok(Item::Heartbeat(pb::KvHeartbeat { next_seq: 7 }))
    );

    let range = pb::GetKvEventsRequest {
        from: Some(pb::KvCursor {
            publisher_id: info.publisher_id,
            next_seq: 1,
        }),
        end_seq: Some(3),
    };
    let mut range = client.get_kv_events(range).await.unwrap().into_inner();
    expect_start(&mut range, pb::KvStreamMode::Events, 1).await;
    expect_batches(&mut range, 1..3).await;
    assert!(range.message().await.unwrap().is_none());
}

#[tokio::test]
async fn cursor_the_history_cannot_serve_is_refused_or_follows_live() {
    let publisher = publisher();
    let relay = relay(source(&publisher), 2, 1024);
    let mut client = serve(&relay).await;
    publish(&publisher, 0..5);
    let info = info_at(&mut client, 5).await;
    assert_eq!(info.oldest_seq, 3);

    let fail = pb::KvCursorMiss::Fail;
    let evicted = subscription(Some((&info.publisher_id, 1)), fail);
    let status = client.subscribe_kv_events(evicted).await.unwrap_err();
    assert_eq!(status.code(), Code::OutOfRange);
    let foreign = subscription(Some(("another-publisher", 4)), fail);
    let status = client.subscribe_kv_events(foreign).await.unwrap_err();
    assert_eq!(status.code(), Code::FailedPrecondition);
    // Failing on a miss is only meaningful with something to miss.
    let status = client.subscribe_kv_events(subscription(None, fail)).await;
    assert_eq!(status.unwrap_err().code(), Code::InvalidArgument);
    let range = pb::GetKvEventsRequest {
        from: Some(pb::KvCursor {
            publisher_id: info.publisher_id.clone(),
            next_seq: 1,
        }),
        end_seq: None,
    };
    let status = client.get_kv_events(range).await.unwrap_err();
    assert_eq!(status.code(), Code::OutOfRange);

    let live_only = subscription(Some((&info.publisher_id, 1)), pb::KvCursorMiss::LiveOnly);
    let mut stream = client
        .subscribe_kv_events(live_only)
        .await
        .unwrap()
        .into_inner();
    expect_start(&mut stream, pb::KvStreamMode::LiveOnly, 5).await;
    publish(&publisher, 5..6);
    expect_batches(&mut stream, 5..6).await;
}

#[tokio::test]
async fn lost_history_ends_streams_and_starts_a_new_publisher_id() {
    let mut publisher = WithholdingPublisher::bind().await;
    publisher.ring.push(0);
    let relay = relay(publisher.config.clone(), 1024, 1024);
    let mut client = serve(&relay).await;
    assert_eq!(publisher.serve_replay().await, 0);
    let before = info_at(&mut client, 1).await;
    let request = subscription(None, pb::KvCursorMiss::LiveOnly);
    let mut stream = client
        .subscribe_kv_events(request)
        .await
        .unwrap()
        .into_inner();
    expect_start(&mut stream, pb::KvStreamMode::LiveOnly, 1).await;

    // The publisher's ring has moved past 1..5 without the relay seeing them.
    publisher.ring = vec![5];
    publisher.send_live(5).await;
    while publisher.serve_replay().await != 1 {}
    assert_eq!(next_event(&mut stream).await, Err(Code::DataLoss));

    let after = info_at(&mut client, 6).await;
    assert_ne!(after.publisher_id, before.publisher_id);
    assert_eq!((after.oldest_seq, after.complete), (5, false));
    // The old cursor names a stream that no longer exists.
    let stale = subscription(Some((&before.publisher_id, 1)), pb::KvCursorMiss::Fail);
    let status = client.subscribe_kv_events(stale).await.unwrap_err();
    assert_eq!(status.code(), Code::FailedPrecondition);
}

#[tokio::test]
async fn consumer_that_does_not_keep_up_is_dropped() {
    let publisher = publisher();
    let relay = relay(source(&publisher), 1024, 1);
    let handle = relay.handle();
    let admission = handle.subscribe(None, pb::KvCursorMiss::LiveOnly);
    let mut live = admission.await.unwrap().live;
    publish(&publisher, 0..3);
    let caught_up = async {
        while handle.info().await.unwrap().next_seq != 3 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    timeout(WAIT, caught_up)
        .await
        .expect("relay did not catch up");

    // The one queued batch is delivered; the overflow ended the feed.
    assert_eq!(live.recv().await.unwrap().seq, 0);
    assert_eq!(live.recv().await.unwrap_err(), EndReason::Lagged);
}
