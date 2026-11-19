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
use tokio::sync::watch;
use tokio::time::timeout;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Channel;
use tonic::{Code, Streaming};

use common::{WAIT, WithholdingPublisher, churn_payload, payload};

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
    serve_until(relay, watch::channel(false).1).await
}

async fn serve_until(relay: &Relay, stop: watch::Receiver<bool>) -> Client {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let service = KvEventService::new(relay.handle(), stop);
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
async fn open_streams_end_when_the_server_stops() {
    let publisher = publisher();
    let relay = relay(source(&publisher), 1024, 1024);
    let (stop, stopped) = watch::channel(false);
    let mut client = serve_until(&relay, stopped).await;
    let request = subscription(None, pb::KvCursorMiss::LiveOnly);
    let mut stream = client
        .subscribe_kv_events(request)
        .await
        .unwrap()
        .into_inner();
    expect_start(&mut stream, pb::KvStreamMode::LiveOnly, 0).await;

    stop.send(true).unwrap();
    assert_eq!(next_event(&mut stream).await, Err(Code::Unavailable));
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

/// What a consumer holds: for each block, its parent and the tiers it is in.
type View = std::collections::BTreeMap<i64, (Option<i64>, Vec<i32>)>;

fn apply_store(view: &mut View, stored: &pb::KvBlocksStored) {
    let mut parent = stored.parent_block_hash;
    for hash in &stored.block_hashes {
        let block = view.entry(*hash).or_insert((parent, Vec::new()));
        for tier in &stored.tiers {
            if !block.1.contains(tier) {
                block.1.push(*tier);
            }
        }
        parent = Some(*hash);
    }
}

fn apply_batch(view: &mut View, batch: &pb::KvEventBatch) {
    use pb::kv_cache_event::Event;
    for event in &batch.events {
        match event.event.as_ref().unwrap() {
            Event::Stored(stored) => apply_store(view, stored),
            Event::Removed(removed) => {
                for hash in &removed.block_hashes {
                    if let Some(block) = view.get_mut(hash) {
                        block.1.retain(|tier| *tier != removed.tier);
                        if block.1.is_empty() {
                            view.remove(hash);
                        }
                    }
                }
            }
            Event::Cleared(_) => view.clear(),
        }
    }
}

fn publish_churn(publisher: &Publisher, seqs: std::ops::Range<u64>) {
    for seq in seqs {
        assert_eq!(publisher.publish(churn_payload(seq)).unwrap(), seq);
    }
}

/// Read a snapshot into a view; returns the view, its cut and its chunk count.
async fn load_snapshot(stream: &mut Streaming<pb::KvStreamItem>) -> (View, u64, u32) {
    let mut view = View::new();
    let mut chunks = 0;
    loop {
        match next_event(stream).await {
            Ok(Item::Snapshot(chunk)) => {
                assert_eq!(chunk.index, chunks);
                chunks += 1;
                for stored in &chunk.stored {
                    // A consumer links a block to a parent it already holds,
                    // unless that parent is one of the evicted middle blocks.
                    let parent = stored.parent_block_hash;
                    assert!(parent.is_none_or(|p| view.contains_key(&p) || p % 3 == 1));
                    apply_store(&mut view, stored);
                }
            }
            Ok(Item::SnapshotEnd(end)) => {
                assert_eq!((end.chunks, end.blocks), (chunks, view.len() as u64));
                return (view, end.next_seq, chunks);
            }
            other => panic!("expected a snapshot item, got {other:?}"),
        }
    }
}

/// Apply the batches `[from, until)` of the stream to the view.
async fn follow(stream: &mut Streaming<pb::KvStreamItem>, view: &mut View, from: u64, until: u64) {
    for seq in from..until {
        let Ok(Item::Batch(batch)) = next_event(stream).await else {
            panic!("expected batch {seq}");
        };
        assert_eq!(batch.seq, seq);
        apply_batch(view, &batch);
    }
}

#[tokio::test]
async fn snapshot_then_live_leaves_a_consumer_where_the_whole_stream_would() {
    let publisher = publisher();
    let relay = relay(source(&publisher), 1024, 1024);
    let mut client = serve(&relay).await;
    publish_churn(&publisher, 0..20);
    let info = info_at(&mut client, 20).await;
    assert_eq!(info.blocks, 3 * 20 - 16);

    // A consumer with no position loads the tree in several small chunks.
    let mut request = subscription(None, pb::KvCursorMiss::Snapshot);
    request.max_chunk_bytes = 200;
    let mut loaded = client
        .subscribe_kv_events(request)
        .await
        .unwrap()
        .into_inner();
    expect_start(&mut loaded, pb::KvStreamMode::Snapshot, 20).await;
    // These are published while the snapshot is still being read.
    publish_churn(&publisher, 20..25);
    let (mut loaded_view, cut, chunks) = load_snapshot(&mut loaded).await;
    assert!(chunks > 1 && cut == 20);
    info_at(&mut client, 25).await;

    // The same snapshot serves a later consumer while the history still
    // reaches its cut; it catches up from the batches after it.
    let request = subscription(None, pb::KvCursorMiss::Snapshot);
    let mut reused = client
        .subscribe_kv_events(request)
        .await
        .unwrap()
        .into_inner();
    expect_start(&mut reused, pb::KvStreamMode::Snapshot, 20).await;
    let (mut reused_view, _, _) = load_snapshot(&mut reused).await;

    let from_start = subscription(Some((&info.publisher_id, 0)), pb::KvCursorMiss::Fail);
    let mut followed = client
        .subscribe_kv_events(from_start)
        .await
        .unwrap()
        .into_inner();
    expect_start(&mut followed, pb::KvStreamMode::Events, 0).await;
    let mut followed_view = View::new();

    publish_churn(&publisher, 25..30);
    follow(&mut loaded, &mut loaded_view, 20, 30).await;
    follow(&mut reused, &mut reused_view, 20, 30).await;
    follow(&mut followed, &mut followed_view, 0, 30).await;
    assert_eq!(loaded_view, followed_view);
    assert_eq!(reused_view, followed_view);
    assert_eq!(
        info_at(&mut client, 30).await.blocks,
        followed_view.len() as u64
    );
}

#[tokio::test]
async fn snapshot_rpc_returns_the_tree_at_its_cut_and_ends() {
    let publisher = publisher();
    let relay = relay(source(&publisher), 2, 1024);
    let mut client = serve(&relay).await;
    publish_churn(&publisher, 0..10);
    let info = info_at(&mut client, 10).await;

    let request = pb::GetKvSnapshotRequest { max_chunk_bytes: 0 };
    let mut snapshot = client.get_kv_snapshot(request).await.unwrap().into_inner();
    expect_start(&mut snapshot, pb::KvStreamMode::Snapshot, 10).await;
    let (view, cut, _) = load_snapshot(&mut snapshot).await;
    assert_eq!(cut, 10);
    assert!(snapshot.message().await.unwrap().is_none());

    // The history kept two batches, so only a snapshot knows the first eight.
    let mut expected = View::new();
    for seq in 0..10 {
        let batch = sglang_kv_events::wire::decode_event_batch(&churn_payload(seq)).unwrap();
        apply_batch(&mut expected, &sglang_kv_events::convert::batch(seq, batch));
    }
    assert_eq!(view, expected);

    // A cursor the history cannot serve falls back to that snapshot by default.
    let stale = subscription(Some((&info.publisher_id, 1)), pb::KvCursorMiss::Snapshot);
    let mut stream = client
        .subscribe_kv_events(stale)
        .await
        .unwrap()
        .into_inner();
    expect_start(&mut stream, pb::KvStreamMode::Snapshot, 10).await;
    assert_eq!(load_snapshot(&mut stream).await.0, expected);
}
