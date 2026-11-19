//! The tonic `sglang.kv.v1.KvEventService` over a [`RelayHandle`].

use std::time::Duration;

use sglang_kv_v1_types as pb;
use sglang_kv_v1_types::kv_event_service_server::KvEventServiceServer;
use sglang_kv_v1_types::kv_stream_item::Item;
use tokio::sync::watch;
use tonic::codegen::BoxStream;
use tonic::{Request, Response, Status};

use crate::relay::{Admission, Batch, EndReason, RelayError, RelayHandle};

// Arbitrary; an idle stream sends this often so a dead peer is noticed.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);

pub struct KvEventService {
    relay: RelayHandle,
    stop: watch::Receiver<bool>,
}

impl KvEventService {
    /// Open streams end with UNAVAILABLE once `stop` turns true, so a server
    /// closing its listeners does not wait on them.
    pub fn new(relay: RelayHandle, stop: watch::Receiver<bool>) -> KvEventServiceServer<Self> {
        KvEventServiceServer::new(Self { relay, stop })
    }
}

async fn stopped(mut stop: watch::Receiver<bool>) {
    if stop.wait_for(|stop| *stop).await.is_err() {
        // The sender is gone without asking for a stop.
        std::future::pending::<()>().await;
    }
}

fn status(error: RelayError) -> Status {
    let message = error.to_string();
    match error {
        RelayError::Unavailable => Status::unavailable(message),
        RelayError::OutOfRange { .. } => Status::out_of_range(message),
        RelayError::WrongPublisher { .. } => Status::failed_precondition(message),
        RelayError::NoSnapshot => Status::unimplemented(message),
        RelayError::NoCursor => Status::invalid_argument(message),
    }
}

fn end_status(reason: EndReason) -> Status {
    match reason {
        EndReason::DataLoss => Status::data_loss(
            "KV event history was lost; the stream continues under a new publisher_id",
        ),
        EndReason::Lagged => {
            Status::aborted("the consumer fell too far behind; resume from the last cursor")
        }
        EndReason::Unavailable => Status::unavailable("the KV event relay stopped"),
    }
}

fn item(item: Item) -> pb::KvStreamItem {
    pb::KvStreamItem { item: Some(item) }
}

fn start(mode: pb::KvStreamMode, info: pb::KvStreamInfo, next_seq: u64) -> pb::KvStreamItem {
    item(Item::Start(pb::KvStreamStart {
        mode: mode as i32,
        info: Some(info),
        next_seq,
    }))
}

fn batch(batch: &Batch) -> pb::KvStreamItem {
    item(Item::Batch(pb::KvEventBatch::clone(batch)))
}

fn follow(admission: Admission, stop: watch::Receiver<bool>) -> BoxStream<pb::KvStreamItem> {
    let Admission {
        mode,
        info,
        mut next_seq,
        backlog,
        mut live,
    } = admission;
    Box::pin(async_stream::try_stream! {
        yield start(mode, info, next_seq);
        for missed in &backlog {
            next_seq = missed.seq + 1;
            yield batch(missed);
        }
        drop(backlog);
        let stopped = stopped(stop);
        tokio::pin!(stopped);
        loop {
            let received = tokio::select! {
                received = tokio::time::timeout(HEARTBEAT_INTERVAL, live.recv()) => Some(received),
                () = &mut stopped => None,
            };
            match received {
                Some(Ok(Ok(live_batch))) => {
                    next_seq = live_batch.seq + 1;
                    yield batch(&live_batch);
                }
                Some(Ok(Err(reason))) => Err(end_status(reason))?,
                Some(Err(_idle)) => yield item(Item::Heartbeat(pb::KvHeartbeat { next_seq })),
                None => Err(Status::unavailable("the server is shutting down"))?,
            }
        }
    })
}

#[tonic::async_trait]
impl pb::kv_event_service_server::KvEventService for KvEventService {
    async fn get_kv_stream_info(
        &self,
        _request: Request<pb::GetKvStreamInfoRequest>,
    ) -> Result<Response<pb::KvStreamInfo>, Status> {
        let info = self.relay.info().await.map_err(status)?;
        Ok(Response::new(info))
    }

    async fn subscribe_kv_events(
        &self,
        request: Request<pb::SubscribeKvEventsRequest>,
    ) -> Result<Response<BoxStream<pb::KvStreamItem>>, Status> {
        let request = request.into_inner();
        let on_miss = request.on_miss();
        let admission = self.relay.subscribe(request.resume, on_miss).await;
        let admission = admission.map_err(status)?;
        Ok(Response::new(follow(admission, self.stop.clone())))
    }

    async fn get_kv_events(
        &self,
        request: Request<pb::GetKvEventsRequest>,
    ) -> Result<Response<BoxStream<pb::KvStreamItem>>, Status> {
        let request = request.into_inner();
        let from = request
            .from
            .ok_or_else(|| Status::invalid_argument("`from` is required"))?;
        let next_seq = from.next_seq;
        let events = self.relay.events(from, request.end_seq).await;
        let (info, batches) = events.map_err(status)?;
        // The stream holds the shared batches and copies one as it is sent.
        let items = std::iter::once(start(pb::KvStreamMode::Events, info, next_seq))
            .chain(batches.into_iter().map(|retained| batch(&retained)))
            .map(Ok);
        Ok(Response::new(Box::pin(tokio_stream::iter(items))))
    }
}
