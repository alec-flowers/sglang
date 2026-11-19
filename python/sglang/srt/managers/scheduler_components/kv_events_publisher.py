from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Optional,
)

import msgspec
import zmq

from sglang.srt.disaggregation.kv_events import (
    EventPublisherFactory,
    KVEventBatch,
    KVEventsConfig,
    RelayedEventPublisher,
    is_kv_publisher_rank,
    select_kv_publisher_dp_rank,
)
from sglang.srt.managers.io_struct import hook_custom_types, sock_send
from sglang.srt.runtime_context import get_parallel

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from sglang.srt.mem_cache.base_prefix_cache import BasePrefixCache


class SchedulerStats: ...  # type: ignore[no-redef]


class KvMetrics(msgspec.Struct, tag=True, kw_only=True, array_like=True):
    request_active_slots: int = 0
    request_total_slots: int = 0
    kv_active_blocks: int = 0
    kv_total_blocks: int = 0
    num_requests_waiting: int = 0
    gpu_cache_usage_perc: float = 0.0
    gpu_prefix_cache_hit_rate: float = 0.0
    data_parallel_rank: int = 0


hook_custom_types(KvMetrics)


@dataclass(kw_only=True, slots=True)
class SchedulerKvEventsPublisher:
    kv_events_config: Optional[str]
    tree_cache: BasePrefixCache
    send_metrics_from_scheduler: Optional[zmq.Socket]
    max_running_requests: int
    max_total_num_tokens: int
    get_stats: Callable
    enable_kv_cache_events: bool = False
    kv_event_publisher: Any = None
    kv_publisher_dp_rank: int = 0
    # The Rust publisher takes payloads the cache recorder encodes itself.
    publishes_encoded_batches: bool = False
    # The cache can publish its whole tree again when the publisher's consumer
    # lost batches and asks for it.
    resyncs_kv_events: bool = False
    metrics_drop_warned: bool = False

    def __post_init__(self) -> None:
        self.init_kv_events(self.kv_events_config)

    def init_kv_events(self, kv_events_config: Optional[str]):
        self.enable_kv_cache_events = is_kv_publisher_rank(kv_events_config)

        if self.enable_kv_cache_events:
            parallel = get_parallel()
            self.kv_publisher_dp_rank = select_kv_publisher_dp_rank(
                parallel.attn_dp_size, parallel.attn_dp_rank, parallel.dp_rank
            )
            self.kv_event_publisher = EventPublisherFactory.create(
                kv_events_config, self.kv_publisher_dp_rank
            )
            self.publishes_encoded_batches = (
                self.kv_event_publisher.accepts_encoded_batches
            )
            self.resyncs_kv_events = (
                self.publishes_encoded_batches
                and self.tree_cache.supports_kv_event_resync()
            )

    def emit_kv_metrics(self):
        if not self.enable_kv_cache_events:
            return

        kv_metrics = KvMetrics()
        kv_metrics.request_active_slots = self.get_stats().num_running_reqs.total
        kv_metrics.request_total_slots = self.max_running_requests
        kv_metrics.kv_active_blocks = int(
            self.get_stats().token_usage * self.max_total_num_tokens
        )
        kv_metrics.kv_total_blocks = self.max_total_num_tokens
        kv_metrics.num_requests_waiting = self.get_stats().num_queue_reqs.total
        kv_metrics.gpu_cache_usage_perc = self.get_stats().token_usage
        kv_metrics.gpu_prefix_cache_hit_rate = self.get_stats().cache_hit_rate
        kv_metrics.data_parallel_rank = (
            get_parallel().dp_rank if get_parallel().dp_rank is not None else 0
        )

        if not self.send_metrics_from_scheduler.closed:
            # Auxiliary telemetry must not stall the scheduler when the receiver
            # is unavailable or cannot keep up. KV events use a separate publisher.
            try:
                sock_send(
                    self.send_metrics_from_scheduler, kv_metrics, flags=zmq.NOBLOCK
                )
            except zmq.Again:
                if not self.metrics_drop_warned:
                    logger.warning(
                        "KV metrics receiver unavailable or queue full; dropping auxiliary metrics"
                    )
                    self.metrics_drop_warned = True

    def local_kv_event_sources(self, block_size: int) -> list[dict[str, Any]]:
        if self.kv_event_publisher is None:
            return []
        source = self.kv_event_publisher.describe_local_source(block_size)
        return [source] if source is not None else []

    def kv_event_relay_source(self, block_size: int) -> Optional[dict[str, Any]]:
        """The private publisher the Rust server relays as sglang.kv.v1, if any."""
        if not isinstance(self.kv_event_publisher, RelayedEventPublisher):
            return None
        source = self.kv_event_publisher.describe_relay_source(block_size)
        source["hash_format"] = KVEventsConfig.from_cli(self.kv_events_config).format
        source["resync"] = self.resyncs_kv_events
        return source

    def kv_event_streams(self, block_size: int) -> list[dict[str, Any]]:
        """The sglang.kv.v1 streams this rank's listener serves, as
        GetServerInfo reports them."""
        source = self.kv_event_relay_source(block_size)
        if source is None:
            return []
        return [{key: source[key] for key in ("dp_rank", "block_size", "hash_format")}]

    def publish_kv_events(self):
        if not self.enable_kv_cache_events:
            return

        if self.resyncs_kv_events and self.kv_event_publisher.take_resync_request():
            # The whole tree replaces the queued events, which it already shows.
            payloads = self.tree_cache.take_resync_encoded_events(
                ts=time.time(), attn_dp_rank=self.kv_publisher_dp_rank
            )
            logger.warning(
                "A KV event consumer lost batches; publishing the cache's whole "
                "tree again in %d batches",
                len(payloads),
            )
            for payload in payloads:
                self.kv_event_publisher.publish_encoded(payload)
            return

        if self.publishes_encoded_batches:
            payload = self.tree_cache.take_encoded_events(
                ts=time.time(), attn_dp_rank=self.kv_publisher_dp_rank
            )
            if payload is not None:
                self.kv_event_publisher.publish_encoded(payload)
            return

        events = self.tree_cache.take_events()
        if events:
            batch = KVEventBatch(ts=time.time(), events=events)
            self.kv_event_publisher.publish(batch)
