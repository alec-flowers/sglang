"""Unit tests for srt/disaggregation/kv_events KV-event publisher rank selection.

Covers the data-parallel rank used to offset each scheduler's KV-event
publisher port, across pure DP, DP-attention, and single-replica modes. The
port offset must make every independent KV cache publish on a distinct port so
the router can subscribe per replica (the `dp_size` it reads from
`/server_info`).
"""

import atexit
import json
import os
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import msgspec
import zmq

from sglang.srt.arg_groups.validation_hook import check_kv_events_grpc_args
from sglang.srt.disaggregation import kv_events
from sglang.srt.disaggregation.kv_events import (
    AllBlocksCleared,
    BlockRemoved,
    BlockStored,
    EventPublisherFactory,
    KVEventBatch,
    RustZmqEventPublisher,
    StorageMedium,
    ZmqEventPublisher,
    encode_kv_event_batch,
    kv_event_namespace_seed,
    namespaced_block_hash,
    resolve_load_pub_range,
)
from sglang.srt.environ import envs
from sglang.srt.managers.scheduler_components import (
    kv_events_publisher as scheduler_publisher,
)
from sglang.srt.mem_cache.base_prefix_cache import BasePrefixCache
from sglang.srt.mem_cache.events import KVCacheEventRecorder
from sglang.srt.runtime_context import describe_kv_events_publisher
from sglang.srt.server_args import ServerArgs
from sglang.srt.utils.network import get_free_port
from sglang.test.ci.ci_register import register_cpu_ci
from sglang.test.test_utils import CustomTestCase

register_cpu_ci(est_time=12, suite="base-a-test-cpu")

WIRE_GOLDEN = (
    Path(__file__).resolve().parents[4]
    / "rust"
    / "sglang-kv-events"
    / "testdata"
    / "wire_golden.json"
)
KV_EVENT_TYPES = {
    event_type.__name__: event_type
    for event_type in (BlockStored, BlockRemoved, AllBlocksCleared)
}


def create_zmq_publisher(test, *, rust, attn_dp_rank=0, **config):
    with envs.SGLANG_USE_RUST_KV_EVENT_PUBLISHER.override(rust):
        publisher = EventPublisherFactory.create(
            json.dumps({"publisher": "zmq", **config}), attn_dp_rank=attn_dp_rank
        )
    atexit.unregister(publisher.shutdown)
    test.addCleanup(publisher.shutdown)
    return publisher


class TestLocalKvEventSource(CustomTestCase):
    def test_advertised_source_delivers_events(self):
        cases = (
            (False, "*", None, "kv"),
            (False, "127.0.0.1", True, ""),
            (True, "*", None, "kv"),
            (True, "127.0.0.1", True, ""),
        )
        for rust, host, bind, topic in cases:
            with self.subTest(rust=rust, host=host):
                with zmq.Context.instance().socket(zmq.PUB) as probe:
                    port = probe.bind_to_random_port("tcp://*")
                publisher = create_zmq_publisher(
                    self,
                    rust=rust,
                    attn_dp_rank=4,
                    endpoint=f"tcp://{host}:{port - 4}",
                    bind=bind,
                    topic=topic,
                )
                self.assertIsInstance(
                    publisher, RustZmqEventPublisher if rust else ZmqEventPublisher
                )
                source = publisher.describe_local_source(64)
                self.assertEqual(
                    source,
                    dict(
                        dp_rank=4,
                        endpoint=f"tcp://127.0.0.1:{port}",
                        topic=topic,
                        block_size=64,
                    ),
                )
                self._assert_event_received(publisher, source)

    def test_rust_publisher_leaves_unsupported_endpoints_to_python(self):
        # It only binds, has no inproc transport and does not resolve host
        # names; selecting it there would turn a working config into a startup
        # failure or a bind on another address.
        for config in (
            dict(endpoint="inproc://kv-fallback-test"),
            dict(endpoint=f"tcp://127.0.0.1:{get_free_port()}"),
            dict(endpoint=f"tcp://localhost:{get_free_port()}", bind=True),
            dict(
                endpoint=f"tcp://*:{get_free_port()}",
                replay_endpoint="inproc://kv-fallback-replay",
            ),
        ):
            with self.subTest(config=config):
                publisher = create_zmq_publisher(self, rust=True, **config)
                self.assertIsInstance(publisher, ZmqEventPublisher)

    def _assert_event_received(self, publisher, source):
        with zmq.Context() as context, context.socket(zmq.SUB) as subscriber:
            subscriber.setsockopt_string(zmq.SUBSCRIBE, source["topic"])
            subscriber.connect(source["endpoint"])
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                publisher.publish(KVEventBatch(ts=1.0, events=[AllBlocksCleared()]))
                if subscriber.poll(100):
                    topic, _, payload = subscriber.recv_multipart()
                    self.assertEqual(topic, source["topic"].encode())
                    batch = msgspec.msgpack.decode(payload, type=KVEventBatch)
                    self.assertEqual(batch.attn_dp_rank, source["dp_rank"])
                    self.assertEqual(batch.events, [AllBlocksCleared()])
                    break
            else:
                self.fail("No event received from the advertised local source")


class _RecorderOnlyCache:
    """The event-draining surface of a prefix cache."""

    def __init__(self):
        self.kv_events = KVCacheEventRecorder(enabled=True, page_size=1)

    take_events = BasePrefixCache.take_events
    take_encoded_events = BasePrefixCache.take_encoded_events
    supports_kv_event_resync = BasePrefixCache.supports_kv_event_resync
    take_resync_encoded_events = BasePrefixCache.take_resync_encoded_events


class _TreeRecorder(KVCacheEventRecorder):
    """A recorder that can republish its tree, as the Rust core's can."""

    supports_resync = True

    def take_resync_encoded(self, *, ts, attn_dp_rank):
        self.take()
        return [b"tree-0", b"tree-1"]


class _ResyncableCache(_RecorderOnlyCache):
    def __init__(self):
        self.kv_events = _TreeRecorder(enabled=True, page_size=1)


class TestSchedulerPublishing(CustomTestCase):
    def _component(self, config, *, rust=False, cache=None):
        parallel = SimpleNamespace(
            pp_rank=0,
            attn_tp_rank=0,
            attn_cp_rank=0,
            attn_dp_size=1,
            attn_dp_rank=0,
            dp_rank=2,
        )
        with (
            patch.object(kv_events, "get_parallel", return_value=parallel),
            patch.object(scheduler_publisher, "get_parallel", return_value=parallel),
            envs.SGLANG_USE_RUST_KV_EVENT_PUBLISHER.override(rust),
        ):
            component = scheduler_publisher.SchedulerKvEventsPublisher(
                kv_events_config=json.dumps(config),
                tree_cache=cache or _RecorderOnlyCache(),
                send_metrics_from_scheduler=None,
                max_running_requests=1,
                max_total_num_tokens=1,
                get_stats=lambda: None,
            )
        atexit.unregister(component.kv_event_publisher.shutdown)
        self.addCleanup(component.kv_event_publisher.shutdown)
        return component

    def _published_payload(self, *, rust):
        with zmq.Context.instance().socket(zmq.PUB) as probe:
            port = probe.bind_to_random_port("tcp://*")
        component = self._component(
            {"publisher": "zmq", "endpoint": f"tcp://*:{port - 2}"}, rust=rust
        )
        self.assertEqual(component.publishes_encoded_batches, rust)
        with (
            zmq.Context() as context,
            context.socket(zmq.SUB) as subscriber,
            patch.object(scheduler_publisher.time, "time", return_value=1.5),
        ):
            subscriber.setsockopt_string(zmq.SUBSCRIBE, "")
            subscriber.connect(f"tcp://127.0.0.1:{port}")
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                component.tree_cache.kv_events.record_all_cleared()
                component.publish_kv_events()
                if subscriber.poll(100):
                    return subscriber.recv_multipart()[2]
        self.fail("No batch reached the subscriber")

    def test_both_backends_publish_the_same_payload(self):
        # The Rust publisher is handed bytes the cache recorder encoded; they
        # must be the batch the Python publisher would have encoded itself,
        # rank included.
        python_payload = self._published_payload(rust=False)
        self.assertEqual(self._published_payload(rust=True), python_payload)
        batch = msgspec.msgpack.decode(python_payload, type=KVEventBatch)
        self.assertEqual(batch, KVEventBatch(1.5, [AllBlocksCleared()], 2))

    def test_only_the_grpc_publisher_has_a_relay_source(self):
        # The Rust server starts a relay for exactly this entry, so a "zmq"
        # launch must not produce one and a "grpc" launch must name the rank,
        # the block size and the hash format the relay reports.
        zmq_only = self._component(
            {"publisher": "zmq", "endpoint": f"tcp://*:{get_free_port()}"}
        )
        self.assertIsNone(zmq_only.kv_event_relay_source(16))
        self.assertEqual(zmq_only.kv_event_streams(16), [])
        relayed = self._component({"publisher": "grpc", "format": "dynamo"})
        source = relayed.kv_event_relay_source(16)
        self.assertTrue(source.pop("endpoint").startswith("ipc://"))
        self.assertTrue(source.pop("replay_endpoint").startswith("ipc://"))
        # This cache only records events, so it cannot republish its tree.
        self.assertEqual(
            source,
            dict(
                topic="", dp_rank=2, block_size=16, hash_format="dynamo", resync=False
            ),
        )
        self.assertEqual(
            relayed.kv_event_streams(16),
            [dict(dp_rank=2, block_size=16, hash_format="dynamo")],
        )
        # Without compat_zmq no socket is offered to ZMQ consumers.
        self.assertEqual(relayed.local_kv_event_sources(16), [])

    def _stream_after(self, replay_endpoint, request=None):
        """Send `request`, then return every payload the publisher retains.
        Requests are handled in order, so the replay shows the state after it."""
        with zmq.Context.instance().socket(zmq.DEALER) as dealer:
            dealer.setsockopt(zmq.RCVTIMEO, 5000)
            dealer.setsockopt(zmq.LINGER, 0)
            dealer.connect(replay_endpoint)
            if request is not None:
                dealer.send_multipart([b"", request])
            dealer.send_multipart([b"", (0).to_bytes(8, "big")])
            payloads = []
            while True:
                _, _, seq, payload = dealer.recv_multipart()
                if int.from_bytes(seq, "big", signed=True) == -1:
                    return payloads
                payloads.append(payload)

    def test_only_the_relay_can_have_the_tree_published_again(self):
        # A resync walks the whole tree on the scheduler thread. The relay asks
        # for one on its private socket after losing batches; a ZMQ consumer of
        # the compat sockets must not be able to trigger it.
        component = self._component(
            {
                "publisher": "grpc",
                "format": "dynamo",
                "compat_zmq": True,
                "endpoint": f"tcp://*:{get_free_port()}",
                "replay_endpoint": f"tcp://*:{get_free_port()}",
            },
            cache=_ResyncableCache(),
        )
        relay = component.kv_event_relay_source(16)
        self.assertTrue(relay["resync"])
        compat = component.local_kv_event_sources(16)[0]
        cleared = encode_kv_event_batch([AllBlocksCleared()], ts=1.5, attn_dp_rank=2)

        with patch.object(scheduler_publisher.time, "time", return_value=1.5):
            component.tree_cache.kv_events.record_all_cleared()
            self._stream_after(compat["replay_endpoint"], b"resync")
            component.publish_kv_events()
            stream = self._stream_after(relay["replay_endpoint"], b"resync")
            self.assertEqual(stream, [cleared])

            # The tree replaces the queued event, which it already shows.
            component.tree_cache.kv_events.record_all_cleared()
            component.publish_kv_events()
            component.publish_kv_events()
        for source in (relay, compat):
            stream = self._stream_after(source["replay_endpoint"])
            self.assertEqual(stream, [cleared, b"tree-0", b"tree-1"])


class TestGrpcPublisher(CustomTestCase):
    def _publisher(self, **config):
        publisher = EventPublisherFactory.create(
            json.dumps({"publisher": "grpc", "format": "dynamo", **config})
        )
        atexit.unregister(publisher.shutdown)
        self.addCleanup(publisher.shutdown)
        return publisher

    def _replay(self, endpoint):
        dealer = zmq.Context.instance().socket(zmq.DEALER)
        dealer.setsockopt(zmq.RCVTIMEO, 5000)
        dealer.connect(endpoint)
        self.addCleanup(dealer.close, linger=0)
        dealer.send_multipart([b"", (0).to_bytes(8, "big")])
        batches = []
        while True:
            _, _, seq, payload = dealer.recv_multipart()
            seq = int.from_bytes(seq, "big", signed=True)
            if seq == -1:
                return batches
            batches.append((seq, payload))

    def test_relay_and_compat_sockets_carry_the_same_stream(self):
        # A ZMQ consumer and a consumer of the relayed gRPC stream must agree
        # on every sequence number, or a cursor from one is wrong for the other.
        port = get_free_port()
        publisher = self._publisher(
            compat_zmq=True,
            endpoint=f"tcp://*:{port}",
            replay_endpoint=f"tcp://*:{get_free_port()}",
        )
        for block_hash in range(3):
            removed = BlockRemoved(block_hashes=[block_hash])
            publisher.publish(KVEventBatch(ts=1.0, events=[removed]))
        compat = publisher.describe_local_source(16)
        self.assertEqual(compat["endpoint"], f"tcp://127.0.0.1:{port}")
        relayed = self._replay(publisher.describe_relay_source(16)["replay_endpoint"])
        self.assertEqual([seq for seq, _ in relayed], [0, 1, 2])
        self.assertEqual(self._replay(compat["replay_endpoint"]), relayed)

    def test_private_sockets_are_not_advertised_and_do_not_outlive_shutdown(self):
        def server_info_block(**config):
            config = json.dumps({"publisher": "grpc", "format": "dynamo", **config})
            args = ServerArgs(model_path="dummy", page_size=16, kv_events_config=config)
            return describe_kv_events_publisher(args)

        self.assertIsNone(server_info_block())
        # With compat_zmq the block is what a "zmq" launch advertises.
        self.assertEqual(
            server_info_block(compat_zmq=True),
            describe_kv_events_publisher(
                ServerArgs(
                    model_path="dummy",
                    page_size=16,
                    kv_events_config='{"publisher": "zmq"}',
                )
            ),
        )

        publisher = self._publisher()
        self.assertIsNone(publisher.describe_local_source(16))
        socket_path = publisher.describe_relay_source(16)["endpoint"][len("ipc://") :]
        self.assertTrue(os.path.exists(socket_path))
        publisher.shutdown()
        self.assertFalse(os.path.exists(os.path.dirname(socket_path)))

    def test_grpc_publisher_requirements_are_checked_at_startup(self):
        def check(*, rust_server, grpc_port, **config):
            config = json.dumps({"publisher": "grpc", "format": "dynamo", **config})
            args = ServerArgs(
                model_path="dummy", kv_events_config=config, grpc_port=grpc_port
            )
            with envs.SGLANG_RUST_SERVER.override(rust_server):
                check_kv_events_grpc_args(args)

        check(rust_server=True, grpc_port=30001)
        # Other publishers are not served by the Rust server.
        check(rust_server=False, grpc_port=None, publisher="zmq", format="default")
        for missing, kwargs in (
            ("SGLANG_RUST_SERVER", dict(rust_server=False, grpc_port=30001)),
            ("--grpc-port", dict(rust_server=True, grpc_port=None)),
            ("dynamo", dict(rust_server=True, grpc_port=30001, format="default")),
            (
                "compat_zmq",
                dict(
                    rust_server=True,
                    grpc_port=30001,
                    compat_zmq=True,
                    endpoint="inproc://kv",
                ),
            ),
        ):
            with self.subTest(missing=missing):
                with self.assertRaisesRegex(ValueError, missing):
                    check(**kwargs)


class TestOffsetEndpointPort(CustomTestCase):
    def test_ipc_endpoint_gets_a_dp_suffix(self):
        self.assertEqual(
            ZmqEventPublisher.offset_endpoint_port("ipc:///tmp/kv_events", 2),
            "ipc:///tmp/kv_events_dp2",
        )


class TestShutdown(CustomTestCase):
    def test_full_queue_does_not_skip_socket_cleanup(self):
        with patch.object(threading.Thread, "start"):
            publisher = ZmqEventPublisher(
                attn_dp_rank=0, endpoint="inproc://kv-shutdown-test", max_queue_size=1
            )
        atexit.unregister(publisher.shutdown)
        publisher.publish(KVEventBatch(ts=1.0, events=[]))
        publisher.shutdown()
        self.assertTrue(publisher._pub.closed)


class TestResolveLoadPubRange(CustomTestCase):
    """The single source of truth both the bind and /server_info route through."""

    def test_reason_is_set_only_for_actionable_declines(self):
        # Off by default is unremarkable (no reason); an opt-in the operator
        # asked for that can't resolve is worth surfacing.
        _, quiet = resolve_load_pub_range(
            kv_endpoint="tcp://10.0.0.5:5557", replay_endpoint=None, dp_size=1
        )
        self.assertIsNone(quiet)
        _, auto_loud = resolve_load_pub_range(
            kv_endpoint="tcp://10.0.0.5:5557",  # connect-style: can't derive
            replay_endpoint=None,
            dp_size=1,
            load_publish_endpoint="auto",
        )
        self.assertIsNotNone(auto_loud)
        # A missing config surfaces at startup — no message may render a bare
        # "None". Both the likely mistakes (auto and an explicit address
        # without --kv-events-config) go through this.
        for endpoint in ("auto", "tcp://*:7000"):
            with self.subTest(endpoint=endpoint):
                _, no_cfg = resolve_load_pub_range(
                    kv_endpoint=None,
                    replay_endpoint=None,
                    dp_size=1,
                    load_publish_endpoint=endpoint,
                )
                self.assertIsNotNone(no_cfg)
                self.assertNotIn("None", no_cfg)
                self.assertIn("--kv-events-config", no_cfg)


class TestBlockStoredWireFormat(CustomTestCase):
    def _event(self, **extra):
        return BlockStored(
            block_hashes=[123],
            parent_block_hash=None,
            token_ids=[1, 2],
            block_size=2,
            lora_id=None,
            medium=StorageMedium.GPU,
            **extra,
        )

    def test_one_decoder_reads_a_mixed_batch(self):
        batch = KVEventBatch(
            ts=1.0,
            events=[
                self._event(),
                self._event(cache_salt="tenant-a"),
                self._event(session_id="session-a"),
                BlockRemoved(block_hashes=[123], medium=StorageMedium.GPU),
                AllBlocksCleared(),
            ],
        )
        round_tripped = msgspec.msgpack.decode(
            msgspec.msgpack.encode(batch), type=KVEventBatch
        )
        stored = round_tripped.events[:3]
        self.assertEqual([e.cache_salt for e in stored], [None, "tenant-a", None])
        self.assertEqual([e.session_id for e in stored], [None, None, "session-a"])
        self.assertIsInstance(round_tripped.events[3], BlockRemoved)
        self.assertIsInstance(round_tripped.events[4], AllBlocksCleared)

    def test_msgspec_bytes_match_the_rust_codec_fixture(self):
        # rust/sglang-kv-events must encode the same bytes (tests/wire_golden.rs),
        # so a schema change here needs the fixture and the Rust codec updated.
        cases = json.loads(WIRE_GOLDEN.read_text())["cases"]
        for case in cases:
            with self.subTest(case=case["name"]):
                events = []
                for fields in case["batch"]["events"]:
                    fields = dict(fields)
                    events.append(KV_EVENT_TYPES[fields.pop("type")](**fields))
                batch = KVEventBatch(
                    ts=case["batch"]["ts"],
                    events=events,
                    attn_dp_rank=case["batch"]["attn_dp_rank"],
                )
                self.assertEqual(
                    msgspec.msgpack.encode(batch).hex(), case["msgpack_hex"]
                )


class TestNamespacedBlockHash(CustomTestCase):
    def test_values_are_pinned(self):
        # Rust has its own copy of this function, and its test uses the same
        # values. Thus the two copies must give the same hashes, and the hashes
        # must not change.
        seed = kv_event_namespace_seed(extra_key="lora-a", cache_salt=None)
        self.assertEqual(
            namespaced_block_hash(123, namespace_seed=seed), 9220659119954863560
        )
        self.assertEqual(
            namespaced_block_hash(-5, namespace_seed=seed), -1810974732006142875
        )


class TestReplay(CustomTestCase):
    def test_descriptor_advertises_replay_port(self):
        def descriptor(**cfg):
            cfg = msgspec.json.encode({"publisher": "zmq", **cfg}).decode()
            args = ServerArgs(model_path="dummy", page_size=16, kv_events_config=cfg)
            return describe_kv_events_publisher(args)

        self.assertNotIn("replay_endpoint_port_base", descriptor())
        self.assertEqual(
            descriptor(replay_endpoint="tcp://*:6000")["replay_endpoint_port_base"],
            6000,
        )

    def test_router_serves_buffered_batches_then_end_seq(self):
        # The router's DEALER client relies on this exact framing.
        for rust in (False, True):
            with self.subTest(rust=rust):
                replay = f"tcp://127.0.0.1:{get_free_port()}"
                publisher = create_zmq_publisher(
                    self,
                    rust=rust,
                    # The Rust publisher has no inproc transport.
                    endpoint=(
                        f"tcp://127.0.0.1:{get_free_port()}"
                        if rust
                        else "inproc://kv-replay-test"
                    ),
                    bind=True,
                    replay_endpoint=replay,
                    topic="kv@prefill@model",
                )
                dealer = zmq.Context.instance().socket(zmq.DEALER)
                dealer.setsockopt(zmq.RCVTIMEO, 5000)
                dealer.connect(replay)
                try:
                    for _ in range(3):
                        publisher.publish(
                            KVEventBatch(ts=1.0, events=[AllBlocksCleared()])
                        )
                    if not rust:
                        # The Rust publisher orders a replay after every
                        # batch published before the request.
                        publisher._event_queue.join()
                    dealer.send_multipart([b"", (1).to_bytes(8, "big")])
                    frames = [dealer.recv_multipart() for _ in range(3)]
                finally:
                    dealer.close(linger=0)
                    publisher.shutdown()
                self.assertEqual([len(f) for f in frames], [4, 4, 4])
                seqs = [int.from_bytes(f[2], "big", signed=True) for f in frames]
                self.assertEqual(seqs, [1, 2, -1])
                self.assertEqual([f[0] for f in frames], [b""] * 3)
                self.assertEqual(
                    [f[1] for f in frames], [b"kv@prefill@model"] * 2 + [b""]
                )
                for frame in frames[:2]:
                    batch = msgspec.msgpack.decode(frame[3], type=KVEventBatch)
                    self.assertEqual(batch.events, [AllBlocksCleared()])
                    self.assertEqual(batch.attn_dp_rank, 0)
                self.assertEqual(frames[2][3], b"")


if __name__ == "__main__":
    unittest.main()
