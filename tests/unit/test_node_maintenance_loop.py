
import asyncio
import socket
from types import SimpleNamespace

import pytest

from p2p.config import NodeConfig
from p2p.node.node import Node
from p2p.dht.records import create_node_record


def free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def make_config(tmp_path):
    return NodeConfig(
        node_state_dir=str(tmp_path / "node"),
        listen_host="127.0.0.1",
        listen_port=free_port(),
        bootstrap_peers=[],
        node_id_bits=256,
        k_bucket_size=8,
        alpha=3,
        connect_timeout_ms=1000,
        read_timeout_ms=2000,
        ping_timeout_ms=1000,
        max_frame_payload=65536,
        protocol_version=1,
        log_level="WARNING",
        log_file=str(tmp_path / "node.log"),
    )


@pytest.mark.asyncio
async def test_maintenance_task_starts_and_stops(tmp_path):
    node = Node(make_config(tmp_path))

    try:
        await node.start()

        assert node._maintenance_task is not None
        assert not node._maintenance_task.done()

    finally:
        await node.stop()

    assert node._maintenance_task is None


@pytest.mark.asyncio
async def test_maintenance_loop_runs_periodically(
    tmp_path,
    monkeypatch,
):
    node = Node(make_config(tmp_path))

    node._maintenance_interval = 0.01

    called = asyncio.Event()
    calls = 0

    async def fake_maintain():
        nonlocal calls
        calls += 1

        if calls >= 2:
            called.set()

    monkeypatch.setattr(
        node,
        "maintain_published_records",
        fake_maintain,
    )

    try:
        await node.start()

        await asyncio.wait_for(
            called.wait(),
            timeout=2.0,
        )

        assert calls >= 2

    finally:
        await node.stop()


@pytest.mark.asyncio
async def test_maintenance_does_not_overlap(
    tmp_path,
    monkeypatch,
):
    """Одновременно может выполняться только один цикл."""
    node = Node(make_config(tmp_path))

    record = create_node_record(
        identity=node.identity,
        addresses=["127.0.0.1:9101"],
        sequence_number=1,
    )

    async def fake_cleanup():
        return 0

    async def fake_list_active():
        return [record]

    monkeypatch.setattr(
        node.published_registry,
        "cleanup",
        fake_cleanup,
    )

    monkeypatch.setattr(
        node.published_registry,
        "list_active",
        fake_list_active,
    )

    active = 0
    max_active = 0
    calls = 0

    async def fake_lookup(*args, **kwargs):
        return SimpleNamespace(
            closest_contacts=[]
        )

    async def fake_maintain(record, contacts):
        nonlocal active, max_active, calls

        active += 1
        calls += 1

        max_active = max(max_active, active)

        try:
            await asyncio.sleep(0.03)

            return SimpleNamespace(
                checked=0,
                existing=0,
                repaired=0,
                confirmed=0,
                successful=False,
                fully_replicated=False,
            )

        finally:
            active -= 1

    monkeypatch.setattr(
        node,
        "lookup_node",
        fake_lookup,
    )

    monkeypatch.setattr(
        node.replica_maintenance,
        "maintain",
        fake_maintain,
    )

    await asyncio.gather(
        node.maintain_published_records(),
        node.maintain_published_records(),
        node.maintain_published_records(),
    )

    assert calls == 3
    assert max_active == 1
    assert active == 0


@pytest.mark.asyncio
async def test_maintenance_skips_foreign_records(
    tmp_path,
    monkeypatch,
):
    """Узел не обслуживает записи чужого владельца."""
    node = Node(make_config(tmp_path))

    foreign_config = make_config(
        tmp_path / "foreign"
    )
    foreign_node = Node(foreign_config)

    foreign_record = create_node_record(
        identity=foreign_node.identity,
        addresses=["127.0.0.1:9201"],
        sequence_number=1,
    )

    async def fake_cleanup():
        return 0

    async def fake_list_active():
        return [foreign_record]

    async def unexpected_lookup(*args, **kwargs):
        pytest.fail(
            "Foreign record must not trigger lookup"
        )

    monkeypatch.setattr(
        node.published_registry,
        "cleanup",
        fake_cleanup,
    )

    monkeypatch.setattr(
        node.published_registry,
        "list_active",
        fake_list_active,
    )

    monkeypatch.setattr(
        node,
        "lookup_node",
        unexpected_lookup,
    )

    await node.maintain_published_records()


@pytest.mark.asyncio
async def test_maintenance_cancellation(
    tmp_path,
    monkeypatch,
):
    """Остановка узла отменяет активный цикл обслуживания."""
    node = Node(make_config(tmp_path))

    node._maintenance_interval = 0.01

    entered = asyncio.Event()
    cancelled = asyncio.Event()

    async def slow_maintain():
        entered.set()

        try:
            await asyncio.sleep(3600)

        except asyncio.CancelledError:
            cancelled.set()
            raise

    monkeypatch.setattr(
        node,
        "maintain_published_records",
        slow_maintain,
    )

    try:
        await node.start()

        await asyncio.wait_for(
            entered.wait(),
            timeout=2.0,
        )

    finally:
        await asyncio.wait_for(
            node.stop(),
            timeout=2.0,
        )

    assert cancelled.is_set()
    assert node._maintenance_task is None
