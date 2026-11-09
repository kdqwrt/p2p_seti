
import time

import pytest

from p2p.dht.published_registry import (
    PublishedRecordRegistry,
)
from p2p.dht.records import (
    create_node_record,
    node_record_key,
)
from p2p.identity.identity import (
    load_or_create_identity,
)


@pytest.fixture
def registry_data(tmp_path):
    identity = load_or_create_identity(
        str(tmp_path / "identity")
    )

    state_dir = str(tmp_path / "state")

    record = create_node_record(
        identity=identity,
        addresses=["127.0.0.1:9101"],
        sequence_number=1,
        ttl_seconds=180,
    )

    return identity, state_dir, record


@pytest.mark.asyncio
async def test_add_and_get(registry_data):
    _, state_dir, record = registry_data

    registry = PublishedRecordRegistry(state_dir)

    assert await registry.add(record) is True

    key = node_record_key(record.node_id)

    assert await registry.get(key) == record


@pytest.mark.asyncio
async def test_load_after_restart(registry_data):
    _, state_dir, record = registry_data

    first = PublishedRecordRegistry(state_dir)

    await first.add(record)

    second = PublishedRecordRegistry(state_dir)

    assert await second.load() == 1

    key = node_record_key(record.node_id)

    assert await second.get(key) == record


@pytest.mark.asyncio
async def test_reject_older_sequence(registry_data):
    identity, state_dir, record = registry_data

    registry = PublishedRecordRegistry(state_dir)

    newer = create_node_record(
        identity=identity,
        addresses=["127.0.0.1:9102"],
        sequence_number=2,
    )

    assert await registry.add(newer) is True
    assert await registry.add(record) is False

    key = node_record_key(record.node_id)

    assert await registry.get(key) == newer


@pytest.mark.asyncio
async def test_reject_conflicting_same_sequence(
    registry_data,
):
    identity, state_dir, record = registry_data

    registry = PublishedRecordRegistry(state_dir)

    conflicting = create_node_record(
        identity=identity,
        addresses=["127.0.0.1:9999"],
        sequence_number=1,
    )

    assert await registry.add(record) is True

    assert await registry.add(conflicting) is False


@pytest.mark.asyncio
async def test_list_active(registry_data):
    _, state_dir, record = registry_data

    registry = PublishedRecordRegistry(state_dir)

    await registry.add(record)

    active = await registry.list_active()

    assert len(active) == 1
    assert active[0] == record


@pytest.mark.asyncio
async def test_cleanup_expired_record(
    registry_data,
):
    identity, state_dir, _ = registry_data

    registry = PublishedRecordRegistry(state_dir)

    now = int(time.time())

    record = create_node_record(
        identity=identity,
        addresses=["127.0.0.1:9101"],
        sequence_number=1,
        ttl_seconds=1,
        now=now - 5,
    )

    # Просроченная запись не должна приниматься.
    with pytest.raises(ValueError):
        await registry.add(record)

    assert await registry.cleanup() == 0
    assert await registry.list_active() == []


@pytest.mark.asyncio
async def test_empty_registry(tmp_path):
    registry = PublishedRecordRegistry(
        str(tmp_path / "empty")
    )

    assert await registry.load() == 0
    assert await registry.list_active() == []
    assert await registry.cleanup() == 0
