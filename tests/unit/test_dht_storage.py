
from dataclasses import replace

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
)

from p2p.dht.records import (
    RecordError,
    create_node_record,
    node_record_key,
)
from p2p.dht.storage import (
    DHTStorage,
    RecordConflictError,
    RecordRollbackError,
)
from p2p.identity.identity import NodeIdentity


@pytest.fixture
def identity():
    return NodeIdentity.from_private_key(
        Ed25519PrivateKey.generate()
    )


def make_record(identity, sequence=1, now=1000, ttl=180):
    return create_node_record(
        identity,
        addresses=["127.0.0.1:9101"],
        sequence_number=sequence,
        ttl_seconds=ttl,
        now=now,
    )


@pytest.mark.asyncio
async def test_store_and_read(tmp_path, identity):
    storage = DHTStorage(tmp_path)
    record = make_record(identity)

    key = await storage.put(record, now=1000)
    result = await storage.get(key, now=1000)

    assert result == record
    assert key == node_record_key(identity.node_id)


@pytest.mark.asyncio
async def test_missing_record(tmp_path):
    storage = DHTStorage(tmp_path)

    result = await storage.get(b"\x00" * 32)

    assert result is None


@pytest.mark.asyncio
async def test_newer_version_replaces_old(tmp_path, identity):
    storage = DHTStorage(tmp_path)

    first = make_record(identity, sequence=1)
    second = make_record(identity, sequence=2)

    await storage.put(first, now=1000)
    await storage.put(second, now=1000)

    result = await storage.get(
        node_record_key(identity.node_id),
        now=1000,
    )

    assert result.sequence_number == 2


@pytest.mark.asyncio
async def test_rollback_rejected(tmp_path, identity):
    storage = DHTStorage(tmp_path)

    newer = make_record(identity, sequence=5)
    older = make_record(identity, sequence=4)

    await storage.put(newer, now=1000)

    with pytest.raises(RecordRollbackError):
        await storage.put(older, now=1000)


@pytest.mark.asyncio
async def test_same_version_same_record_allowed(tmp_path, identity):
    storage = DHTStorage(tmp_path)
    record = make_record(identity)

    await storage.put(record, now=1000)
    await storage.put(record, now=1000)

    assert await storage.count() == 1


@pytest.mark.asyncio
async def test_same_version_different_record_rejected(
    tmp_path,
    identity,
):
    storage = DHTStorage(tmp_path)

    first = make_record(identity, sequence=1, now=1000)
    second = create_node_record(
        identity,
        addresses=["127.0.0.1:9102"],
        sequence_number=1,
        now=1000,
    )

    await storage.put(first, now=1000)

    with pytest.raises(RecordConflictError):
        await storage.put(second, now=1000)


@pytest.mark.asyncio
async def test_expired_record_removed(tmp_path, identity):
    storage = DHTStorage(tmp_path)

    record = make_record(identity, ttl=120)

    key = await storage.put(record, now=1000)

    result = await storage.get(key, now=1120)

    assert result is None
    assert await storage.count() == 0


@pytest.mark.asyncio
async def test_cleanup_removes_expired(tmp_path, identity):
    storage = DHTStorage(tmp_path)

    record = make_record(identity, ttl=120)

    await storage.put(record, now=1000)

    removed = await storage.cleanup(now=1120)

    assert removed == 1
    assert await storage.count() == 0


@pytest.mark.asyncio
async def test_persistence_after_restart(tmp_path, identity):
    # Используем реальное время, чтобы запись была
    # действительна при загрузке с диска.
    import time

    now = int(time.time())

    first_storage = DHTStorage(tmp_path)
    record = make_record(identity, now=now)

    key = await first_storage.put(record, now=now)

    second_storage = DHTStorage(tmp_path)
    loaded = await second_storage.load()

    assert loaded == 1
    assert await second_storage.get(key) == record


@pytest.mark.asyncio
async def test_invalid_signature_rejected(tmp_path, identity):
    storage = DHTStorage(tmp_path)

    record = make_record(identity)
    invalid = replace(record, signature=b"\x00" * 64)

    with pytest.raises(RecordError):
        await storage.put(invalid, now=1000)


@pytest.mark.asyncio
async def test_expired_record_not_stored(tmp_path, identity):
    storage = DHTStorage(tmp_path)

    record = make_record(identity, ttl=120)

    with pytest.raises(RecordError):
        await storage.put(record, now=1120)


@pytest.mark.asyncio
async def test_storage_files_created(tmp_path, identity):
    storage = DHTStorage(tmp_path)
    record = make_record(identity)

    key = await storage.put(record, now=1000)

    path = tmp_path / "dht" / f"{key.hex()}.json"

    assert path.exists()
