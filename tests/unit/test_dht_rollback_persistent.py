
import time

import pytest

from p2p.dht.records import (
    create_node_record,
    node_record_key,
)
from p2p.dht.storage import (
    DHTStorage,
    RecordRollbackError,
    RecordConflictError,
)
from p2p.identity.identity import load_or_create_identity


@pytest.mark.asyncio
async def test_rollback_after_ttl_cleanup(tmp_path):
    identity = load_or_create_identity(
        str(tmp_path / "identity")
    )
    storage = DHTStorage(tmp_path / "storage")

    now = int(time.time())

    old_record = create_node_record(
        identity=identity,
        addresses=["127.0.0.1:9101"],
        sequence_number=5,
        ttl_seconds=60,
        now=now,
    )

    new_record = create_node_record(
        identity=identity,
        addresses=["127.0.0.1:9102"],
        sequence_number=6,
        ttl_seconds=60,
        now=now,
    )

    key = node_record_key(identity.node_id)

    await storage.put(old_record, now=now)
    await storage.put(new_record, now=now)

    assert await storage.cleanup(now=now + 61) == 1
    assert await storage.get(key, now=now + 61) is None

    # Проверяем rollback в момент, когда старая запись
    # ещё проходит проверку срока действия.
    with pytest.raises(RecordRollbackError):
        await storage.put(old_record, now=now)


@pytest.mark.asyncio
async def test_rollback_after_restart(tmp_path):
    identity = load_or_create_identity(
        str(tmp_path / "identity")
    )

    directory = tmp_path / "storage"
    now = int(time.time())

    record5 = create_node_record(
        identity=identity,
        addresses=["127.0.0.1:9101"],
        sequence_number=5,
        now=now,
    )

    record6 = create_node_record(
        identity=identity,
        addresses=["127.0.0.1:9102"],
        sequence_number=6,
        now=now,
    )

    storage = DHTStorage(directory)
    await storage.put(record5, now=now)
    await storage.put(record6, now=now)

    restarted = DHTStorage(directory)
    await restarted.load()

    with pytest.raises(RecordRollbackError):
        await restarted.put(record5, now=now)


@pytest.mark.asyncio
async def test_same_sequence_different_content_rejected(tmp_path):
    identity = load_or_create_identity(
        str(tmp_path / "identity")
    )
    storage = DHTStorage(tmp_path / "storage")

    now = int(time.time())

    first = create_node_record(
        identity=identity,
        addresses=["127.0.0.1:9101"],
        sequence_number=10,
        now=now,
    )

    conflicting = create_node_record(
        identity=identity,
        addresses=["127.0.0.1:9102"],
        sequence_number=10,
        now=now,
    )

    await storage.put(first, now=now)

    with pytest.raises(RecordConflictError):
        await storage.put(conflicting, now=now)
