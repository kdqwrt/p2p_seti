
import os

import pytest

from p2p.dht.records import (
    create_node_record,
    node_record_key,
)
from p2p.dht.storage import DHTStorage, StorageError
from p2p.identity.identity import load_or_create_identity


@pytest.mark.asyncio
async def test_version_write_calls_fsync(tmp_path, monkeypatch):
    identity = load_or_create_identity(
        str(tmp_path / "identity")
    )

    storage = DHTStorage(tmp_path / "storage")

    record = create_node_record(
        identity=identity,
        addresses=["127.0.0.1:9101"],
        sequence_number=10,
        ttl_seconds=300,
    )

    calls = []

    real_fsync = os.fsync

    def tracked_fsync(fd):
        calls.append(fd)
        return real_fsync(fd)

    monkeypatch.setattr(os, "fsync", tracked_fsync)

    await storage.put(record)

    assert len(calls) >= 1

    key = node_record_key(record.node_id)
    assert storage._version_path(key).exists()


@pytest.mark.asyncio
async def test_fsync_failure_rejects_store(tmp_path, monkeypatch):
    identity = load_or_create_identity(
        str(tmp_path / "identity")
    )

    storage = DHTStorage(tmp_path / "storage")

    record = create_node_record(
        identity=identity,
        addresses=["127.0.0.1:9101"],
        sequence_number=10,
        ttl_seconds=300,
    )

    def failing_fsync(fd):
        raise OSError("Simulated disk sync failure")

    monkeypatch.setattr(os, "fsync", failing_fsync)

    with pytest.raises(StorageError):
        await storage.put(record)

    key = node_record_key(record.node_id)

    assert await storage.get(key) is None
    assert key not in storage._versions
    assert not storage._version_path(key).exists()
