
import json

import pytest

from p2p.dht.records import (
    create_node_record,
    node_record_key,
)
from p2p.dht.storage import DHTStorage, StorageError
from p2p.identity.identity import load_or_create_identity


@pytest.fixture
def journal_environment(tmp_path):
    identity = load_or_create_identity(
        str(tmp_path / "identity")
    )

    directory = tmp_path / "storage"

    record = create_node_record(
        identity=identity,
        addresses=["127.0.0.1:9101"],
        sequence_number=10,
        ttl_seconds=300,
    )

    key = node_record_key(record.node_id)

    return directory, record, key


@pytest.mark.asyncio
async def test_invalid_json_rejected(journal_environment):
    directory, record, key = journal_environment

    storage = DHTStorage(directory)
    await storage.put(record)

    path = storage._version_path(key)
    path.write_text("{invalid json", encoding="utf-8")

    with pytest.raises(StorageError):
        DHTStorage(directory)


@pytest.mark.asyncio
async def test_invalid_metadata_rejected(journal_environment):
    directory, record, key = journal_environment

    storage = DHTStorage(directory)
    await storage.put(record)

    path = storage._version_path(key)

    invalid_cases = [
        {},
        {"sequence_number": "10", "digest": "a" * 64},
        {"sequence_number": -1, "digest": "a" * 64},
        {"sequence_number": 2**64, "digest": "a" * 64},
        {"sequence_number": 10, "digest": "invalid"},
        {"sequence_number": 10, "digest": "Z" * 64},
        {"sequence_number": True, "digest": "a" * 64},
    ]

    for data in invalid_cases:
        path.write_text(
            json.dumps(data),
            encoding="utf-8",
        )

        with pytest.raises(StorageError):
            DHTStorage(directory)


@pytest.mark.asyncio
async def test_corruption_detected_on_reload(
    journal_environment,
):
    directory, record, key = journal_environment

    storage = DHTStorage(directory)
    await storage.put(record)

    path = storage._version_path(key)

    path.write_text(
        "corrupted",
        encoding="utf-8",
    )

    with pytest.raises(StorageError):
        await storage.load()


@pytest.mark.asyncio
async def test_valid_journal_survives_restart(
    journal_environment,
):
    directory, record, key = journal_environment

    storage = DHTStorage(directory)
    await storage.put(record)

    restarted = DHTStorage(directory)
    await restarted.load()

    assert restarted._versions[key][0] == 10
    assert await restarted.get(key) == record


@pytest.mark.asyncio
async def test_invalid_version_filename_rejected(tmp_path):
    directory = tmp_path / "storage"

    storage = DHTStorage(directory)

    invalid_path = (
        storage.versions_directory / "invalid_key.json"
    )

    invalid_path.write_text(
        json.dumps({
            "sequence_number": 10,
            "digest": "a" * 64,
        }),
        encoding="utf-8",
    )

    with pytest.raises(StorageError):
        DHTStorage(directory)
