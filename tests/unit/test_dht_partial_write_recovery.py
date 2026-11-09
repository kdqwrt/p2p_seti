
import pytest

from p2p.dht.records import (
    create_node_record,
    node_record_key,
)
from p2p.dht.storage import (
    DHTStorage,
    StorageError,
    RecordRollbackError,
    RecordConflictError,
)
from p2p.identity.identity import load_or_create_identity


@pytest.mark.asyncio
async def test_version_survives_record_write_failure(
    tmp_path,
    monkeypatch,
):
    identity = load_or_create_identity(
        str(tmp_path / "identity")
    )

    directory = tmp_path / "storage"
    storage = DHTStorage(directory)

    record19 = create_node_record(
        identity=identity,
        addresses=["127.0.0.1:9101"],
        sequence_number=19,
        ttl_seconds=300,
    )

    record20 = create_node_record(
        identity=identity,
        addresses=["127.0.0.1:9102"],
        sequence_number=20,
        ttl_seconds=300,
    )

    key = node_record_key(identity.node_id)

    await storage.put(record19)

    def failing_write_record(key, record):
        raise StorageError("Simulated record write failure")

    monkeypatch.setattr(
        storage,
        "_write_record",
        failing_write_record,
    )

    # Журнал версии 20 успевает сохраниться,
    # но запись NodeRecord не сохраняется.
    with pytest.raises(StorageError):
        await storage.put(record20)

    assert storage._versions[key][0] == 20

    # Создаём новое хранилище — имитация перезапуска.
    restarted = DHTStorage(directory)
    await restarted.load()

    assert restarted._versions[key][0] == 20

    # Версия 19 не должна снова стать активной.
    assert await restarted.get(key) is None

    with pytest.raises(RecordRollbackError):
        await restarted.put(record19)

    # Версию 20 можно повторно отправить.
    await restarted.put(record20)

    assert await restarted.get(key) == record20


@pytest.mark.asyncio
async def test_conflicting_record_rejected_after_partial_write(
    tmp_path,
    monkeypatch,
):
    identity = load_or_create_identity(
        str(tmp_path / "identity")
    )

    directory = tmp_path / "storage"
    storage = DHTStorage(directory)

    original = create_node_record(
        identity=identity,
        addresses=["127.0.0.1:9101"],
        sequence_number=20,
        ttl_seconds=300,
    )

    conflicting = create_node_record(
        identity=identity,
        addresses=["127.0.0.1:9102"],
        sequence_number=20,
        ttl_seconds=300,
    )

    def failing_write_record(key, record):
        raise StorageError("Simulated record write failure")

    monkeypatch.setattr(
        storage,
        "_write_record",
        failing_write_record,
    )

    with pytest.raises(StorageError):
        await storage.put(original)

    restarted = DHTStorage(directory)
    await restarted.load()

    # Даже без NodeRecord журнал должен помнить
    # digest первоначальной версии.
    with pytest.raises(RecordConflictError):
        await restarted.put(conflicting)

    # Оригинальная версия по-прежнему допустима.
    await restarted.put(original)

    key = node_record_key(identity.node_id)
    assert await restarted.get(key) == original
