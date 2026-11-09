
import os

import pytest

from p2p.dht.records import (
    create_node_record,
    node_record_key,
)
from p2p.dht.storage import (
    DHTStorage,
    StorageError,
    RecordRollbackError,
)
from p2p.identity.identity import load_or_create_identity


@pytest.fixture
def environment(tmp_path):
    identity = load_or_create_identity(
        str(tmp_path / "identity")
    )

    directory = tmp_path / "storage"

    def make_record(sequence, port):
        return create_node_record(
            identity=identity,
            addresses=[f"127.0.0.1:{port}"],
            sequence_number=sequence,
            ttl_seconds=300,
        )

    return directory, identity, make_record


@pytest.mark.asyncio
async def test_failed_version_replace_preserves_old_version(
    environment,
    monkeypatch,
):
    directory, identity, make_record = environment

    storage = DHTStorage(directory)

    record10 = make_record(10, 9101)
    record11 = make_record(11, 9102)

    key = node_record_key(identity.node_id)

    await storage.put(record10)

    original_replace = os.replace
    version_path = storage._version_path(key)

    def failing_replace(src, dst):
        if os.fspath(dst) == os.fspath(version_path):
            raise OSError("Simulated version replace failure")

        return original_replace(src, dst)

    monkeypatch.setattr(os, "replace", failing_replace)

    with pytest.raises(StorageError):
        await storage.put(record11)

    assert storage._versions[key][0] == 10
    assert await storage.get(key) == record10

    # Восстанавливаем обычную работу файловой системы.
    monkeypatch.setattr(os, "replace", original_replace)

    restarted = DHTStorage(directory)
    await restarted.load()

    assert restarted._versions[key][0] == 10
    assert await restarted.get(key) == record10

    await restarted.put(record11)

    assert await restarted.get(key) == record11


@pytest.mark.asyncio
async def test_failed_record_replace_preserves_version_guard(
    environment,
    monkeypatch,
):
    directory, identity, make_record = environment

    storage = DHTStorage(directory)

    record10 = make_record(10, 9101)
    record11 = make_record(11, 9102)

    key = node_record_key(identity.node_id)

    await storage.put(record10)

    original_replace = os.replace
    record_path = storage.directory / f"{key.hex()}.json"

    def failing_replace(src, dst):
        if os.fspath(dst) == os.fspath(record_path):
            raise OSError("Simulated record replace failure")

        return original_replace(src, dst)

    monkeypatch.setattr(os, "replace", failing_replace)

    with pytest.raises(StorageError):
        await storage.put(record11)

    assert storage._versions[key][0] == 11

    monkeypatch.setattr(os, "replace", original_replace)

    restarted = DHTStorage(directory)
    await restarted.load()

    assert restarted._versions[key][0] == 11

    # Версия 10 осталась на диске, но не должна
    # загружаться как действующая запись.
    assert await restarted.get(key) is None

    with pytest.raises(RecordRollbackError):
        await restarted.put(record10)

    await restarted.put(record11)

    assert await restarted.get(key) == record11


@pytest.mark.asyncio
async def test_orphan_tmp_file_does_not_override_journal(
    environment,
):
    directory, identity, make_record = environment

    storage = DHTStorage(directory)

    record10 = make_record(10, 9101)
    record11 = make_record(11, 9102)

    key = node_record_key(identity.node_id)

    await storage.put(record10)

    # Имитируем временный файл, оставшийся
    # после аварийного завершения.
    temporary = storage._version_path(key).with_suffix(".tmp")

    temporary.write_text(
        '{"sequence_number": 11, "digest": "' + "a" * 64 + '"}',
        encoding="utf-8",
    )

    restarted = DHTStorage(directory)
    await restarted.load()

    # Незавершённая операция не должна считаться
    # подтверждённой версией.
    assert restarted._versions[key][0] == 10
    assert await restarted.get(key) == record10

    # Новую версию можно сохранить штатно.
    await restarted.put(record11)

    assert restarted._versions[key][0] == 11
    assert await restarted.get(key) == record11


@pytest.mark.asyncio
async def test_orphan_record_tmp_is_ignored(
    environment,
):
    directory, identity, make_record = environment

    storage = DHTStorage(directory)

    record10 = make_record(10, 9101)

    key = node_record_key(identity.node_id)

    await storage.put(record10)

    temporary = (
        storage.directory / f"{key.hex()}.tmp"
    )

    temporary.write_text(
        "incomplete record",
        encoding="utf-8",
    )

    restarted = DHTStorage(directory)
    await restarted.load()

    assert await restarted.get(key) == record10
    assert restarted._versions[key][0] == 10
