
import time

import pytest

from p2p.dht.records import (
    create_node_record,
    node_record_key,
)
from p2p.dht.storage import DHTStorage
from p2p.node.node import Node
from test_dht_store_error_codes_rpc import make_config


@pytest.mark.asyncio
async def test_expired_record_is_removed_on_read(tmp_path):
    """
    Просроченная запись не возвращается и удаляется.
    """
    storage = DHTStorage(tmp_path / "storage")

    owner = Node(
        make_config(tmp_path / "owner", 19031)
    )

    now = int(time.time())

    record = create_node_record(
        identity=owner.identity,
        addresses=["127.0.0.1:19031"],
        sequence_number=1,
        ttl_seconds=180,
    )
    key = node_record_key(record.node_id)

    await storage.put(record, now=now)

    assert await storage.get(key, now=now) == record
    assert await storage.count() == 1

    # Переносим логическое время за окончание TTL.
    future = record.expires_at + 1

    assert await storage.get(key, now=future) is None
    assert await storage.count() == 0

    assert not (storage.directory / f"{key.hex()}.json").exists()


@pytest.mark.asyncio
async def test_cleanup_removes_expired_records(tmp_path):
    """
    Фоновая очистка удаляет просроченную запись.
    """
    storage = DHTStorage(tmp_path / "storage")

    owner = Node(
        make_config(tmp_path / "owner", 19032)
    )

    record = create_node_record(
        identity=owner.identity,
        addresses=["127.0.0.1:19032"],
        sequence_number=1,
        ttl_seconds=180,
    )
    key = node_record_key(record.node_id)

    await storage.put(record)

    assert await storage.count() == 1

    removed = await storage.cleanup(
        now=record.expires_at + 1
    )

    assert removed == 1
    assert await storage.count() == 0
    assert await storage.get(key) is None

    # Повторная очистка ничего не удаляет.
    assert await storage.cleanup(
        now=record.expires_at + 1
    ) == 0


@pytest.mark.asyncio
async def test_expired_record_not_restored_after_restart(tmp_path):
    """
    После истечения TTL запись не должна восстановиться
    из файлового хранилища.
    """
    state_dir = tmp_path / "storage"
    storage = DHTStorage(state_dir)

    owner = Node(
        make_config(tmp_path / "owner", 19033)
    )

    record = create_node_record(
        identity=owner.identity,
        addresses=["127.0.0.1:19033"],
        sequence_number=1,
        ttl_seconds=180,
    )
    key = node_record_key(record.node_id)

    await storage.put(record)

    assert await storage.get(key) == record

    # Создаём новое хранилище с тем же каталогом.
    restarted_storage = DHTStorage(state_dir)

    # Пока TTL действует, запись восстанавливается.
    assert await restarted_storage.load() == 1
    assert await restarted_storage.get(key) == record

    # Проверяем поведение после истечения TTL.
    assert (
        await restarted_storage.get(
            key,
            now=record.expires_at + 1,
        )
        is None
    )

    assert await restarted_storage.count() == 0
