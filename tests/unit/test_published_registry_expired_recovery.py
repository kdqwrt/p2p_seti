
import time

import pytest

from p2p.dht.published_registry import PublishedRecordRegistry
from p2p.dht.records import create_node_record, node_record_key
from p2p.identity.identity import load_or_create_identity


@pytest.mark.asyncio
async def test_expired_record_survives_cleanup_and_restart(
    tmp_path,
):
    state_dir = str(tmp_path / "owner")
    identity = load_or_create_identity(state_dir)

    registry = PublishedRecordRegistry(state_dir)

    record = create_node_record(
        identity=identity,
        addresses=["127.0.0.1:9101"],
        sequence_number=7,
        ttl_seconds=4,
        now=int(time.time()) - 5,
    )

    key = node_record_key(record.node_id)

    # Имитируем запись, которая была опубликована,
    # пока срок её действия ещё не истёк.
    registry.directory.mkdir(parents=True, exist_ok=True)
    registry._record_path(key).write_bytes(record.to_bytes())

    assert await registry.load() == 1

    assert await registry.get(key) is None
    assert await registry.get_stored(key) == record
    assert await registry.list_active() == []

    assert await registry.cleanup() == 0

    # Проверяем восстановление из файлов после перезапуска.
    restarted = PublishedRecordRegistry(state_dir)

    assert await restarted.load() == 1
    assert await restarted.get_stored(key) == record
    assert await restarted.get(key) is None
    assert await restarted.list_active() == []

    # История последней публикации не потеряна.
    assert (
        (await restarted.get_stored(key)).sequence_number
        == 7
    )
