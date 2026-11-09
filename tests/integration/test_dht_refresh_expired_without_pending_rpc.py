
import asyncio
import time

import pytest

from p2p.node.node import Node
from p2p.dht.records import (
    create_node_record,
    node_record_key,
    validate_node_record,
)

from test_dht_store_error_codes_rpc import free_port, make_config


@pytest.mark.asyncio
async def test_expired_record_without_pending_recovers_after_restart(
    tmp_path,
):
    ports = []

    while len(ports) < 3:
        port = free_port()
        if port not in ports:
            ports.append(port)

    owner_config = make_config(tmp_path / "owner", ports[0])

    owner = Node(owner_config)
    replica1 = Node(make_config(tmp_path / "replica1", ports[1]))
    replica2 = Node(make_config(tmp_path / "replica2", ports[2]))

    started = []

    try:
        await owner.start()
        started.append(owner)

        # Создаём действительную запись с коротким TTL.
        old_record = create_node_record(
            identity=owner.identity,
            addresses=[f"127.0.0.1:{ports[0]}"],
            sequence_number=1,
            ttl_seconds=3,
        )

        key = node_record_key(old_record.node_id)

        assert await owner.published_registry.add(old_record)

        # Pending отсутствует.
        assert await owner.published_registry.get_pending(key) is None

        # Останавливаем узел до истечения TTL.
        await owner.stop()
        started.remove(owner)

        # Ждём истечения срока действия записи.
        delay = max(
            0.0,
            old_record.expires_at - time.time() + 1.1,
        )
        await asyncio.sleep(delay)

        # Перезапускаем владельца с прежней identity.
        restarted = Node(owner_config)

        assert restarted.identity.node_id == owner.identity.node_id

        await restarted.start()
        started.append(restarted)

        # История сохранилась, но запись уже не действительна.
        stored = await restarted.published_registry.get_stored(key)

        assert stored == old_record
        assert await restarted.published_registry.get(key) is None
        assert await restarted.published_registry.get_pending(key) is None

        # Запускаем две реплики.
        await replica1.start()
        started.append(replica1)

        await replica2.start()
        started.append(replica2)

        for replica in (replica1, replica2):
            await restarted.ping(
                host=replica.local_contact.host,
                port=replica.local_contact.port,
                expected_node_id=replica.identity.node_id,
            )

        # Запускаем восстановление публикации.
        await restarted.maintain_published_records()

        current = await restarted.published_registry.get(key)

        assert current is not None
        assert current.sequence_number == 2
        assert current.node_id == old_record.node_id
        assert current != old_record

        validate_node_record(current)

        # Проверяем реальные TCP-реплики.
        stored1 = await restarted.find_value(
            replica1.local_contact,
            key,
        )

        stored2 = await restarted.find_value(
            replica2.local_contact,
            key,
        )

        assert stored1 == current
        assert stored2 == current

        # Незавершённых публикаций не осталось.
        assert await restarted.published_registry.get_pending(key) is None

    finally:
        for node in reversed(started):
            await node.stop()
