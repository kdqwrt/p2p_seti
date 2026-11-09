
import asyncio
import time

import pytest

from p2p.node.node import Node
from p2p.dht.records import (
    create_node_record,
    node_record_key,
    validate_node_record,
)

from test_dht_store_error_codes_rpc import (
    free_port,
    make_config,
)


@pytest.mark.asyncio
async def test_expired_pending_recovers_after_restart(tmp_path):
    """
    После истечения TTL владелец восстанавливает
    публикацию, не используя повторно sequence_number.
    Проверка реплик выполняется через реальные TCP RPC.
    """

    ports = []
    while len(ports) < 3:
        port = free_port()
        if port not in ports:
            ports.append(port)

    owner_config = make_config(
        tmp_path / "owner",
        ports[0],
    )

    owner = Node(owner_config)
    replica1 = Node(
        make_config(tmp_path / "replica1", ports[1])
    )
    replica2 = Node(
        make_config(tmp_path / "replica2", ports[2])
    )

    started = []

    try:
        await owner.start()
        started.append(owner)

        await replica1.start()
        started.append(replica1)

        await owner.ping(
            host=replica1.local_contact.host,
            port=replica1.local_contact.port,
            expected_node_id=replica1.identity.node_id,
        )

        # Исходная запись ещё действительна,
        # но уже достигла половины своего TTL.
        old_record = create_node_record(
            identity=owner.identity,
            addresses=[f"127.0.0.1:{ports[0]}"],
            sequence_number=1,
            ttl_seconds=4,
            now=int(time.time()) - 3,
        )

        key = node_record_key(old_record.node_id)

        assert await owner.published_registry.add(
            old_record
        )

        # Доступна только одна реплика.
        # Публикация версии 2 будет незавершённой.
        assert (
            await owner.refresh_published_record(
                old_record
            )
            is False
        )

        pending = await owner.published_registry.get_pending(
            key
        )

        assert pending is not None
        assert pending.sequence_number == 2

        # Убеждаемся, что версия 2 действительно
        # попала на первую реплику через TCP.
        stored = await owner.find_value(
            replica1.local_contact,
            key,
        )
        assert stored == pending

        # Останавливаем владельца до истечения TTL.
        await owner.stop()
        started.remove(owner)

        # Ждём истечения срока действия pending.
        delay = pending.expires_at - time.time() + 1.2
        if delay > 0:
            await asyncio.sleep(delay)

        # Создаём владельца заново с тем же ключом.
        restarted = Node(owner_config)
        assert (
            restarted.identity.node_id
            == owner.identity.node_id
        )

        await restarted.start()
        started.append(restarted)

        restored = await (
            restarted.published_registry.get_stored(key)
        )
        assert restored == old_record

        expired_pending = await (
            restarted.published_registry.get_pending(key)
        )
        assert expired_pending is not None
        assert expired_pending.sequence_number == 2
        assert expired_pending.expires_at <= int(time.time())

        # Теперь доступны две реплики.
        await replica2.start()
        started.append(replica2)

        for replica in (replica1, replica2):
            await restarted.ping(
                host=replica.local_contact.host,
                port=replica.local_contact.port,
                expected_node_id=replica.identity.node_id,
            )

        # Восстанавливаем публикацию через обычный
        # механизм автоматического обслуживания.
        await restarted.maintain_published_records()

        current = await (
            restarted.published_registry.get(key)
        )

        assert current is not None
        assert current.sequence_number == 3
        validate_node_record(current)

        # Обе реплики должны получить версию 3.
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

        # Незавершённая публикация больше не нужна.
        assert (
            await restarted.published_registry.get_pending(
                key
            )
            is None
        )

    finally:
        for node in reversed(started):
            await node.stop()
