
import time

import pytest

from p2p.node.node import Node
from p2p.dht.records import (
    create_node_record,
    node_record_key,
    validate_node_record,
)
from p2p.dht.published_registry import PublishedRecordRegistry

from test_dht_store_error_codes_rpc import (
    free_port,
    make_config,
)


@pytest.mark.asyncio
async def test_pending_refresh_survives_owner_restart(tmp_path):
    """
    Незавершённая публикация переживает перезапуск
    владельца и завершается без изменения подписи.
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

        old_record = create_node_record(
            identity=owner.identity,
            addresses=[f"127.0.0.1:{ports[0]}"],
            sequence_number=1,
            ttl_seconds=180,
            now=int(time.time()) - 100,
        )

        key = node_record_key(old_record.node_id)

        assert await owner.published_registry.add(
            old_record
        )

        # Первая попытка: доступен только один хранитель.
        assert (
            await owner.refresh_published_record(old_record)
            is False
        )

        pending_before = (
            await owner.published_registry.get_pending(key)
        )

        assert pending_before is not None
        assert pending_before.sequence_number == 2

        validate_node_record(pending_before)

        stored_before = await owner.find_value(
            replica1.local_contact,
            key,
        )

        assert stored_before == pending_before

        pending_bytes = pending_before.to_bytes()

        # Останавливаем владельца.
        await owner.stop()
        started.remove(owner)

        # Проверяем, что данные остались на диске.
        disk_registry = PublishedRecordRegistry(
            owner_config.node_state_dir
        )

        await disk_registry.load()

        assert await disk_registry.get(key) == old_record

        pending_on_disk = await disk_registry.get_pending(key)

        assert pending_on_disk is not None
        assert pending_on_disk.to_bytes() == pending_bytes

        # Создаём новый объект Node с тем же каталогом.
        restarted_owner = Node(owner_config)

        assert (
            restarted_owner.identity.node_id
            == owner.identity.node_id
        )

        await restarted_owner.start()
        started.append(restarted_owner)

        # Запись должна восстановиться после start().
        restored_old = (
            await restarted_owner.published_registry.get(key)
        )

        restored_pending = (
            await restarted_owner.published_registry.get_pending(
                key
            )
        )

        assert restored_old == old_record
        assert restored_pending is not None
        assert restored_pending.to_bytes() == pending_bytes

        # Подключаем второго хранителя.
        await replica2.start()
        started.append(replica2)

        await restarted_owner.ping(
            host=replica1.local_contact.host,
            port=replica1.local_contact.port,
            expected_node_id=replica1.identity.node_id,
        )

        await restarted_owner.ping(
            host=replica2.local_contact.host,
            port=replica2.local_contact.port,
            expected_node_id=replica2.identity.node_id,
        )

        # Завершаем ранее начатую публикацию.
        assert (
            await restarted_owner.refresh_published_record(
                old_record
            )
            is True
        )

        current = (
            await restarted_owner.published_registry.get(key)
        )

        assert current is not None
        assert current.sequence_number == 2
        assert current.to_bytes() == pending_bytes

        validate_node_record(current)

        # Проверяем обе реплики через реальные TCP RPC.
        stored1 = await restarted_owner.find_value(
            replica1.local_contact,
            key,
        )

        stored2 = await restarted_owner.find_value(
            replica2.local_contact,
            key,
        )

        assert stored1 == current
        assert stored2 == current

        assert stored1.to_bytes() == pending_bytes
        assert stored2.to_bytes() == pending_bytes

        # После подтверждения pending больше не нужен.
        assert (
            await restarted_owner.published_registry.get_pending(
                key
            )
            is None
        )

    finally:
        for node in reversed(started):
            await node.stop()
