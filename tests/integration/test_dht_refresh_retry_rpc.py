
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
async def test_partial_refresh_retries_same_signed_record(
    tmp_path,
):
    """
    Первый хранитель принимает новую версию.
    Второй подключается позднее.

    Повторная публикация должна использовать
    те же подписанные байты.
    """
    ports = []

    while len(ports) < 3:
        port = free_port()
        if port not in ports:
            ports.append(port)

    owner = Node(make_config(tmp_path / "owner", ports[0]))
    replica1 = Node(make_config(tmp_path / "replica1", ports[1]))
    replica2 = Node(make_config(tmp_path / "replica2", ports[2]))

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

        # Доступен только один хранитель.
        first_result = await owner.refresh_published_record(
            old_record
        )

        assert first_result is False

        pending = await owner.published_registry.get_pending(
            key
        )

        assert pending is not None
        assert pending.sequence_number == 2
        validate_node_record(pending)

        # Первый хранитель действительно принял запись.
        stored1 = await owner.find_value(
            replica1.local_contact,
            key,
        )

        assert stored1 == pending

        # Новая версия ещё не подтверждена двумя узлами.
        assert await owner.published_registry.get(key) == old_record

        # Теперь подключается второй хранитель.
        await replica2.start()
        started.append(replica2)

        await owner.ping(
            host=replica2.local_contact.host,
            port=replica2.local_contact.port,
            expected_node_id=replica2.identity.node_id,
        )

        second_result = await owner.refresh_published_record(
            old_record
        )

        assert second_result is True

        # Оба хранителя содержат одну подписанную версию.
        stored1_after = await owner.find_value(
            replica1.local_contact,
            key,
        )

        stored2 = await owner.find_value(
            replica2.local_contact,
            key,
        )

        assert stored1_after == pending
        assert stored2 == pending
        assert stored1_after.to_bytes() == stored2.to_bytes()

        # Реестр владельца переключился на версию 2.
        current = await owner.published_registry.get(key)

        assert current == pending
        assert current.sequence_number == 2

        # Незавершённая публикация удалена.
        assert (
            await owner.published_registry.get_pending(key)
            is None
        )

    finally:
        for node in reversed(started):
            await node.stop()
