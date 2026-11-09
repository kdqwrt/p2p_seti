
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
async def test_failed_refresh_preserves_last_record(tmp_path):
    """
    Если все хранители недоступны, переопубликование
    не должно уничтожать последнюю успешную версию.
    """

    owner_port = free_port()
    replica_port = free_port()

    while replica_port == owner_port:
        replica_port = free_port()

    owner = Node(
        make_config(tmp_path / "owner", owner_port)
    )

    replica = Node(
        make_config(tmp_path / "replica", replica_port)
    )

    try:
        await owner.start()
        await replica.start()

        # Устанавливаем реальное TCP-соединение.
        await owner.ping(
            host=replica.local_contact.host,
            port=replica.local_contact.port,
            expected_node_id=replica.identity.node_id,
        )

        # TTL = 180 секунд.
        # Запись выпущена 100 секунд назад.
        # Следовательно, половина TTL уже прошла,
        # но запись ещё действительна.
        now = int(time.time())

        old_record = create_node_record(
            identity=owner.identity,
            addresses=[f"127.0.0.1:{owner_port}"],
            sequence_number=1,
            ttl_seconds=180,
            now=now - 100,
        )

        validate_node_record(old_record)

        key = node_record_key(old_record.node_id)

        # Имитируем запись, ранее успешно опубликованную
        # и зарегистрированную владельцем.
        registered = await owner.published_registry.add(
            old_record
        )

        assert registered is True

        # Единственный известный хранитель отключается.
        await replica.stop()

        # Запускаем обновление.
        # Требуется минимум 2 подтверждения STORE.
        refreshed = await owner.refresh_published_record(
            old_record
        )

        assert refreshed is False

        # Последняя подтверждённая версия сохранилась.
        current = await owner.published_registry.get(key)

        assert current is not None
        assert current == old_record
        assert current.sequence_number == 1

        # Подпись и TTL остались действительными.
        validate_node_record(current)

        # Проверяем сохранность после загрузки с диска.
        from p2p.dht.published_registry import (
            PublishedRecordRegistry,
        )

        restored_registry = PublishedRecordRegistry(
            owner.config.node_state_dir
        )

        loaded = await restored_registry.load()

        assert loaded == 1

        restored = await restored_registry.get(key)

        assert restored == old_record

    finally:
        await replica.stop()
        await owner.stop()
