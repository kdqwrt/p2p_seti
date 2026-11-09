
import asyncio
import socket
import time

import pytest

from p2p.config import NodeConfig
from p2p.node.node import Node
from p2p.dht.records import (
    create_node_record,
    node_record_key,
)


def free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def make_config(state_dir, port):
    return NodeConfig(
        node_state_dir=str(state_dir),
        listen_host="127.0.0.1",
        listen_port=port,
        bootstrap_peers=[],
        node_id_bits=256,
        k_bucket_size=8,
        alpha=3,
        connect_timeout_ms=300,
        read_timeout_ms=1000,
        ping_timeout_ms=300,
        max_frame_payload=65536,
        protocol_version=1,
        log_level="WARNING",
        log_file=str(state_dir / "node.log"),
    )


@pytest.mark.asyncio
async def test_ttl_expiration_and_republication(tmp_path):
    """
    Проверяет:
    - истечение TTL;
    - недоступность записи через TCP;
    - удаление просроченных копий;
    - отсутствие восстановления старой записи;
    - публикацию новой версии.
    """

    ports = set()

    while len(ports) < 4:
        ports.add(free_port())

    ports = list(ports)

    nodes = [
        Node(
            make_config(
                tmp_path / f"node{i}",
                ports[i],
            )
        )
        for i in range(4)
    ]

    publisher = nodes[0]
    replicas = nodes[1:]
    started = []

    try:
        # 1. Запускаем настоящие TCP-узлы.
        for node in nodes:
            await node.start()
            started.append(node)

        for replica in replicas:
            await publisher.ping(
                host=replica.local_contact.host,
                port=replica.local_contact.port,
            )

        assert publisher.routing_table.size() == 3

        # 2. Публикуем запись с TTL = 3 секунды.
        old_record = create_node_record(
            identity=publisher.identity,
            addresses=[f"127.0.0.1:{ports[0]}"],
            sequence_number=1,
            ttl_seconds=3,
        )

        key = node_record_key(old_record.node_id)

        result = await publisher.publish_record(old_record)

        assert result.successful is True
        assert result.fully_replicated is True
        assert result.confirmed == 3

        print("\nINITIAL PUBLICATION: 3 replicas")

        # 3. Проверяем начальное состояние.
        for replica in replicas:
            stored = await replica.dht_storage.get(key)
            assert stored == old_record

            received = await publisher.find_value(
                replica.local_contact,
                key,
            )
            assert received == old_record

        registered = await publisher.published_registry.get(key)
        assert registered == old_record

        # 4. Ждём фактического истечения TTL.
        # Используем expires_at, а не произвольный sleep.
        delay = max(
            0.0,
            old_record.expires_at - time.time() + 1.1,
        )

        await asyncio.sleep(delay)

        assert time.time() >= old_record.expires_at

        print("\nTTL EXPIRED")

        # 5. Просроченная запись не должна
        # возвращаться через FIND_VALUE.
        for replica in replicas:
            received = await publisher.find_value(
                replica.local_contact,
                key,
            )
            assert received is None

        print("EXPIRED RECORD: not accessible via TCP")

        # 6. Очищаем DHT-хранилища.
        for replica in replicas:
            await replica.dht_storage.cleanup()

            stored = await replica.dht_storage.get(key)
            assert stored is None

            assert await replica.dht_storage.count() == 0

            # Проверяем удаление файла с диска.
            path = (
                replica.dht_storage.directory
                / f"{key.hex()}.json"
            )
            assert not path.exists()

        print("EXPIRED REPLICAS: removed")

        # 7. Проверяем реестр издателя.
        await publisher.published_registry.cleanup()

        active_records = (
            await publisher.published_registry.list_active()
        )

        assert old_record not in active_records


        await publisher.maintain_published_records()

        refreshed = await publisher.published_registry.get(key)

        assert refreshed is not None
        assert refreshed.node_id == old_record.node_id
        assert refreshed.sequence_number == old_record.sequence_number + 1
        assert refreshed.expires_at > int(time.time())
        assert refreshed != old_record

        # Проверяем, что на репликах появилась именно
        # новая подписанная версия, а не просроченная.
        for replica in replicas:
            received = await publisher.find_value(
                replica.local_contact,
                key,
            )

            assert received == refreshed
            assert received != old_record

            stored = await replica.dht_storage.get(key)
            assert stored == refreshed

        print("MAINTENANCE: new record published automatically")


        new_record = create_node_record(
            identity=publisher.identity,
            addresses=[f"127.0.0.1:{ports[0]}"],
            sequence_number=refreshed.sequence_number + 1,
            ttl_seconds=180,
        )

        assert new_record.sequence_number == 3
        assert new_record.sequence_number > refreshed.sequence_number
        assert new_record.expires_at > refreshed.expires_at

        assert new_record.sequence_number > old_record.sequence_number
        assert new_record.expires_at > old_record.expires_at

        new_key = node_record_key(new_record.node_id)

        assert new_key == key

        # 10. Повторная публикация.
        result = await publisher.publish_record(new_record)

        assert result.successful is True
        assert result.fully_replicated is True
        assert result.confirmed == 3

        # 11. Проверяем новые копии через TCP.
        for replica in replicas:
            stored = await replica.dht_storage.get(key)
            assert stored == new_record

            received = await publisher.find_value(
                replica.local_contact,
                key,
            )
            assert received == new_record

        # 12. Реестр содержит новую публикацию.
        registered = await publisher.published_registry.get(key)

        assert registered == new_record

        print("REPUBLICATION: 3 new replicas available")

    finally:
        for node in reversed(started):
            await node.stop()
