
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
from p2p.routing.distance import xor_distance


def free_port():
    with socket.socket() as sock:
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


async def wait_until(predicate, timeout=8.0):
    """Ждёт выполнения условия, не изменяя работу узлов."""
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        if await predicate():
            return

        await asyncio.sleep(0.05)

    pytest.fail(
        f"Condition was not satisfied within {timeout} seconds"
    )


@pytest.mark.asyncio
async def test_automatic_replica_repair_over_real_tcp(tmp_path):
    """
    Проверяет восстановление реплики через настоящую сеть.

    Никакие сетевые методы и алгоритмы DHT не подменяются.
    """

    ports = set()

    while len(ports) < 5:
        ports.add(free_port())

    ports = list(ports)

    nodes = [
        Node(
            make_config(
                tmp_path / f"node{i}",
                ports[i],
            )
        )
        for i in range(5)
    ]


    nodes[0]._maintenance_interval = 0.05

    started = []

    try:
        # 1. Запускаем настоящие TCP-серверы.
        for node in nodes:
            await node.start()
            started.append(node)

        publisher = nodes[0]
        replicas = nodes[1:]



        # 2. Издатель обнаруживает четыре узла.
        for replica in replicas:
            await publisher.ping(
                host=replica.local_contact.host,
                port=replica.local_contact.port,
            )

        assert publisher.routing_table.size() == 4

        # 3. Создаём настоящую подписанную запись.
        record = create_node_record(
            identity=publisher.identity,
            addresses=[
                f"127.0.0.1:{ports[0]}"
            ],
            sequence_number=1,
            ttl_seconds=180,
        )

        key = node_record_key(record.node_id)

        nearest = sorted(
            replicas,
            key=lambda node: xor_distance(
                node.identity.node_id,
                key,
            ),
        )

        primary_replicas = nearest[:3]
        reserve_replica = nearest[3]

        # 4. Публикуем запись штатным методом.
        result = await publisher.publish_record(record)

        assert result.successful is True
        assert result.fully_replicated is True
        assert result.confirmed == 3

        # Проверяем фактическое состояние хранилищ.
        for replica in primary_replicas:
            stored = await replica.dht_storage.get(key)
            assert stored == record

        # Резервный узел пока не должен иметь запись.
        assert await reserve_replica.dht_storage.get(key) is None

        # 5. Отключаем одну из основных реплик.
        failed_replica = primary_replicas[0]

        await failed_replica.stop()
        started.remove(failed_replica)

        # Проверяем, что TCP-соединение действительно
        # больше не устанавливается.
        with pytest.raises(
            (ConnectionError, OSError, asyncio.TimeoutError)
        ):
            await publisher.ping(
                host=failed_replica.local_contact.host,
                port=failed_replica.local_contact.port,
                expected_node_id=failed_replica.identity.node_id,
            )

        # 6. Ждём АВТОМАТИЧЕСКОГО восстановления.
        # maintain_published_records() вручную НЕ вызываем.
        async def reserve_has_record():
            stored = await reserve_replica.dht_storage.get(key)
            return stored == record

        await wait_until(
            reserve_has_record,
            timeout=8.0,
        )

        # 7. Проверяем все три доступные реплики.
        surviving_replicas = [
            replica
            for replica in primary_replicas
            if replica is not failed_replica
        ]

        available_replicas = (
            surviving_replicas + [reserve_replica]
        )

        assert len(available_replicas) == 3

        for replica in available_replicas:
            stored = await replica.dht_storage.get(key)
            assert stored == record

            # Проверяем и получение через настоящий TCP.
            received = await publisher.find_value(
                replica.local_contact,
                key,
            )

            assert received == record

        # 8. Запись должна оставаться в реестре издателя.
        published = await publisher.published_registry.get(key)

        assert published == record

    finally:
        for node in reversed(started):
            await node.stop()
