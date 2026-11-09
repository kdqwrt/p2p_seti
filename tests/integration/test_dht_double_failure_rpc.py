
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


async def wait_until(predicate, timeout=10.0):
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        if await predicate():
            return

        await asyncio.sleep(0.05)

    pytest.fail(
        f"Expected condition not reached in {timeout}s"
    )


@pytest.mark.asyncio
async def test_automatic_repair_after_two_replica_failures(tmp_path):
    """
    Шесть реальных TCP-узлов.

    Три основные реплики получают запись.
    Две основные реплики отключаются.
    Два резервных узла должны получить запись
    автоматически через фоновое обслуживание.
    """

    ports = set()

    while len(ports) < 6:
        ports.add(free_port())

    ports = list(ports)

    nodes = [
        Node(
            make_config(
                tmp_path / f"node{i}",
                ports[i],
            )
        )
        for i in range(6)
    ]

    publisher = nodes[0]
    replicas = nodes[1:]

    # Настройка ДО Node.start().
    publisher._maintenance_interval = 0.05

    started = []

    try:
        # 1. Запуск всех шести TCP-серверов.
        for node in nodes:
            await node.start()
            started.append(node)

        # 2. Издатель обнаруживает пять соседей.
        for replica in replicas:
            await publisher.ping(
                host=replica.local_contact.host,
                port=replica.local_contact.port,
            )

        assert publisher.routing_table.size() == 5

        # 3. Создаём подписанную запись.
        record = create_node_record(
            identity=publisher.identity,
            addresses=[
                f"127.0.0.1:{ports[0]}"
            ],
            sequence_number=1,
            ttl_seconds=180,
        )

        key = node_record_key(record.node_id)

        # 4. Определяем основные и резервные узлы.
        nearest = sorted(
            replicas,
            key=lambda node: xor_distance(
                node.identity.node_id,
                key,
            ),
        )

        primary_replicas = nearest[:3]
        reserve_replicas = nearest[3:]

        assert len(primary_replicas) == 3
        assert len(reserve_replicas) == 2

        # 5. Публикация через штатный алгоритм.
        result = await publisher.publish_record(record)

        assert result.successful is True
        assert result.fully_replicated is True
        assert result.confirmed == 3

        for replica in primary_replicas:
            stored = await replica.dht_storage.get(key)
            assert stored == record

        # Резервные узлы изначально пустые.
        for replica in reserve_replicas:
            stored = await replica.dht_storage.get(key)
            assert stored is None

        # 6. Отключаем две основные реплики.
        failed_replicas = primary_replicas[:2]

        for replica in failed_replicas:
            await replica.stop()
            started.remove(replica)

        # 7. Проверяем реальную недоступность TCP.
        for replica in failed_replicas:
            with pytest.raises(
                (
                    ConnectionError,
                    OSError,
                    asyncio.TimeoutError,
                )
            ):
                await publisher.ping(
                    host=replica.local_contact.host,
                    port=replica.local_contact.port,
                    expected_node_id=replica.identity.node_id,
                )

        # 8. Две резервные реплики должны получить
        # запись БЕЗ ручного вызова обслуживания.
        async def both_reserves_have_record():
            for replica in reserve_replicas:
                stored = await replica.dht_storage.get(key)

                if stored != record:
                    return False

            return True

        await wait_until(
            both_reserves_have_record,
            timeout=10.0,
        )

        # 9. Проверяем три работающие реплики.
        surviving_primary = primary_replicas[2]

        available_replicas = [
            surviving_primary,
            *reserve_replicas,
        ]

        assert len(available_replicas) == 3

        for replica in available_replicas:
            # Фактическое состояние DHT.
            stored = await replica.dht_storage.get(key)
            assert stored == record

            # Чтение через настоящий TCP.
            received = await publisher.find_value(
                replica.local_contact,
                key,
            )

            assert received == record

        # 10. Издатель сохранил свою публикацию.
        published = await publisher.published_registry.get(key)
        assert published == record

        # 11. Фоновая задача продолжает работать.
        assert publisher._maintenance_task is not None
        assert not publisher._maintenance_task.done()

    finally:
        # Гарантированная остановка работающих узлов.
        for node in reversed(started):
            await node.stop()
