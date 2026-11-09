
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


def make_config(state_dir, port, bootstrap_peers=None):
    return NodeConfig(
        node_state_dir=str(state_dir),
        listen_host="127.0.0.1",
        listen_port=port,
        bootstrap_peers=bootstrap_peers or [],
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
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        if await predicate():
            return

        await asyncio.sleep(0.05)

    pytest.fail(
        f"Condition was not satisfied within {timeout} seconds"
    )


@pytest.mark.asyncio
async def test_replica_repair_after_publisher_restart(tmp_path):
    """
    Проверяет восстановление репликации после
    полного перезапуска издателя.

    Используются настоящие TCP-серверы.
    Сетевые методы и алгоритмы DHT не подменяются.
    """

    ports = set()

    while len(ports) < 5:
        ports.add(free_port())

    ports = list(ports)

    state_dirs = [
        tmp_path / f"node{i}"
        for i in range(5)
    ]

    nodes = [
        Node(make_config(state_dirs[i], ports[i]))
        for i in range(5)
    ]

    publisher = nodes[0]
    replicas = nodes[1:]

    # Интервал задаётся ДО запуска фоновой задачи.
    publisher._maintenance_interval = 0.05

    started = []

    try:
        # 1. Запускаем сеть.
        for node in nodes:
            await node.start()
            started.append(node)

        # Издатель обнаруживает четыре узла.
        for replica in replicas:
            await publisher.ping(
                host=replica.local_contact.host,
                port=replica.local_contact.port,
            )

        assert publisher.routing_table.size() == 4

        # 2. Публикуем подписанную запись.
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

        result = await publisher.publish_record(record)

        assert result.successful is True
        assert result.fully_replicated is True
        assert result.confirmed == 3

        for replica in primary_replicas:
            assert (
                await replica.dht_storage.get(key)
                == record
            )

        assert (
            await reserve_replica.dht_storage.get(key)
            is None
        )

        original_node_id = publisher.identity.node_id
        original_public_key = publisher.identity.public_key

        # 3. Полностью останавливаем издателя.
        await publisher.stop()
        started.remove(publisher)

        # 4. Пока издатель выключен,
        # отключаем одну основную реплику.
        failed_replica = primary_replicas[0]

        await failed_replica.stop()
        started.remove(failed_replica)

        surviving_replicas = [
            replica
            for replica in primary_replicas
            if replica is not failed_replica
        ]

        # Проверяем, что резервная копия
        # ещё не появилась.
        assert (
            await reserve_replica.dht_storage.get(key)
            is None
        )

        # 5. Создаём НОВЫЙ экземпляр издателя
        # с тем же каталогом состояния.
        #
        # Bootstrap-адреса задаются через конфигурацию,
        # а не прямым изменением routing_table.
        available_nodes = (
            surviving_replicas + [reserve_replica]
        )

        bootstrap_peers = [
            {
                "host": node.local_contact.host,
                "port": node.local_contact.port,
            }
            for node in available_nodes
        ]

        restarted_publisher = Node(
            make_config(
                state_dirs[0],
                ports[0],
                bootstrap_peers=bootstrap_peers,
            )
        )

        restarted_publisher._maintenance_interval = 0.05

        # 6. Проверяем сохранение идентичности.
        assert (
            restarted_publisher.identity.node_id
            == original_node_id
        )

        assert (
            restarted_publisher.identity.public_key
            == original_public_key
        )

        # 7. Запускаем издателя повторно.
        # Node.start() должен самостоятельно:
        # - загрузить реестр;
        # - подключиться к bootstrap-узлам;
        # - запустить фоновое обслуживание.
        await restarted_publisher.start()
        started.append(restarted_publisher)

        # 8. Проверяем восстановление публикации.
        published = await (
            restarted_publisher.published_registry.get(key)
        )

        assert published == record

        active_records = await (
            restarted_publisher.published_registry.list_active()
        )

        assert record in active_records

        # 9. Проверяем, что узел обнаружил
        # соседей после перезапуска.
        assert (
            restarted_publisher.routing_table.size()
            >= 3
        )

        # 10. Не вызываем обслуживание вручную.
        # Ждём, пока фоновый механизм сам
        # восстановит недостающую реплику.
        async def reserve_has_record():
            stored = await reserve_replica.dht_storage.get(key)
            return stored == record

        await wait_until(
            reserve_has_record,
            timeout=8.0,
        )

        # 11. Проверяем три доступные копии.
        available_replicas = (
            surviving_replicas + [reserve_replica]
        )

        assert len(available_replicas) == 3

        for replica in available_replicas:
            stored = await replica.dht_storage.get(key)

            assert stored == record

            received = await restarted_publisher.find_value(
                replica.local_contact,
                key,
            )

            assert received == record

        # 12. Фоновая задача должна
        # продолжать работать.
        assert (
            restarted_publisher._maintenance_task
            is not None
        )

        assert not (
            restarted_publisher._maintenance_task.done()
        )

    finally:
        for node in reversed(started):
            await node.stop()
