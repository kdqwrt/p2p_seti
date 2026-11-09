
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
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
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


async def wait_until(predicate, timeout=10.0):
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        if await predicate():
            return

        await asyncio.sleep(0.05)

    pytest.fail(
        f"Condition not satisfied within {timeout} seconds"
    )


@pytest.mark.asyncio
async def test_repeated_failures_and_recovery(tmp_path):
    """
    Проверяет несколько циклов отказов и возвращений
    узлов через реальные TCP-соединения.

    Без monkeypatch и ручного запуска обслуживания.
    """

    ports = set()

    while len(ports) < 7:
        ports.add(free_port())

    ports = list(ports)

    state_dirs = [
        tmp_path / f"node{i}"
        for i in range(7)
    ]

    configs = [
        make_config(state_dirs[i], ports[i])
        for i in range(7)
    ]

    nodes = [Node(config) for config in configs]

    publisher = nodes[0]
    peers = nodes[1:]

    publisher._maintenance_interval = 0.05

    started = []

    try:
        # 1. Запускаем семь узлов.
        for node in nodes:
            await node.start()
            started.append(node)

        # 2. Формируем таблицу маршрутизации.
        for peer in peers:
            await publisher.ping(
                host=peer.local_contact.host,
                port=peer.local_contact.port,
            )

        assert publisher.routing_table.size() == 6

        # 3. Публикуем подписанную запись.
        record = create_node_record(
            identity=publisher.identity,
            addresses=[
                f"127.0.0.1:{ports[0]}"
            ],
            sequence_number=1,
            ttl_seconds=180,
        )

        key = node_record_key(record.node_id)

        result = await publisher.publish_record(record)

        assert result.successful is True
        assert result.fully_replicated is True
        assert result.confirmed == 3

        # Упорядочиваем узлы по XOR-расстоянию.
        nearest = sorted(
            peers,
            key=lambda node: xor_distance(
                node.identity.node_id,
                key,
            ),
        )

        primary_replicas = nearest[:3]

        # Сохраняем список работающих объектов.
        active_peers = list(peers)

        async def count_available_copies():
            holders = []

            for peer in active_peers:
                stored = await peer.dht_storage.get(key)

                if stored == record:
                    holders.append(peer)

            return holders

        async def assert_three_copies():
            async def enough_copies():
                holders = await count_available_copies()
                return len(holders) >= 3

            await wait_until(enough_copies)

            holders = await count_available_copies()

            assert len(holders) >= 3

            # Проверяем RPC-чтение всех найденных копий.
            for holder in holders:
                received = await publisher.find_value(
                    holder.local_contact,
                    key,
                )

                assert received == record

            assert publisher._maintenance_task is not None
            assert not publisher._maintenance_task.done()

        # 4. Проверяем исходную репликацию.
        for peer in primary_replicas:
            assert await peer.dht_storage.get(key) == record

        await assert_three_copies()

        # 5. Три последовательных цикла отказов.
        for cycle, failed_node in enumerate(
            primary_replicas,
            start=1,
        ):
            print(
                f"\nCycle {cycle}: stopping "
                f"{failed_node.identity.node_id.hex()[:12]}"
            )

            original_id = failed_node.identity.node_id

            # Останавливаем выбранную реплику.
            await failed_node.stop()
            started.remove(failed_node)
            active_peers.remove(failed_node)

            # Проверяем, что остановленный TCP-узел
            # действительно недоступен.
            with pytest.raises(
                (
                    ConnectionError,
                    OSError,
                    asyncio.TimeoutError,
                )
            ):
                await publisher.ping(
                    host=failed_node.local_contact.host,
                    port=failed_node.local_contact.port,
                    expected_node_id=original_id,
                )

            # Репликация восстанавливается автоматически.
            # Никаких maintain_published_records() здесь нет.
            await assert_three_copies()

            print(
                f"Cycle {cycle}: replication recovered"
            )

            # 6. Перезапускаем отказавший узел,
            # используя прежнее состояние на диске.
            bootstrap_peers = [
                {
                    "host": publisher.local_contact.host,
                    "port": publisher.local_contact.port,
                }
            ]

            restart_config = make_config(
                state_dirs[nodes.index(failed_node)],
                failed_node.local_contact.port,
                bootstrap_peers=bootstrap_peers,
            )

            restarted_node = Node(restart_config)

            # Проверяем сохранение идентичности.
            assert restarted_node.identity.node_id == original_id

            await restarted_node.start()
            started.append(restarted_node)
            active_peers.append(restarted_node)

            # Убеждаемся, что вернувшийся узел
            # действительно отвечает по TCP.
            contact = await publisher.ping(
                host=restarted_node.local_contact.host,
                port=restarted_node.local_contact.port,
                expected_node_id=original_id,
            )

            assert contact is not None

            # Обновляем ссылки для следующих циклов.
            nodes[nodes.index(failed_node)] = restarted_node

            # После возвращения узла доступность
            # трёх копий должна сохраняться.
            await assert_three_copies()

            print(
                f"Cycle {cycle}: node restarted successfully"
            )

        # 7. Финальная проверка реестра.
        published = await publisher.published_registry.get(key)

        assert published == record

        # 8. Финальная проверка доступных реплик.
        await assert_three_copies()

    finally:
        for node in reversed(started):
            await node.stop()
