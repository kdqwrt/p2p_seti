
import socket

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


def make_config(state_dir, port, bootstrap_peers=None):
    return NodeConfig(
        node_state_dir=str(state_dir),
        listen_host="127.0.0.1",
        listen_port=port,
        bootstrap_peers=bootstrap_peers or [],
        node_id_bits=256,
        k_bucket_size=8,
        alpha=3,
        connect_timeout_ms=1000,
        read_timeout_ms=2000,
        ping_timeout_ms=1000,
        max_frame_payload=65536,
        protocol_version=1,
        log_level="WARNING",
        log_file=str(state_dir / "node.log"),
    )


@pytest.mark.asyncio
async def test_discover_network_through_single_bootstrap(tmp_path):
    """
    Проверяет обнаружение сети через один bootstrap-адрес.

    Четыре узла образуют существующую сеть.
    Пятый узел знает только первый адрес.

    Никаких подмен сетевых методов нет.
    """

    ports = set()

    while len(ports) < 5:
        ports.add(free_port())

    ports = list(ports)

    existing_nodes = [
        Node(
            make_config(
                tmp_path / f"existing_{i}",
                ports[i],
            )
        )
        for i in range(4)
    ]

    started = []

    try:
        # 1. Запускаем существующую сеть.
        for node in existing_nodes:
            await node.start()
            started.append(node)

        # 2. Формируем сеть из четырёх узлов.
        # Это подготовка существующей сети,
        # а не ручная настройка нового участника.
        for i, node in enumerate(existing_nodes):
            for other in existing_nodes[i + 1:]:
                await node.ping(
                    host=other.local_contact.host,
                    port=other.local_contact.port,
                )

        # Проверяем, что bootstrap-узел
        # действительно знает остальных.
        bootstrap_node = existing_nodes[0]

        assert bootstrap_node.routing_table.size() == 3

        # 3. Создаём нового участника.
        # Ему известен только ОДИН адрес.
        newcomer_config = make_config(
            tmp_path / "newcomer",
            ports[4],
            bootstrap_peers=[
                {
                    "host": bootstrap_node.local_contact.host,
                    "port": bootstrap_node.local_contact.port,
                }
            ],
        )

        assert len(newcomer_config.bootstrap_peers) == 1

        newcomer = Node(newcomer_config)

        # 4. Запускаем нового участника.
        # Node.start() сам выполняет bootstrap.
        await newcomer.start()
        started.append(newcomer)

        # 5. Проверяем обнаружение соседей.
        discovered_contacts = newcomer.routing_table.closest(
            newcomer.identity.node_id,
            limit=8,
        )

        discovered_ids = {
            contact.node_id
            for contact in discovered_contacts
        }

        expected_ids = {
            node.identity.node_id
            for node in existing_nodes
        }

        # Должны быть обнаружены ВСЕ четыре узла.
        assert expected_ids.issubset(discovered_ids), (
            "New node did not discover all existing peers. "
            f"Discovered: {len(discovered_ids)}, "
            f"expected: {len(expected_ids)}"
        )

        # 6. Создаём настоящую подписанную запись.
        record = create_node_record(
            identity=newcomer.identity,
            addresses=[
                f"127.0.0.1:{ports[4]}"
            ],
            sequence_number=1,
            ttl_seconds=180,
        )

        key = node_record_key(record.node_id)

        # 7. Публикуем запись.
        # Новый узел НЕ получал ручных ping ко всем.
        result = await newcomer.publish_record(record)

        assert result.successful is True
        assert result.fully_replicated is True
        assert result.confirmed == 3

        # 8. Проверяем реальное размещение записи.
        holders = []

        for node in existing_nodes:
            stored = await node.dht_storage.get(key)

            if stored == record:
                holders.append(node)

        assert len(holders) == 3, (
            "Expected exactly three remote replicas, "
            f"found {len(holders)}"
        )

        # 9. Проверяем чтение через настоящий TCP.
        for holder in holders:
            received = await newcomer.find_value(
                holder.local_contact,
                key,
            )

            assert received == record

        # 10. Проверяем реестр издателя.
        published = await newcomer.published_registry.get(key)

        assert published == record

    finally:
        for node in reversed(started):
            await node.stop()
