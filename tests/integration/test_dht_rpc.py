
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


def get_free_port() -> int:
    """Находит свободный TCP-порт для теста."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def make_config(state_dir, port: int) -> NodeConfig:
    """Создаёт конфигурацию тестового узла."""

    return NodeConfig(
        node_state_dir=str(state_dir),
        listen_host="127.0.0.1",
        listen_port=port,
        bootstrap_peers=[],
        node_id_bits=256,
        k_bucket_size=4,
        alpha=3,
        connect_timeout_ms=2000,
        read_timeout_ms=3000,
        ping_timeout_ms=2000,
        max_frame_payload=65536,
        protocol_version=1,
        log_level="WARNING",
        log_file=str(state_dir / "node.log"),
    )


@pytest.mark.asyncio
async def test_dht_store_and_find_value_between_two_nodes(tmp_path):
    """
    Узел 1 сохраняет подписанную запись на узле 2,
    затем запрашивает её обратно по DHT-ключу.
    """

    port1 = get_free_port()
    port2 = get_free_port()

    while port2 == port1:
        port2 = get_free_port()

    node1 = Node(make_config(tmp_path / "node1", port1))
    node2 = Node(make_config(tmp_path / "node2", port2))

    await node1.start()
    try:
        await node2.start()
        try:
            record = create_node_record(
                identity=node1.identity,
                addresses=[
                    f"127.0.0.1:{port1}",
                ],
                sequence_number=1,
                now=int(time.time()),
            )

            key = node_record_key(node1.identity.node_id)

            # До сохранения записи на втором узле нет.
            missing = await node1.find_value(
                node2.local_contact,
                key,
            )
            assert missing is None

            # Первый узел сохраняет запись на втором.
            stored = await node1.store_record(
                node2.local_contact,
                record,
            )
            assert stored is True

            # Проверяем, что второй узел сохранил запись.
            local_record = await node2.dht_storage.get(key)
            assert local_record == record

            # Первый узел получает запись обратно по TCP.
            received = await node1.find_value(
                node2.local_contact,
                key,
            )

            assert received is not None
            assert received == record
            assert received.signature == record.signature

        finally:
            await node2.stop()
    finally:
        await node1.stop()


@pytest.mark.asyncio
async def test_dht_record_survives_node_restart(tmp_path):
    """
    Проверяет, что DHT-запись сохраняется на диске
    и восстанавливается после перезапуска узла.
    """

    port1 = get_free_port()
    port2 = get_free_port()

    while port2 == port1:
        port2 = get_free_port()

    node1 = Node(make_config(tmp_path / "node1", port1))
    node2_config = make_config(tmp_path / "node2", port2)
    node2 = Node(node2_config)

    await node1.start()
    try:
        await node2.start()
        try:
            record = create_node_record(
                identity=node1.identity,
                addresses=[f"127.0.0.1:{port1}"],
                sequence_number=1,
                now=int(time.time()),
            )

            key = node_record_key(node1.identity.node_id)

            assert await node1.store_record(
                node2.local_contact,
                record,
            )

        finally:
            await node2.stop()

        # Пересоздаём объект с той же директорией состояния.
        restarted_node2 = Node(node2_config)

        await restarted_node2.start()
        try:
            received = await node1.find_value(
                restarted_node2.local_contact,
                key,
            )

            assert received == record

        finally:
            await restarted_node2.stop()

    finally:
        await node1.stop()
