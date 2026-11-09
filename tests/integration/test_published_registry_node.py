
import socket

import pytest

from p2p.config import NodeConfig
from p2p.node.node import Node
from p2p.dht.records import (
    create_node_record,
    node_record_key,
)


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
        connect_timeout_ms=1000,
        read_timeout_ms=2000,
        ping_timeout_ms=1000,
        max_frame_payload=65536,
        protocol_version=1,
        log_level="WARNING",
        log_file=str(state_dir / "node.log"),
    )


@pytest.mark.asyncio
async def test_successful_publication_is_registered(tmp_path):
    ports = set()

    while len(ports) < 4:
        ports.add(free_port())

    ports = list(ports)

    nodes = [
        Node(make_config(tmp_path / f"node{i}", ports[i]))
        for i in range(4)
    ]

    started = []

    try:
        for node in nodes:
            await node.start()
            started.append(node)

        publisher = nodes[0]

        for replica in nodes[1:]:
            await publisher.ping(
                host=replica.local_contact.host,
                port=replica.local_contact.port,
            )

        record = create_node_record(
            identity=publisher.identity,
            addresses=[f"127.0.0.1:{ports[0]}"],
            sequence_number=1,
        )

        result = await publisher.publish_record(record)

        assert result.successful is True
        assert result.confirmed == 3

        key = node_record_key(record.node_id)

        registered = await publisher.published_registry.get(key)

        assert registered == record

    finally:
        for node in reversed(started):
            await node.stop()


@pytest.mark.asyncio
async def test_failed_publication_is_not_registered(tmp_path):
    config = make_config(
        tmp_path / "isolated",
        free_port(),
    )

    node = Node(config)

    try:
        await node.start()

        record = create_node_record(
            identity=node.identity,
            addresses=["127.0.0.1:9101"],
            sequence_number=1,
        )

        result = await node.publish_record(record)

        assert result.successful is False
        assert result.confirmed == 0

        key = node_record_key(record.node_id)

        assert await node.published_registry.get(key) is None

    finally:
        await node.stop()


@pytest.mark.asyncio
async def test_registry_survives_node_restart(tmp_path):
    state_dir = tmp_path / "restart"

    config = make_config(state_dir, free_port())

    first = Node(config)

    try:
        await first.start()

        record = create_node_record(
            identity=first.identity,
            addresses=["127.0.0.1:9101"],
            sequence_number=1,
        )

        # Проверяем восстановление ранее зарегистрированной
        # записи без повторной публикации.
        assert await first.published_registry.add(record)

    finally:
        await first.stop()

    second = Node(config)

    try:
        await second.start()

        key = node_record_key(record.node_id)

        restored = await second.published_registry.get(key)

        assert restored == record

    finally:
        await second.stop()
