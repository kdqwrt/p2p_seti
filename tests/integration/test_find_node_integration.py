
import socket
from dataclasses import replace

import pytest

from p2p.config import NodeConfig
from p2p.node.node import Node


def get_free_port() -> int:
    """Выбирает свободный локальный TCP-порт."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def make_config(tmp_path, name: str, port: int) -> NodeConfig:
    """Создаёт конфигурацию узла для тестирования."""
    base = NodeConfig.from_file("configs/node1.json")

    return replace(
        base,
        node_state_dir=str(tmp_path / name),
        listen_host="127.0.0.1",
        listen_port=port,
        bootstrap_peers=[],
        log_file=str(tmp_path / f"{name}.log"),
    )


@pytest.mark.asyncio
async def test_find_node_discovers_third_node(tmp_path):
    """
    Узел 2 знает только узел 1.
    Узел 1 знает узел 3.
    Узел 2 должен обнаружить узел 3 через FIND_NODE.
    """

    ports = [get_free_port() for _ in range(3)]
    assert len(set(ports)) == 3

    node1 = Node(make_config(tmp_path, "node1", ports[0]))
    node2 = Node(make_config(tmp_path, "node2", ports[1]))
    node3 = Node(make_config(tmp_path, "node3", ports[2]))

    nodes = [node1, node2, node3]

    try:
        for node in nodes:
            await node.start()

        # Узел 1 знакомится с узлом 3.
        await node1.ping(
            host=node3.local_contact.host,
            port=node3.local_contact.port,
            expected_node_id=node3.identity.node_id,
        )

        # Узел 2 знакомится только с узлом 1.
        await node2.ping(
            host=node1.local_contact.host,
            port=node1.local_contact.port,
            expected_node_id=node1.identity.node_id,
        )

        # Проверяем начальное состояние.
        assert (
            node1.routing_table.get_contact(node3.identity.node_id)
            is not None
        )

        assert (
            node2.routing_table.get_contact(node3.identity.node_id)
            is None
        )

        # Узел 2 спрашивает узел 1 о третьем узле.
        contacts = await node2.find_node(
            contact=node1.local_contact,
            target_node_id=node3.identity.node_id,
        )

        # Узел 3 должен присутствовать в ответе.
        found = [
            contact
            for contact in contacts
            if contact.node_id == node3.identity.node_id
        ]

        assert len(found) == 1
        assert found[0].host == node3.local_contact.host
        assert found[0].port == node3.local_contact.port

        # FIND_NODE не должен автоматически доверять
        # контактам, полученным от другого узла.
        assert (
            node2.routing_table.get_contact(node3.identity.node_id)
            is None
        )

        # Теперь узел 2 самостоятельно проверяет узел 3.
        await node2.ping(
            host=found[0].host,
            port=found[0].port,
            expected_node_id=found[0].node_id,
        )

        # После успешного PING контакт появляется в таблице.
        assert (
            node2.routing_table.get_contact(node3.identity.node_id)
            is not None
        )

    finally:
        for node in reversed(nodes):
            await node.stop()
