
import socket
from dataclasses import replace

import pytest

from p2p.config import NodeConfig
from p2p.node.node import Node


def get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def make_config(tmp_path, name: str, port: int) -> NodeConfig:
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
async def test_iterative_lookup_four_nodes(tmp_path):
    """Узел 1 находит узел 4 через промежуточные узлы."""

    ports = []
    while len(ports) < 4:
        port = get_free_port()
        if port not in ports:
            ports.append(port)

    nodes = [
        Node(make_config(tmp_path, f"node{i+1}", ports[i]))
        for i in range(4)
    ]

    node1, node2, node3, node4 = nodes

    try:
        for node in nodes:
            await node.start()

        # Формируем цепочку 1 -> 2 -> 3 -> 4.
        await node1.ping(
            node2.local_contact.host,
            node2.local_contact.port,
            expected_node_id=node2.identity.node_id,
        )

        await node2.ping(
            node3.local_contact.host,
            node3.local_contact.port,
            expected_node_id=node3.identity.node_id,
        )

        await node3.ping(
            node4.local_contact.host,
            node4.local_contact.port,
            expected_node_id=node4.identity.node_id,
        )

        # Узел 1 изначально не знает узел 4.
        assert (
            node1.routing_table.get_contact(node4.identity.node_id)
            is None
        )

        result = await node1.lookup_node(node4.identity.node_id)

        assert result.found is True
        assert any(
            contact.node_id == node4.identity.node_id
            for contact in result.closest_contacts
        )

        assert (
            node1.routing_table.get_contact(node4.identity.node_id)
            is not None
        )

        assert result.rpc_count >= 2
        assert result.iterations >= 2

    finally:
        for node in reversed(nodes):
            await node.stop()



@pytest.mark.asyncio
async def test_bootstrap_discovers_other_nodes(tmp_path):
    """Новый узел обнаруживает соседей через bootstrap."""

    ports = []
    while len(ports) < 3:
        port = get_free_port()
        if port not in ports:
            ports.append(port)

    node1 = Node(make_config(tmp_path, "bootstrap1", ports[0]))
    node2 = Node(make_config(tmp_path, "bootstrap2", ports[1]))

    node3_config = replace(
        make_config(tmp_path, "bootstrap3", ports[2]),
        bootstrap_peers=[
            {"host": "127.0.0.1", "port": ports[0]}
        ],
    )
    node3 = Node(node3_config)

    try:
        await node1.start()
        await node2.start()

        # Первый узел знает второй.
        await node1.ping(
            node2.local_contact.host,
            node2.local_contact.port,
            expected_node_id=node2.identity.node_id,
        )

        # Третий узел знает только адрес первого.
        # При start() должен выполниться self-lookup.
        await node3.start()

        assert (
            node3.routing_table.get_contact(
                node1.identity.node_id
            ) is not None
        )

        assert (
            node3.routing_table.get_contact(
                node2.identity.node_id
            ) is not None
        )

    finally:
        await node3.stop()
        await node2.stop()
        await node1.stop()



@pytest.mark.asyncio
async def test_lookup_handles_unavailable_node(tmp_path):
    """Поиск не падает, если известный узел недоступен."""

    ports = []
    while len(ports) < 3:
        port = get_free_port()
        if port not in ports:
            ports.append(port)

    nodes = [
        Node(make_config(tmp_path, f"failure{i}", ports[i]))
        for i in range(3)
    ]

    node1, node2, node3 = nodes

    try:
        for node in nodes:
            await node.start()

        # Узел 1 знает узлы 2 и 3.
        await node1.ping(
            node2.local_contact.host,
            node2.local_contact.port,
            expected_node_id=node2.identity.node_id,
        )

        await node1.ping(
            node3.local_contact.host,
            node3.local_contact.port,
            expected_node_id=node3.identity.node_id,
        )

        # Узел 2 становится недоступен.
        await node2.stop()

        # Ищем доступный узел 3.
        result = await node1.lookup_node(
            node3.identity.node_id
        )

        assert result.found is True
        assert result.rpc_count >= 1

        assert any(
            contact.node_id == node3.identity.node_id
            for contact in result.closest_contacts
        )

    finally:
        # Повторный stop() для node2 не требуется.
        await node3.stop()
        await node1.stop()



@pytest.mark.asyncio
async def test_lookup_alternative_path_after_failure(tmp_path):
    """Поиск достигает цели через живых соседей при отказе узла."""

    ports = []
    while len(ports) < 5:
        port = get_free_port()
        if port not in ports:
            ports.append(port)

    nodes = [
        Node(make_config(tmp_path, f"alternative{i}", ports[i]))
        for i in range(5)
    ]

    node1, node2, node3, node4, node5 = nodes

    async def connect(source, destination):
        await source.ping(
            destination.local_contact.host,
            destination.local_contact.port,
            expected_node_id=destination.identity.node_id,
        )

    try:
        for node in nodes:
            await node.start()

        # Создаём логические связи.
        await connect(node1, node2)
        await connect(node1, node3)
        await connect(node3, node4)
        await connect(node4, node5)

        # Учитываем, что входящие PING могут инициировать
        # обратную проверку контактов.
        assert node1.routing_table.get_contact(
            node5.identity.node_id
        ) is None

        # Имитируем отказ второго узла.
        await node2.stop()

        # Ищем пятый узел.
        result = await node1.lookup_node(
            node5.identity.node_id
        )

        assert result.found is True

        assert any(
            contact.node_id == node5.identity.node_id
            for contact in result.closest_contacts
        )

        assert node1.routing_table.get_contact(
            node5.identity.node_id
        ) is not None

        assert result.rpc_count >= 2

    finally:
        await node5.stop()
        await node4.stop()
        await node3.stop()
        await node1.stop()
