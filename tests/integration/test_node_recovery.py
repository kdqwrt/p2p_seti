
import asyncio
import socket
from dataclasses import replace

import pytest

from p2p.config import NodeConfig
from p2p.node.node import Node


def get_free_port() -> int:
    """Возвращает свободный TCP-порт."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.mark.asyncio
async def test_node_recovery(tmp_path):
    """Проверяет восстановление узла с прежним NodeID."""

    config_path = (
        __import__("pathlib").Path(__file__).resolve().parents[2]
        / "configs"
        / "node1.json"
    )

    base_config = NodeConfig.from_file(str(config_path))

    ports = []
    while len(ports) < 3:
        port = get_free_port()
        if port not in ports:
            ports.append(port)

    configs = [
        replace(
            base_config,
            listen_host="127.0.0.1",
            listen_port=ports[i],
            node_state_dir=str(tmp_path / f"node{i + 1}"),
            log_file=str(tmp_path / f"node{i + 1}.log"),
            bootstrap_peers=[],
        )
        for i in range(3)
    ]

    node1, node2, node3 = [Node(config) for config in configs]
    started = []

    try:
        for node in (node1, node2, node3):
            await node.start()
            started.append(node)

        # Формируем цепочку: node1 -> node2 -> node3.
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

        original_node_id = node2.identity.node_id

        # До отказа node2 доступен.
        await node1.ping(
            node2.local_contact.host,
            node2.local_contact.port,
            expected_node_id=original_node_id,
        )

        # Останавливаем node2.
        await node2.stop()
        started.remove(node2)

        # Проверяем, что node2 перестал принимать соединения.
        with pytest.raises(OSError):
            await node1.ping(
                node2.local_contact.host,
                node2.local_contact.port,
                expected_node_id=original_node_id,
            )

        # Восстанавливаем node2 с прежним состоянием.
        restored_node2 = Node(configs[1])

        assert restored_node2.identity.node_id == original_node_id, (
            "NodeID изменился после восстановления"
        )

        await restored_node2.start()
        started.append(restored_node2)

        # После восстановления PING снова работает.
        await node1.ping(
            restored_node2.local_contact.host,
            restored_node2.local_contact.port,
            expected_node_id=original_node_id,
        )

        # Восстанавливаем связь node2 -> node3.
        await restored_node2.ping(
            node3.local_contact.host,
            node3.local_contact.port,
            expected_node_id=node3.identity.node_id,
        )

        # Проверяем поиск node3 через восстановленный node2.
        result = await node1.lookup_node(
            node3.identity.node_id,
            collect_trace=True,
        )

        assert result.found is True, (
            "После восстановления поиск узла не работает"
        )

        assert result.trace is not None, (
            "Трассировка FIND_NODE не была собрана"
        )

        assert result.rpc_count > 0, (
            "Поиск не выполнил ни одного RPC-запроса"
        )

        assert any(
            event.peer_node_id == original_node_id.hex()
            and event.status == "success"
            for event in result.trace
        ), "Восстановленный узел не участвовал в FIND_NODE"

    finally:
        for node in reversed(started):
            await node.stop()
