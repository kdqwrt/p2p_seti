import socket
from dataclasses import replace

import pytest

from p2p.config import NodeConfig
from p2p.node.node import Node


def free_ports(count: int) -> list[int]:
    sockets = []

    try:
        for _ in range(count):
            sock = socket.socket()
            sock.bind(("127.0.0.1", 0))
            sockets.append(sock)

        return [
            sock.getsockname()[1]
            for sock in sockets
        ]

    finally:
        for sock in sockets:
            sock.close()


@pytest.mark.asyncio
async def test_lookup_after_bootstrap_seed_stops(tmp_path):
    base = NodeConfig.from_file(
        "configs/node1.json"
    )

    ports = free_ports(5)

    nodes = []
    started = []

    for i in range(5):
        config = replace(
            base,
            node_state_dir=str(
                tmp_path / f"node{i}"
            ),
            log_file=str(
                tmp_path / f"node{i}.log"
            ),
            listen_host="127.0.0.1",
            listen_port=ports[i],
            bootstrap_peers=(
                []
                if i == 0
                else [
                    {
                        "host": "127.0.0.1",
                        "port": ports[0],
                    }
                ]
            ),
        )

        nodes.append(Node(config))

    try:
        for node in nodes:
            await node.start()
            started.append(node)


        await nodes[1].ping(
            nodes[4].local_contact.host,
            nodes[4].local_contact.port,
            expected_node_id=nodes[4].identity.node_id,
        )


        await nodes[0].stop()
        started.remove(nodes[0])


        result = await nodes[1].lookup_node(
            nodes[4].identity.node_id
        )

        assert result.found is True

        assert any(
            contact.node_id == nodes[4].identity.node_id
            for contact in result.closest_contacts
        )

    finally:
        for node in reversed(started):
            await node.stop()
