
import socket

import pytest

from p2p.config import NodeConfig
from p2p.node.node import Node
from p2p.dht.records import create_node_record, node_record_key
from p2p.dht.replication import replicate_record


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
        k_bucket_size=4,
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
async def test_replication_over_real_tcp(tmp_path):
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

        owner = nodes[0]
        replicas = nodes[1:]

        record = create_node_record(
            identity=owner.identity,
            addresses=[f"127.0.0.1:{ports[0]}"],
            sequence_number=1,
        )

        key = node_record_key(owner.identity.node_id)

        result = await replicate_record(
            record=record,
            contacts=[node.local_contact for node in replicas],
            store=owner.store_record,
        )

        assert result.attempted == 3
        assert result.confirmed == 3
        assert result.successful is True

        for node in replicas:
            stored = await node.dht_storage.get(key)
            assert stored == record

        # Останавливаем одну из реплик.
        await replicas[0].stop()
        started.remove(replicas[0])

        # Две оставшиеся реплики доступны.
        available = 0

        for node in replicas[1:]:
            received = await owner.find_value(
                node.local_contact,
                key,
            )

            if received == record:
                available += 1

        assert available == 2

        # Повторная публикация должна выдержать
        # недоступность одной реплики.
        result_after_failure = await replicate_record(
            record=record,
            contacts=[node.local_contact for node in replicas],
            store=owner.store_record,
        )

        assert result_after_failure.attempted == 3
        assert result_after_failure.confirmed == 2
        assert result_after_failure.successful is True

    finally:
        for node in reversed(started):
            await node.stop()
