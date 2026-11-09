
import socket

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
async def test_publish_replaces_failed_replica_over_tcp(
    tmp_path,
    monkeypatch,
):
    # Один издатель и четыре потенциальные реплики.
    ports = set()

    while len(ports) < 5:
        ports.add(free_port())

    ports = list(ports)

    nodes = [
        Node(
            make_config(
                tmp_path / f"node{i}",
                ports[i],
            )
        )
        for i in range(5)
    ]

    started = []

    try:
        # Запускаем все пять TCP-серверов.
        for node in nodes:
            await node.start()
            started.append(node)

        publisher = nodes[0]
        replicas = nodes[1:]

        # Издатель узнаёт обо всех четырёх узлах.
        for replica in replicas:
            await publisher.ping(
                host=replica.local_contact.host,
                port=replica.local_contact.port,
            )

        # Создаём подписанную запись.
        record = create_node_record(
            identity=publisher.identity,
            addresses=[
                f"127.0.0.1:{ports[0]}"
            ],
            sequence_number=1,
        )

        key = node_record_key(record.node_id)

        # Сортируем реплики по XOR-расстоянию.
        nearest = sorted(
            replicas,
            key=lambda node: xor_distance(
                node.identity.node_id,
                key,
            ),
        )

        # Первая из ближайших трёх реплик
        # должна отказать во время STORE.
        failed_replica = nearest[0]
        reserve_replica = nearest[3]

        original_store = publisher.store_record
        failure_triggered = False

        async def store_with_failure(contact, record):
            nonlocal failure_triggered

            if contact.node_id == failed_replica.identity.node_id:
                failure_triggered = True

                # Отключаем настоящий TCP-сервер.
                await failed_replica.stop()

                if failed_replica in started:
                    started.remove(failed_replica)

            return await original_store(contact, record)

        # Подменяем только момент отправки STORE.
        # Сама передача остаётся настоящей TCP RPC.
        monkeypatch.setattr(
            publisher,
            "store_record",
            store_with_failure,
        )

        # Проверяем весь путь:
        # lookup_node -> replicate_record -> STORE.
        result = await publisher.publish_record(record)

        assert failure_triggered is True

        assert result.attempted == 4
        assert result.confirmed == 3
        assert result.required == 2
        assert result.successful is True
        assert result.fully_replicated is True

        assert (
            failed_replica.identity.node_id
            in result.failed_nodes
        )

        assert (
            reserve_replica.identity.node_id
            in result.confirmed_nodes
        )

        # Проверяем фактическое хранение записей.
        for replica in nearest[1:]:
            stored = await replica.dht_storage.get(key)
            assert stored == record

        # Проверяем получение записи по настоящему TCP.
        received = await publisher.find_value(
            reserve_replica.local_contact,
            key,
        )

        assert received == record

    finally:
        for node in reversed(started):
            await node.stop()
