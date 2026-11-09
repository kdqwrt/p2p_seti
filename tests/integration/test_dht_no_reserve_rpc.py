
import asyncio
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
        connect_timeout_ms=300,
        read_timeout_ms=1000,
        ping_timeout_ms=300,
        max_frame_payload=65536,
        protocol_version=1,
        log_level="WARNING",
        log_file=str(state_dir / "node.log"),
    )


@pytest.mark.asyncio
async def test_replica_loss_without_reserve_over_tcp(tmp_path):
    """
    Проверяет потерю реплики без резервных узлов.

    Сеть:
        A — издатель
        B — реплика
        C — реплика
        D — реплика

    После отключения B остаются только C и D.
    Третью доступную копию создать негде.
    """

    ports = set()

    while len(ports) < 4:
        ports.add(free_port())

    ports = list(ports)

    nodes = [
        Node(
            make_config(
                tmp_path / f"node{i}",
                ports[i],
            )
        )
        for i in range(4)
    ]

    publisher = nodes[0]

    # Устанавливаем интервал ДО запуска.
    publisher._maintenance_interval = 0.05

    started = []

    try:
        # 1. Запускаем все узлы.
        for node in nodes:
            await node.start()
            started.append(node)

        replicas = nodes[1:]

        # 2. Издатель обнаруживает соседей.
        for replica in replicas:
            await publisher.ping(
                host=replica.local_contact.host,
                port=replica.local_contact.port,
            )

        assert publisher.routing_table.size() == 3

        # 3. Создаём подписанную запись.
        record = create_node_record(
            identity=publisher.identity,
            addresses=[
                f"127.0.0.1:{ports[0]}"
            ],
            sequence_number=1,
            ttl_seconds=180,
        )

        key = node_record_key(record.node_id)

        # 4. Публикуем запись.
        result = await publisher.publish_record(record)

        assert result.successful is True
        assert result.fully_replicated is True
        assert result.confirmed == 3

        # Все три узла действительно получили запись.
        for replica in replicas:
            stored = await replica.dht_storage.get(key)
            assert stored == record

        # 5. Отключаем одну реплику.
        failed_replica = replicas[0]

        await failed_replica.stop()
        started.remove(failed_replica)

        # Убеждаемся, что TCP-сервер недоступен.
        with pytest.raises(
            (ConnectionError, OSError, asyncio.TimeoutError)
        ):
            await publisher.ping(
                host=failed_replica.local_contact.host,
                port=failed_replica.local_contact.port,
                expected_node_id=failed_replica.identity.node_id,
            )

        # 6. Даём фоновому обслуживанию время
        # выполнить несколько циклов.
        await asyncio.sleep(0.5)

        # Фоновая задача не должна аварийно завершиться.
        assert publisher._maintenance_task is not None
        assert not publisher._maintenance_task.done()

        # 7. Проверяем две оставшиеся реплики.
        surviving_replicas = [
            replica
            for replica in replicas
            if replica is not failed_replica
        ]

        assert len(surviving_replicas) == 2

        for replica in surviving_replicas:
            stored = await replica.dht_storage.get(key)
            assert stored == record

            received = await publisher.find_value(
                replica.local_contact,
                key,
            )

            assert received == record

        # 8. Издатель не должен терять публикацию.
        published = await publisher.published_registry.get(key)

        assert published == record

        # 9. В сети физически работают только
        # два узла, способных хранить удалённые реплики.
        # Проверяем, что оба доступны.
        for replica in surviving_replicas:
            await publisher.ping(
                host=replica.local_contact.host,
                port=replica.local_contact.port,
                expected_node_id=replica.identity.node_id,
            )

    finally:
        for node in reversed(started):
            await node.stop()
