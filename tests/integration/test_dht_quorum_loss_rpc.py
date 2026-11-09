
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
        connect_timeout_ms=300,
        read_timeout_ms=1000,
        ping_timeout_ms=300,
        max_frame_payload=65536,
        protocol_version=1,
        log_level="WARNING",
        log_file=str(state_dir / "node.log"),
    )


@pytest.mark.asyncio
async def test_quorum_loss_and_recovery(tmp_path):
    """
    Проверка потери кворума и его восстановления.

    1. Издатель публикует запись на трёх узлах.
    2. Две реплики отключаются.
    3. Обслуживание обнаруживает отсутствие кворума.
    4. Узлы возвращаются.
    5. Обслуживание подтверждает восстановление.

    Все DHT-запросы выполняются через настоящий TCP.
    """

    ports = set()

    while len(ports) < 4:
        ports.add(free_port())

    ports = list(ports)

    state_dirs = [
        tmp_path / f"node{i}"
        for i in range(4)
    ]

    nodes = [
        Node(make_config(state_dirs[i], ports[i]))
        for i in range(4)
    ]

    publisher = nodes[0]
    replicas = nodes[1:]

    started = []

    try:
        # 1. Запускаем сеть.
        for node in nodes:
            await node.start()
            started.append(node)

        # 2. Издатель обнаруживает три реплики.
        for replica in replicas:
            await publisher.ping(
                host=replica.local_contact.host,
                port=replica.local_contact.port,
            )

        assert publisher.routing_table.size() == 3

        # 3. Создаём запись.
        record = create_node_record(
            identity=publisher.identity,
            addresses=[f"127.0.0.1:{ports[0]}"],
            sequence_number=1,
            ttl_seconds=180,
        )

        key = node_record_key(record.node_id)

        # 4. Первоначальная публикация.
        published = await publisher.publish_record(record)

        assert published.successful is True
        assert published.fully_replicated is True
        assert published.confirmed == 3

        for replica in replicas:
            assert await replica.dht_storage.get(key) == record

        # Сохраняем контакты всех трёх реплик.
        # В том числе тех, которые затем отключатся.
        contacts = [
            replica.local_contact
            for replica in replicas
        ]

        # 5. Отключаем две реплики.
        failed_replicas = replicas[:2]
        surviving_replica = replicas[2]

        for replica in failed_replicas:
            await replica.stop()
            started.remove(replica)

        # Одна доступная копия осталась.
        assert (
            await surviving_replica.dht_storage.get(key)
            == record
        )

        # 6. Проверяем отсутствие кворума.
        # Реальные FIND_VALUE и STORE через TCP.
        result = await publisher.replica_maintenance.maintain(
            record,
            contacts,
        )

        print(
            "\nQUORUM LOST:",
            f"checked={result.checked}",
            f"existing={result.existing}",
            f"repaired={result.repaired}",
            f"confirmed={result.confirmed}",
            f"required={result.required}",
            f"successful={result.successful}",
            f"fully_replicated={result.fully_replicated}",
        )

        assert result.checked == 3
        assert result.existing == 1
        assert result.repaired == 0
        assert result.confirmed == 1
        assert result.required == 2

        assert result.successful is False
        assert result.fully_replicated is False

        # 7. Возвращаем одну реплику.
        first_failed = failed_replicas[0]

        restarted_first = Node(
            make_config(
                state_dirs[1],
                ports[1],
                bootstrap_peers=[
                    {
                        "host": publisher.local_contact.host,
                        "port": publisher.local_contact.port,
                    }
                ],
            )
        )

        assert (
            restarted_first.identity.node_id
            == first_failed.identity.node_id
        )

        await restarted_first.start()
        started.append(restarted_first)

        # Проверяем восстановление TCP-связи.
        await publisher.ping(
            host=restarted_first.local_contact.host,
            port=restarted_first.local_contact.port,
            expected_node_id=first_failed.identity.node_id,
        )

        # 8. Теперь две реплики доступны.
        result = await publisher.replica_maintenance.maintain(
            record,
            contacts,
        )

        print(
            "\nQUORUM RESTORED:",
            f"confirmed={result.confirmed}",
            f"successful={result.successful}",
            f"fully_replicated={result.fully_replicated}",
        )

        assert result.confirmed == 2
        assert result.required == 2
        assert result.successful is True
        assert result.fully_replicated is False

        # 9. Возвращаем последнюю реплику.
        second_failed = failed_replicas[1]

        restarted_second = Node(
            make_config(
                state_dirs[2],
                ports[2],
                bootstrap_peers=[
                    {
                        "host": publisher.local_contact.host,
                        "port": publisher.local_contact.port,
                    }
                ],
            )
        )

        assert (
            restarted_second.identity.node_id
            == second_failed.identity.node_id
        )

        await restarted_second.start()
        started.append(restarted_second)

        await publisher.ping(
            host=restarted_second.local_contact.host,
            port=restarted_second.local_contact.port,
            expected_node_id=second_failed.identity.node_id,
        )

        # 10. Полная репликация восстановлена.
        result = await publisher.replica_maintenance.maintain(
            record,
            contacts,
        )

        print(
            "\nFULL REPLICATION RESTORED:",
            f"confirmed={result.confirmed}",
            f"successful={result.successful}",
            f"fully_replicated={result.fully_replicated}",
        )

        assert result.checked == 3
        assert result.confirmed == 3
        assert result.successful is True
        assert result.fully_replicated is True

        # 11. Проверяем фактическое хранение.
        available_replicas = [
            surviving_replica,
            restarted_first,
            restarted_second,
        ]

        for replica in available_replicas:
            stored = await replica.dht_storage.get(key)
            assert stored == record

            received = await publisher.find_value(
                replica.local_contact,
                key,
            )
            assert received == record

        # 12. Проверяем сохранность реестра издателя.
        registered = await publisher.published_registry.get(key)
        assert registered == record

    finally:
        for node in reversed(started):
            await node.stop()
