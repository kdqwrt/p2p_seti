
import asyncio
import math
import socket
import time

import pytest
from p2p.transport.connection import RpcError
from p2p.config import NodeConfig
from p2p.node.node import Node
from p2p.dht.records import (
    create_node_record,
    node_record_key,
)


OWNER_COUNT = 4
MAX_VERSION = 10
READS_PER_OWNER = 30


def free_port():
    with socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM,
    ) as sock:
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
        read_timeout_ms=5000,
        ping_timeout_ms=5000,
        max_frame_payload=65536,
        protocol_version=1,
        log_level="WARNING",
        log_file=str(state_dir / "node.log"),
    )


@pytest.mark.asyncio
async def test_concurrent_read_write_consistency(tmp_path):
    """
    Проверяет конкурентные STORE и FIND_VALUE.

    4 владельца.
    1 общий получатель.

    Для каждого владельца:
      - начальная версия 1;
      - обновления до версии 10;
      - 30 последовательных чтений.

    Чтения и обновления разных ключей
    выполняются конкурентно через TCP.
    """

    node_count = OWNER_COUNT + 1

    ports = set()

    while len(ports) < node_count:
        ports.add(free_port())

    ports = list(ports)

    nodes = [
        Node(
            make_config(
                tmp_path / f"node{i}",
                ports[i],
            )
        )
        for i in range(node_count)
    ]

    owners = nodes[:OWNER_COUNT]
    receiver = nodes[-1]

    started = []

    try:
        # ----------------------------------
        # 1. Запуск узлов
        # ----------------------------------

        for node in nodes:
            await node.start()
            started.append(node)

        assert len({
            node.identity.node_id
            for node in nodes
        }) == node_count

        for owner in owners:
            await owner.ping(
                host=receiver.local_contact.host,
                port=receiver.local_contact.port,
                update_routing=False,
                expected_node_id=receiver.identity.node_id,
            )

        print(f"\nNODES: {node_count} started")

        # ----------------------------------
        # 2. Подготовка записей
        # ----------------------------------

        records = {}
        keys = {}

        for index, owner in enumerate(owners):
            address = f"127.0.0.1:{ports[index]}"

            records[index] = {
                version: create_node_record(
                    identity=owner.identity,
                    addresses=[address],
                    sequence_number=version,
                    ttl_seconds=180,
                )
                for version in range(
                    1,
                    MAX_VERSION + 1,
                )
            }

            keys[index] = node_record_key(
                records[index][1].node_id
            )

        assert len(set(keys.values())) == OWNER_COUNT

        print("KEYS: 4 independent records")

        # ----------------------------------
        # 3. Начальная версия каждого ключа
        # ----------------------------------

        for index, owner in enumerate(owners):
            result = await owner.store_record(
                receiver.local_contact,
                records[index][1],
            )

            assert result is True

        assert (
            await receiver.dht_storage.count()
            == OWNER_COUNT
        )

        print("INITIAL: version 1 stored for all keys")

        # ----------------------------------
        # 4. Одновременное чтение и запись
        # ----------------------------------

        start_event = asyncio.Event()

        read_counts = [0] * OWNER_COUNT
        write_counts = [0] * OWNER_COUNT

        observed_versions = {
            index: []
            for index in range(OWNER_COUNT)
        }

        async def writer(index):
            owner = owners[index]

            await start_event.wait()

            for version in range(2, MAX_VERSION + 1):
                result = await owner.store_record(
                    receiver.local_contact,
                    records[index][version],
                )

                assert result is True, (
                    f"STORE rejected: "
                    f"owner={index}, version={version}"
                )

                write_counts[index] += 1

                # Даём другим задачам возможность
                # выполнить чтение между обновлениями.
                await asyncio.sleep(0)

        async def reader(index):
            owner = owners[index]
            key = keys[index]

            last_seen_version = 1

            await start_event.wait()

            for _ in range(READS_PER_OWNER):
                received = await owner.find_value(
                    receiver.local_contact,
                    key,
                )

                assert received is not None, (
                    f"Missing record: owner={index}"
                )

                version = received.sequence_number

                assert 1 <= version <= MAX_VERSION

                # Проверяем точное совпадение
                # с одной из подписанных записей.
                assert received == records[index][version], (
                    f"Unexpected record: "
                    f"owner={index}, version={version}"
                )

                # Один читатель не должен наблюдать
                # уменьшение версии своего ключа.
                assert version >= last_seen_version, (
                    f"Version rollback observed: "
                    f"owner={index}, "
                    f"previous={last_seen_version}, "
                    f"current={version}"
                )

                last_seen_version = version

                observed_versions[index].append(version)
                read_counts[index] += 1

                await asyncio.sleep(0)

        tasks = []

        for index in range(OWNER_COUNT):
            tasks.append(
                asyncio.create_task(writer(index))
            )

            tasks.append(
                asyncio.create_task(reader(index))
            )

        started_at = time.perf_counter()

        try:
            start_event.set()

            await asyncio.wait_for(
                asyncio.gather(*tasks),
                timeout=60.0,
            )

        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()

            await asyncio.gather(
                *tasks,
                return_exceptions=True,
            )

        duration = time.perf_counter() - started_at

        # ----------------------------------
        # 5. Проверка выполненных операций
        # ----------------------------------

        expected_writes = OWNER_COUNT * (
            MAX_VERSION - 1
        )

        expected_reads = OWNER_COUNT * READS_PER_OWNER

        assert sum(write_counts) == expected_writes
        assert sum(read_counts) == expected_reads

        assert duration > 0
        assert math.isfinite(duration)

        print(
            f"CONCURRENT STORE: "
            f"{expected_writes} successful"
        )

        print(
            f"CONCURRENT FIND_VALUE: "
            f"{expected_reads} successful"
        )

        print(f"DURATION: {duration:.3f} s")

        # ----------------------------------
        # 6. Анализ наблюдаемых версий
        # ----------------------------------

        for index in range(OWNER_COUNT):
            versions = observed_versions[index]

            assert len(versions) == READS_PER_OWNER

            assert versions == sorted(versions), (
                f"Non-monotonic reads: "
                f"owner={index}, versions={versions}"
            )

            print(
                f"OWNER {index}: "
                f"first={versions[0]}, "
                f"last={versions[-1]}, "
                f"unique={len(set(versions))}"
            )

        print("MONOTONIC READS: passed")

        # ----------------------------------
        # 7. Проверка финального состояния
        # ----------------------------------

        assert (
            await receiver.dht_storage.count()
            == OWNER_COUNT
        )

        for index, owner in enumerate(owners):
            expected = records[index][MAX_VERSION]

            stored = await receiver.dht_storage.get(
                keys[index]
            )

            assert stored == expected

            received = await owner.find_value(
                receiver.local_contact,
                keys[index],
            )

            assert received == expected

        print("FINAL STATE: all keys at version 10")

        # ----------------------------------
        # 8. Проверка защиты от отката
        # ----------------------------------

        for index, owner in enumerate(owners):
            # Проверяем отказ старой версии
            # штатным RPC. В отличие от STORE
            # с актуальной версией, здесь сервер
            # может вернуть ERROR.
            with pytest.raises(
                    RpcError,
                    match="Expected STORE_RESPONSE, received ERROR",
            ):
                await owner.store_record(
                    receiver.local_contact,
                    records[index][1],
                )

            assert (
                await receiver.dht_storage.get(keys[index])
                == records[index][MAX_VERSION]
            )

        print("ROLLBACK PROTECTION: passed")

        # ----------------------------------
        # 9. Проверка доступности сети
        # ----------------------------------

        for owner in owners:
            rtt = await owner.ping(
                host=receiver.local_contact.host,
                port=receiver.local_contact.port,
                update_routing=False,
                expected_node_id=receiver.identity.node_id,
            )

            assert math.isfinite(rtt)
            assert rtt >= 0

        print("POST-LOAD NETWORK: passed")

        print(
            "TOTAL CONCURRENT RPC: "
            f"{expected_writes + expected_reads}"
        )

        print("CONCURRENT READ/WRITE: passed")

    finally:
        for node in reversed(started):
            await node.stop()
