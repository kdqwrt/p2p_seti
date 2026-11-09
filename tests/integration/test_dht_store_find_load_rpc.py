
import asyncio
import math
import socket
import statistics
import time

import pytest

from p2p.config import NodeConfig
from p2p.node.node import Node
from p2p.dht.records import (
    create_node_record,
    node_record_key,
)


OWNER_COUNT = 8
VERSION_COUNT = 10
READS_PER_OWNER = 20
READ_CONCURRENCY = 20


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


def percentile(values, percent):
    assert values

    ordered = sorted(values)
    rank = math.ceil(len(ordered) * percent / 100)

    return ordered[max(0, rank - 1)]


def print_statistics(name, values, duration):
    assert values
    assert duration > 0

    print(f"\n{name}:")
    print(f"  REQUESTS: {len(values)}")
    print(f"  DURATION: {duration:.3f} s")
    print(f"  MEAN: {statistics.mean(values):.2f} ms")
    print(f"  MEDIAN: {statistics.median(values):.2f} ms")
    print(f"  P95: {percentile(values, 95):.2f} ms")
    print(f"  P99: {percentile(values, 99):.2f} ms")
    print(f"  MAX: {max(values):.2f} ms")
    print(
        f"  THROUGHPUT: "
        f"{len(values) / duration:.2f} req/s"
    )


@pytest.mark.asyncio
async def test_dht_store_and_find_value_under_load(tmp_path):
    """
    Нагрузка на DHT через реальные TCP RPC.

    8 владельцев + 1 получатель.

    80 STORE:
        8 ключей, по 10 версий каждого.

    160 FIND_VALUE:
        конкурентное чтение всех ключей.

    Проверяется:
    - успешность STORE;
    - монотонное обновление версий;
    - независимость разных ключей;
    - точность результатов FIND_VALUE;
    - отсутствие ошибок;
    - доступность узлов после нагрузки.
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
        # 1. Запуск сети
        # ----------------------------------

        for node in nodes:
            await node.start()
            started.append(node)

        assert len({
            node.identity.node_id
            for node in nodes
        }) == node_count

        print(f"\nNODES: {node_count} started")

        # ----------------------------------
        # 2. Проверка TCP-соединений
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

        print("NETWORK: all owners connected")

        # ----------------------------------
        # 3. Подготовка подписанных записей
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
                    VERSION_COUNT + 1,
                )
            }

            keys[index] = node_record_key(
                records[index][1].node_id
            )

            assert all(
                node_record_key(record.node_id)
                == keys[index]
                for record in records[index].values()
            )

        assert len(set(keys.values())) == OWNER_COUNT

        print(f"KEYS: {OWNER_COUNT} unique records")

        # ----------------------------------
        # 4. Конкурентные STORE
        # ----------------------------------

        store_latencies = []
        store_failures = []

        start_event = asyncio.Event()

        async def update_owner(index):
            owner = owners[index]

            await start_event.wait()

            for version in range(
                1,
                VERSION_COUNT + 1,
            ):
                record = records[index][version]

                started_at = time.perf_counter()

                try:
                    result = await owner.store_record(
                        receiver.local_contact,
                        record,
                    )

                    elapsed_ms = (
                        time.perf_counter() - started_at
                    ) * 1000

                    assert result is True

                    store_latencies.append(elapsed_ms)

                except Exception as exc:
                    store_failures.append(
                        (index, version, repr(exc))
                    )
                    raise

        tasks = [
            asyncio.create_task(update_owner(index))
            for index in range(OWNER_COUNT)
        ]

        store_started = time.perf_counter()

        start_event.set()

        try:
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

        store_duration = (
            time.perf_counter() - store_started
        )

        assert not store_failures, store_failures

        assert len(store_latencies) == (
            OWNER_COUNT * VERSION_COUNT
        )

        print_statistics(
            "STORE",
            store_latencies,
            store_duration,
        )

        # ----------------------------------
        # 5. Проверка всех сохранённых ключей
        # ----------------------------------

        assert (
            await receiver.dht_storage.count()
            == OWNER_COUNT
        )

        for index in range(OWNER_COUNT):
            stored = await receiver.dht_storage.get(
                keys[index]
            )

            assert stored == records[index][VERSION_COUNT]

            assert stored.sequence_number == VERSION_COUNT

        print("\nSTORE VERIFICATION: all latest versions valid")

        # ----------------------------------
        # 6. Конкурентные FIND_VALUE
        # ----------------------------------

        read_operations = [
            index
            for _ in range(READS_PER_OWNER)
            for index in range(OWNER_COUNT)
        ]

        assert len(read_operations) == (
            OWNER_COUNT * READS_PER_OWNER
        )

        semaphore = asyncio.Semaphore(
            READ_CONCURRENCY
        )

        read_latencies = []
        read_failures = []

        async def read_record(operation_index, owner_index):
            async with semaphore:
                owner = owners[owner_index]
                key = keys[owner_index]

                expected = records[
                    owner_index
                ][VERSION_COUNT]

                started_at = time.perf_counter()

                try:
                    received = await owner.find_value(
                        receiver.local_contact,
                        key,
                    )

                    elapsed_ms = (
                        time.perf_counter() - started_at
                    ) * 1000

                    assert received == expected, (
                        f"Incorrect record for "
                        f"owner={owner_index}"
                    )

                    read_latencies.append(elapsed_ms)

                    return True

                except Exception as exc:
                    read_failures.append(
                        (
                            operation_index,
                            owner_index,
                            repr(exc),
                        )
                    )
                    return False

        read_started = time.perf_counter()

        read_results = await asyncio.wait_for(
            asyncio.gather(
                *[
                    read_record(operation_index, owner_index)
                    for operation_index, owner_index
                    in enumerate(read_operations)
                ]
            ),
            timeout=60.0,
        )

        read_duration = (
            time.perf_counter() - read_started
        )

        successful_reads = sum(read_results)

        assert successful_reads == len(
            read_operations
        ), (
            f"FIND_VALUE failures: "
            f"{read_failures[:10]}"
        )

        assert not read_failures

        assert len(read_latencies) == len(
            read_operations
        )

        print_statistics(
            "FIND_VALUE",
            read_latencies,
            read_duration,
        )

        # ----------------------------------
        # 7. Финальная проверка хранилища
        # ----------------------------------

        assert (
            await receiver.dht_storage.count()
            == OWNER_COUNT
        )

        for index in range(OWNER_COUNT):
            expected = records[index][VERSION_COUNT]

            stored = await receiver.dht_storage.get(
                keys[index]
            )

            assert stored == expected

        print("\nSTORAGE INTEGRITY: passed")

        # ----------------------------------
        # 8. Проверка сети после нагрузки
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

        print("POST-LOAD NETWORK: all nodes responsive")

        total_requests = (
            len(store_latencies)
            + len(read_latencies)
        )

        assert total_requests == 240

        print(f"\nTOTAL RPC: {total_requests}")
        print("SUCCESSFUL RPC: 240")
        print("FAILED RPC: 0")
        print("DHT STORE/FIND_VALUE LOAD: passed")

    finally:
        for node in reversed(started):
            await node.stop()
