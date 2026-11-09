
import asyncio
import math
import socket
import statistics
import time

import pytest

from p2p.config import NodeConfig
from p2p.node.node import Node


NODE_COUNT = 10
REQUEST_COUNT = 200
CONCURRENCY = 20


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
        read_timeout_ms=3000,
        ping_timeout_ms=3000,
        max_frame_payload=65536,
        protocol_version=1,
        log_level="WARNING",
        log_file=str(state_dir / "node.log"),
    )


def percentile(values, percentage):
    """
    Вычисляет процентиль по отсортированным данным
    методом nearest rank.
    """
    assert values

    ordered = sorted(values)
    rank = math.ceil(
        percentage / 100 * len(ordered)
    )

    return ordered[max(0, rank - 1)]


@pytest.mark.asyncio
async def test_tcp_ping_under_concurrent_load(tmp_path):
    """
    Нагрузочная проверка реальных TCP PING/PONG.

    10 узлов.
    200 запросов.
    До 20 одновременно выполняемых запросов.

    Проверяем:
    - отсутствие сетевых ошибок;
    - корректность PING/PONG;
    - отсутствие зависших запросов;
    - измерение задержек;
    - сохранение работоспособности сети.
    """

    ports = set()

    while len(ports) < NODE_COUNT:
        ports.add(free_port())

    ports = list(ports)

    nodes = [
        Node(
            make_config(
                tmp_path / f"node{i}",
                ports[i],
            )
        )
        for i in range(NODE_COUNT)
    ]

    started = []

    try:
        # ----------------------------------
        # 1. Запуск всех узлов
        # ----------------------------------

        for node in nodes:
            await node.start()
            started.append(node)

        assert len(started) == NODE_COUNT

        node_ids = [
            node.identity.node_id
            for node in nodes
        ]

        assert len(set(node_ids)) == NODE_COUNT

        print(f"\nNODES: {NODE_COUNT} started")

        # ----------------------------------
        # 2. Подготовка запросов
        # ----------------------------------

        # Детерминированно распределяем запросы
        # между разными парами узлов.

        operations = []

        for index in range(REQUEST_COUNT):
            sender_index = index % NODE_COUNT

            # Смещение 1..9 гарантирует,
            # что узел не отправляет PING самому себе.
            offset = 1 + (
                (index // NODE_COUNT)
                % (NODE_COUNT - 1)
            )

            receiver_index = (
                sender_index + offset
            ) % NODE_COUNT

            assert sender_index != receiver_index

            operations.append(
                (sender_index, receiver_index)
            )

        assert len(operations) == REQUEST_COUNT

        # Проверяем, что нагрузка распределена
        # между разными направлениями.
        assert len(set(operations)) > NODE_COUNT

        # ----------------------------------
        # 3. Ограничение конкурентности
        # ----------------------------------

        semaphore = asyncio.Semaphore(CONCURRENCY)

        latencies_ms = []
        failures = []

        async def execute_ping(index, sender_index,
                               receiver_index):
            async with semaphore:
                sender = nodes[sender_index]
                receiver = nodes[receiver_index]

                started_at = time.perf_counter()

                try:
                    rtt_ms = await sender.ping(
                        host=receiver.local_contact.host,
                        port=receiver.local_contact.port,
                        update_routing=False,
                        expected_node_id=(
                            receiver.identity.node_id
                        ),
                    )

                    elapsed_ms = (
                        time.perf_counter() - started_at
                    ) * 1000

                    assert isinstance(
                        rtt_ms, (int, float)
                    )

                    assert math.isfinite(rtt_ms)
                    assert rtt_ms >= 0

                    assert math.isfinite(elapsed_ms)
                    assert elapsed_ms >= 0

                    latencies_ms.append(elapsed_ms)

                    return True

                except Exception as exc:
                    failures.append(
                        (
                            index,
                            sender_index,
                            receiver_index,
                            repr(exc),
                        )
                    )
                    return False

        # ----------------------------------
        # 4. Запуск нагрузки
        # ----------------------------------

        started_at = time.perf_counter()

        results = await asyncio.wait_for(
            asyncio.gather(
                *[
                    execute_ping(index, sender, receiver)
                    for index, (sender, receiver)
                    in enumerate(operations)
                ]
            ),
            timeout=60.0,
        )

        duration = time.perf_counter() - started_at

        successful = sum(results)
        failed = len(results) - successful

        # ----------------------------------
        # 5. Проверка корректности
        # ----------------------------------

        print(f"REQUESTS: {REQUEST_COUNT}")
        print(f"CONCURRENCY: {CONCURRENCY}")
        print(f"SUCCESSFUL: {successful}")
        print(f"FAILED: {failed}")

        if failures:
            print("FAILURE DETAILS:")
            for failure in failures[:10]:
                print(f"  {failure}")

        assert len(results) == REQUEST_COUNT

        assert successful == REQUEST_COUNT, (
            f"{failed} TCP requests failed: "
            f"{failures[:10]}"
        )

        assert not failures

        assert len(latencies_ms) == REQUEST_COUNT

        assert duration > 0

        # ----------------------------------
        # 6. Статистика производительности
        # ----------------------------------

        mean_ms = statistics.mean(latencies_ms)
        median_ms = statistics.median(latencies_ms)
        p95_ms = percentile(latencies_ms, 95)
        maximum_ms = max(latencies_ms)

        requests_per_second = (
            REQUEST_COUNT / duration
        )

        print(f"DURATION: {duration:.3f} s")
        print(f"MEAN RTT: {mean_ms:.2f} ms")
        print(f"MEDIAN RTT: {median_ms:.2f} ms")
        print(f"P95 RTT: {p95_ms:.2f} ms")
        print(f"MAX RTT: {maximum_ms:.2f} ms")
        print(
            "THROUGHPUT: "
            f"{requests_per_second:.2f} requests/s"
        )

        # Не задаём жёсткие пороги задержки:
        # они зависят от компьютера и ОС.
        assert all(
            math.isfinite(value) and value >= 0
            for value in latencies_ms
        )

        assert 0 <= median_ms <= maximum_ms
        assert 0 <= p95_ms <= maximum_ms

        # ----------------------------------
        # 7. Проверка после нагрузки
        # ----------------------------------

        # Последовательно проверяем кольцо
        # из всех 10 узлов.
        for index in range(NODE_COUNT):
            sender = nodes[index]
            receiver = nodes[
                (index + 1) % NODE_COUNT
            ]

            rtt_ms = await sender.ping(
                host=receiver.local_contact.host,
                port=receiver.local_contact.port,
                update_routing=False,
                expected_node_id=(
                    receiver.identity.node_id
                ),
            )

            assert math.isfinite(rtt_ms)
            assert rtt_ms >= 0

        print("POST-LOAD CHECK: all nodes responsive")
        print("TCP LOAD TEST: passed")

    finally:
        for node in reversed(started):
            await node.stop()
