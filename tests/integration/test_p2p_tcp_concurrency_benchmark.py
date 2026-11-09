
import asyncio
import math
import random
import socket
import statistics
import time

import pytest

from p2p.config import NodeConfig
from p2p.node.node import Node


NODE_COUNT = 10
REQUEST_COUNT = 1000
CONCURRENCY_LEVELS = (20, 50, 100)
RANDOM_SEED = 2026


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


def percentile(values, percentage):
    assert values

    ordered = sorted(values)
    rank = math.ceil(len(ordered) * percentage / 100)

    return ordered[max(0, rank - 1)]


def build_operations():
    """
    Создаём одинаковую нагрузку для всех испытаний.

    Каждая операция — PING между двумя
    различными узлами.
    """

    rng = random.Random(RANDOM_SEED)
    operations = []

    for _ in range(REQUEST_COUNT):
        sender = rng.randrange(NODE_COUNT)

        receiver = rng.randrange(NODE_COUNT - 1)

        if receiver >= sender:
            receiver += 1

        operations.append((sender, receiver))

    assert len(operations) == REQUEST_COUNT

    assert all(
        sender != receiver
        for sender, receiver in operations
    )

    return operations


async def run_benchmark(nodes, operations, concurrency):
    """
    Выполняет одну серию запросов.

    Время ожидания свободного слота семафора
    не включается в задержку отдельного PING,
    но включается в общее время серии.
    """

    semaphore = asyncio.Semaphore(concurrency)

    latencies = []
    failures = []

    async def execute(index, sender_index, receiver_index):
        async with semaphore:
            sender = nodes[sender_index]
            receiver = nodes[receiver_index]

            started_at = time.perf_counter()

            try:
                rtt_ms = await sender.ping(
                    host=receiver.local_contact.host,
                    port=receiver.local_contact.port,
                    update_routing=False,
                    expected_node_id=receiver.identity.node_id,
                )

                elapsed_ms = (
                    time.perf_counter() - started_at
                ) * 1000

                assert isinstance(rtt_ms, (int, float))
                assert math.isfinite(rtt_ms)
                assert rtt_ms >= 0

                assert math.isfinite(elapsed_ms)
                assert elapsed_ms >= 0

                latencies.append(elapsed_ms)

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

    started_at = time.perf_counter()

    results = await asyncio.wait_for(
        asyncio.gather(
            *[
                execute(index, sender, receiver)
                for index, (sender, receiver)
                in enumerate(operations)
            ]
        ),
        timeout=120.0,
    )

    duration = time.perf_counter() - started_at

    successful = sum(results)
    failed = len(results) - successful

    if failures:
        print("\nFAILURE DETAILS:")
        for failure in failures[:10]:
            print(f"  {failure}")

    assert len(results) == REQUEST_COUNT

    assert successful == REQUEST_COUNT, (
        f"Concurrency={concurrency}: "
        f"{failed} failed requests. "
        f"Examples: {failures[:10]}"
    )

    assert not failures
    assert len(latencies) == REQUEST_COUNT
    assert duration > 0

    return {
        "concurrency": concurrency,
        "successful": successful,
        "failed": failed,
        "duration": duration,
        "mean": statistics.mean(latencies),
        "median": statistics.median(latencies),
        "p95": percentile(latencies, 95),
        "p99": percentile(latencies, 99),
        "maximum": max(latencies),
        "throughput": REQUEST_COUNT / duration,
    }


async def check_network(nodes):
    """
    Проверяет работоспособность всех узлов.
    """

    for index, sender in enumerate(nodes):
        receiver = nodes[(index + 1) % len(nodes)]

        rtt_ms = await sender.ping(
            host=receiver.local_contact.host,
            port=receiver.local_contact.port,
            update_routing=False,
            expected_node_id=receiver.identity.node_id,
        )

        assert isinstance(rtt_ms, (int, float))
        assert math.isfinite(rtt_ms)
        assert rtt_ms >= 0


@pytest.mark.asyncio
async def test_tcp_concurrency_benchmark(tmp_path):
    """
    10 настоящих TCP-узлов.

    Три серии по 1000 PING:
        concurrency=20
        concurrency=50
        concurrency=100

    Проверяет:
    - успешность запросов;
    - отсутствие сетевых ошибок;
    - отсутствие зависаний;
    - работоспособность узлов после нагрузки;
    - корректность измеряемых показателей.
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
        # ------------------------------------
        # 1. Запуск сети
        # ------------------------------------

        for node in nodes:
            await node.start()
            started.append(node)

        assert len(started) == NODE_COUNT

        assert len({
            node.identity.node_id
            for node in nodes
        }) == NODE_COUNT

        print(f"\nNODES: {NODE_COUNT} started")

        # ------------------------------------
        # 2. Прогрев сети
        # ------------------------------------

        for _ in range(3):
            await check_network(nodes)

        print("WARMUP: completed")

        # ------------------------------------
        # 3. Подготовка одинаковых операций
        # ------------------------------------

        operations = build_operations()

        print(f"REQUESTS PER LEVEL: {REQUEST_COUNT}")

        measurements = []

        # ------------------------------------
        # 4. Три уровня нагрузки
        # ------------------------------------

        for concurrency in CONCURRENCY_LEVELS:
            print(
                f"\n--- CONCURRENCY {concurrency} ---"
            )

            result = await run_benchmark(
                nodes,
                operations,
                concurrency,
            )

            measurements.append(result)

            print(
                f"SUCCESS: {result['successful']}"
            )
            print(
                f"FAILED: {result['failed']}"
            )
            print(
                f"DURATION: {result['duration']:.3f} s"
            )
            print(
                f"MEAN: {result['mean']:.2f} ms"
            )
            print(
                f"MEDIAN: {result['median']:.2f} ms"
            )
            print(
                f"P95: {result['p95']:.2f} ms"
            )
            print(
                f"P99: {result['p99']:.2f} ms"
            )
            print(
                f"MAX: {result['maximum']:.2f} ms"
            )
            print(
                f"THROUGHPUT: "
                f"{result['throughput']:.2f} req/s"
            )

            # Проверка после каждой серии.
            await check_network(nodes)

            print("POST-LOAD CHECK: passed")

        # ------------------------------------
        # 5. Сравнение результатов
        # ------------------------------------

        assert len(measurements) == len(
            CONCURRENCY_LEVELS
        )

        assert sum(
            item["successful"]
            for item in measurements
        ) == REQUEST_COUNT * len(
            CONCURRENCY_LEVELS
        )

        assert all(
            item["failed"] == 0
            for item in measurements
        )

        print("\n=== COMPARISON ===")

        print(
            f"{'CONC':>6} "
            f"{'REQ/S':>12} "
            f"{'MEAN MS':>12} "
            f"{'P95 MS':>12} "
            f"{'P99 MS':>12}"
        )

        for item in measurements:
            print(
                f"{item['concurrency']:>6} "
                f"{item['throughput']:>12.2f} "
                f"{item['mean']:>12.2f} "
                f"{item['p95']:>12.2f} "
                f"{item['p99']:>12.2f}"
            )

        fastest = max(
            measurements,
            key=lambda item: item["throughput"],
        )

        print(
            "\nHIGHEST MEASURED THROUGHPUT: "
            f"concurrency={fastest['concurrency']}, "
            f"{fastest['throughput']:.2f} req/s"
        )

        print(
            "TOTAL SUCCESSFUL REQUESTS: "
            f"{REQUEST_COUNT * len(CONCURRENCY_LEVELS)}"
        )

        print("TCP CONCURRENCY BENCHMARK: passed")

    finally:
        for node in reversed(started):
            await node.stop()
