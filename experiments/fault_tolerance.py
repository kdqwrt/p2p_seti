
import argparse
import asyncio
import json
import socket
import sys
import tempfile
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from p2p.config import NodeConfig
from p2p.node.node import Node


SCENARIOS = (
    "normal",
    "bootstrap-failure",
    "alternative-path",
    "node-failure",
    "recovery",
)


def free_ports(count: int) -> list[int]:
    sockets = []
    try:
        for _ in range(count):
            sock = socket.socket()
            sock.bind(("127.0.0.1", 0))
            sockets.append(sock)
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


def metrics(result) -> dict:
    return {
        "found": result.found,
        "rpc_count": result.rpc_count,
        "iterations": result.iterations,
        "timeouts": result.timeouts,
        "duration_ms": round(result.duration_ms, 3),
        "contacts_count": len(result.closest_contacts),

        "verification_count": result.verification_count,
        "verification_failures": result.verification_failures,
        "verification_duration_ms": (
            result.verification_duration_ms
        ),
        "failed_contacts": result.failed_contacts or [],
    }


async def connect(source: Node, destination: Node) -> None:
    await source.ping(
        destination.local_contact.host,
        destination.local_contact.port,
        expected_node_id=destination.identity.node_id,
    )

async def run_recovery(nodes: list[Node], started: list[Node]) -> dict:
    node1, node2, node3, node4, node5 = nodes

    # Создаём связи между участниками сети.
    await connect(node1, node2)
    await connect(node2, node3)
    await connect(node3, node4)
    await connect(node4, node5)

    original_node_id = node3.identity.node_id
    original_contact = node3.local_contact
    original_config = node3.config

    # Проверяем, что узел доступен до отказа.
    await connect(node2, node3)
    before_available = True

    # Останавливаем узел 3.
    await node3.stop()
    started.remove(node3)

    # Проверяем реальную недоступность по RPC.
    try:
        await node2.ping(
            original_contact.host,
            original_contact.port,
            expected_node_id=original_node_id,
        )
        unavailable_after_stop = False
    except (OSError, ConnectionError):
        unavailable_after_stop = True

    # Создаём новый объект с прежним каталогом состояния.
    restored_node = Node(original_config)

    identity_preserved = (
        restored_node.identity.node_id == original_node_id
    )

    # Запускаем восстановленный узел.
    await restored_node.start()
    started.append(restored_node)

    # Проверяем доступность после восстановления.
    try:
        await connect(node2, restored_node)
        available_after_restart = True
    except (OSError, ConnectionError):
        available_after_restart = False

    # Повторно знакомим восстановленный узел с соседями.
    if available_after_restart:
        await connect(restored_node, node4)

    # Проверяем поиск узла 5 после восстановления.
    lookup = await node2.lookup_node(
        node5.identity.node_id,
        collect_trace=True,
    )

    success = (
        before_available
        and unavailable_after_stop
        and identity_preserved
        and available_after_restart
        and lookup.found
    )

    return {
        "before_available": before_available,
        "unavailable_after_stop": unavailable_after_stop,
        "identity_preserved": identity_preserved,
        "available_after_restart": available_after_restart,
        "original_node_id": original_node_id.hex(),
        "restored_node_id": restored_node.identity.node_id.hex(),
        "lookup": metrics(lookup),
        "trace": [
            {
                "iteration": event.iteration,
                "peer_node_id": event.peer_node_id,
                "status": event.status,
                "returned_contacts": event.returned_contacts,
                "duration_ms": event.duration_ms,
            }
            for event in (lookup.trace or [])
        ],
        "success": success,
    }


async def run_once(scenario: str, run_number: int) -> dict:
    base = NodeConfig.from_file(
        str(ROOT / "configs" / "node1.json")
    )
    ports = free_ports(5)

    # Все узлы запускаются без bootstrap.
    # Так мы полностью контролируем начальную топологию.
    with tempfile.TemporaryDirectory(prefix="p2p-fault-") as temp:
        root = Path(temp)
        nodes = []

        for i, port in enumerate(ports):
            config = replace(
                base,
                listen_host="127.0.0.1",
                listen_port=port,
                node_state_dir=str(root / f"node{i + 1}"),
                log_file=str(root / f"node{i + 1}.log"),
                bootstrap_peers=[],
            )
            nodes.append(Node(config))

        started = []

        try:
            for node in nodes:
                await node.start()
                started.append(node)

            node1, node2, node3, node4, node5 = nodes

            if scenario == "recovery":
                recovery_result = await run_recovery(
                    nodes,
                    started,
                )

                return {
                    "timestamp_utc": datetime.now(
                        timezone.utc
                    ).isoformat(),
                    "scenario": scenario,
                    "run": run_number,
                    "node_count": len(nodes),
                    "k": base.k_bucket_size,
                    "alpha": base.alpha,
                    **recovery_result,
                }

            if scenario == "normal":
                # 1 знает 2; 2 знает 3; 3 знает 4; 4 знает 5.
                await connect(node1, node2)
                await connect(node2, node3)
                await connect(node3, node4)
                await connect(node4, node5)

                seeker = node1
                target = node5
                failed_node = None

            elif scenario == "bootstrap-failure":
                # Узел 1 — bootstrap.
                # Остальные узлы знакомятся с ним,
                # а затем между собой.
                for node in nodes[1:]:
                    await connect(node, node1)

                for node in nodes[1:]:
                    for other in nodes[1:]:
                        if node is not other:
                            await connect(node, other)

                seeker = node2
                target = node5
                failed_node = node1


            elif scenario == "alternative-path":

                # Узел 1 знает двух соседей.

                # Узел 2 откажет, но есть путь 1 -> 3 -> 4 -> 5.

                await connect(node1, node2)

                await connect(node1, node3)

                await connect(node3, node4)

                await connect(node4, node5)

                seeker = node1

                target = node5

                failed_node = node2


            elif scenario == "node-failure":

                # Отказ промежуточного узла 3.

                # Основной путь: 1 -> 2 -> 3 -> 4 -> 5.

                # Резервная связь: 2 -> 4.

                await connect(node1, node2)

                await connect(node2, node3)

                await connect(node3, node4)

                await connect(node2, node4)

                await connect(node4, node5)

                seeker = node1

                target = node5

                failed_node = node3

            else:
                raise ValueError(f"Unknown scenario: {scenario}")

            initially_known = (
                seeker.routing_table.get_contact(
                    target.identity.node_id
                ) is not None
            )

            # Для проверки альтернативного маршрута
            # цель не должна быть известна заранее.
            if scenario in ("normal", "alternative-path", "node-failure"):
                if initially_known:
                    raise AssertionError(
                        "Target is already known before lookup"
                    )

            if failed_node is not None:
                await failed_node.stop()
                started.remove(failed_node)

            result = await seeker.lookup_node(
                target.identity.node_id,
                collect_trace=True,
            )

            success = result.found

            if scenario in ("alternative-path", "node-failure"):
                success = (
                        result.found
                        and not initially_known
                        and seeker.routing_table.get_contact(
                    target.identity.node_id
                ) is not None
                )


            failed_node_was_queried = (
                failed_node is not None
                and any(
                    event.peer_node_id == failed_node.identity.node_id.hex()
                    for event in (result.trace or [])
                )
            )

            failed_node_error_observed = (
                failed_node is not None
                and any(
                    event.peer_node_id == failed_node.identity.node_id.hex()
                    and event.status in (
                        "timeout_or_connection_error",
                        "error",
                    )
                    for event in (result.trace or [])
                )
            )
            failed_node_verification_failed = (
                failed_node is not None
                and failed_node.identity.node_id.hex()
                in (result.failed_contacts or [])
            )

            report = {
                "timestamp_utc": datetime.now(
                    timezone.utc
                ).isoformat(),
                "scenario": scenario,
                "run": run_number,
                "node_count": len(nodes),
                "k": base.k_bucket_size,
                "alpha": base.alpha,
                "seeker_node_id": seeker.identity.node_id.hex(),
                "target_node_id": target.identity.node_id.hex(),
                "failed_node_id": (
                    failed_node.identity.node_id.hex()
                    if failed_node is not None
                    else None
                ),
                "target_initially_known": initially_known,
                                "lookup": metrics(result),
                "trace": [
                    {
                        "iteration": event.iteration,
                        "peer_node_id": event.peer_node_id,
                        "status": event.status,
                        "returned_contacts": event.returned_contacts,
                        "duration_ms": event.duration_ms,
                    }
                    for event in (result.trace or [])
                ],
                "success": success,
                "failed_node_was_queried": failed_node_was_queried,
                "failed_node_error_observed": failed_node_error_observed,
                "failed_node_verification_failed": (
                    failed_node_verification_failed
                ),
            }

            return report

        finally:
            for node in reversed(started):
                await node.stop()


async def run_experiments(scenario: str, runs: int) -> None:
    results = []

    print(f"\nСценарий: {scenario}")
    print(f"Количество запусков: {runs}")

    for run_number in range(1, runs + 1):
        print(f"\n--- Запуск {run_number}/{runs} ---")

        try:
            result = await run_once(scenario, run_number)
            results.append(result)

            lookup = result["lookup"]

            if scenario == "recovery":
                print(
                    "Доступен до отказа:",
                    result["before_available"],
                )
                print(
                    "Недоступен после остановки:",
                    result["unavailable_after_stop"],
                )
                print(
                    "NodeID сохранён:",
                    result["identity_preserved"],
                )
                print(
                    "Доступен после восстановления:",
                    result["available_after_restart"],
                )

            print(f"Найден: {lookup['found']}")
            print(f"RPC: {lookup['rpc_count']}")
            print(f"Итераций: {lookup['iterations']}")
            print(f"Таймаутов: {lookup['timeouts']}")
            print(f"Время: {lookup['duration_ms']:.2f} мс")
            print(
                "Проверок PING:",
                lookup["verification_count"],
            )
            print(
                "Неудачных проверок PING:",
                lookup["verification_failures"],
            )
            print(
                "Суммарное время проверок:",
                f"{lookup['verification_duration_ms']:.2f} мс",
            )
            print(f"Успех: {result['success']}")
            if scenario in ("alternative-path", "node-failure"):
                print(
                    "Отказавший узел опрошен:",
                    result["failed_node_was_queried"],
                )
                print(
                    "Отказ обнаружен при проверке PING:",
                    result["failed_node_verification_failed"],
                )
                print(
                    "Ошибка отказавшего узла обнаружена:",
                    result["failed_node_error_observed"],
                )
            print("Трассировка FIND_NODE:")

            for event in result.get("trace", []):
                print(
                    f"  Итерация {event['iteration']} | "
                    f"Узел {event['peer_node_id'][:12]} | "
                    f"{event['status']} | "
                    f"Контактов: {len(event['returned_contacts'])} | "
                    f"{event['duration_ms']:.2f} мс"
                )

        except Exception as exc:
            print(f"Ошибка эксперимента: {exc}")
            results.append({
                "scenario": scenario,
                "run": run_number,
                "success": False,
                "error": str(exc),
            })

    successful = sum(
        1 for result in results if result["success"]
    )

    durations = [
        result["lookup"]["duration_ms"]
        for result in results
        if result["success"] and "lookup" in result
    ]

    summary = {
        "scenario": scenario,
        "runs": runs,
        "successful": successful,
        "failed": runs - successful,
        "success_rate": round(successful / runs, 4),
        "average_success_duration_ms": (
            round(sum(durations) / len(durations), 3)
            if durations else None
        ),
    }

    output_dir = ROOT / "experiments" / "results"
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%S%fZ"
    )
    output_path = output_dir / f"{scenario}_{timestamp}.json"

    output_path.write_text(
        json.dumps(
            {"summary": summary, "runs": results},
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    print("\n=== ИТОГ ===")
    print(f"Успешно: {successful}/{runs}")
    print(f"Доля успешных поисков: {summary['success_rate']:.2%}")
    print(f"Отчёт: {output_path}")

    if successful != runs:
        raise SystemExit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="P2P lookup fault-tolerance experiments"
    )
    parser.add_argument(
        "--scenario",
        choices=SCENARIOS,
        default="normal",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=1,
    )

    args = parser.parse_args()

    if args.runs < 1:
        parser.error("--runs must be >= 1")

    asyncio.run(
        run_experiments(args.scenario, args.runs)
    )


if __name__ == "__main__":
    main()
