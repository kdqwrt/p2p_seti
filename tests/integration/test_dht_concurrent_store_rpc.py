
import asyncio
import random
import secrets
import socket

import pytest

from p2p.config import NodeConfig
from p2p.node.node import Node
from p2p.dht.records import (
    create_node_record,
    node_record_key,
)
from p2p.dht.messages import (
    encode_store_request,
    decode_store_response,
)
from p2p.protocol.constants import MessageType
from p2p.protocol.frame import Frame
from p2p.transport.connection import send_request


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
        connect_timeout_ms=300,
        read_timeout_ms=1000,
        ping_timeout_ms=300,
        max_frame_payload=65536,
        protocol_version=1,
        log_level="WARNING",
        log_file=str(state_dir / "node.log"),
    )


async def send_store_rpc(sender, receiver, record):
    """
    Отправляет STORE_REQUEST через настоящий TCP.

    Возвращает ответ сервера без преобразования
    ERROR в исключение RpcError.
    """

    key = node_record_key(record.node_id)
    request_id = secrets.token_bytes(16)

    frame = Frame(
        version=sender.config.protocol_version,
        message_type=MessageType.STORE_REQUEST,
        flags=0,
        request_id=request_id,
        payload=encode_store_request(key, record),
    )

    response = await send_request(
        host=receiver.local_contact.host,
        port=receiver.local_contact.port,
        frame=frame,
        connect_timeout=sender.config.connect_timeout,
        response_timeout=sender.config.read_timeout_ms / 1000,
        protocol_version=sender.config.protocol_version,
        max_frame_payload=sender.config.max_frame_payload,
        local_node_label=sender.local_node_label,
    )

    assert response.request_id == request_id

    return response


def classify_store_response(response):
    """
    Классифицирует ответ STORE.

    True  — сервер подтвердил сохранение.
    False — сервер отклонил запись.

    Неизвестный тип ответа приводит к ошибке теста.
    """

    if response.message_type == MessageType.ERROR:
        return False

    assert response.message_type == MessageType.STORE_RESPONSE, (
        f"Unexpected response type: "
        f"{response.message_type}"
    )

    result = decode_store_response(response.payload)

    assert isinstance(result, dict)
    assert isinstance(result.get("stored"), bool)

    return result["stored"]


@pytest.mark.asyncio
async def test_concurrent_store_preserves_newest_version(tmp_path):
    """
    Проверяет конкурентную обработку STORE по TCP.

    Сценарии:
    1. Сохранение первоначальной версии.
    2. Одновременная отправка версий 2-10.
    3. Сохранение максимального sequence_number.
    4. Повторные конкурентные запросы.
    5. Отклонение устаревших версий.
    6. Идемпотентное сохранение актуальной версии.
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

    owner = nodes[0]
    senders = nodes[:3]
    receiver = nodes[3]

    started = []

    try:
        # ----------------------------------------
        # 1. Запуск настоящих TCP-серверов
        # ----------------------------------------

        for node in nodes:
            await node.start()
            started.append(node)

        for sender in senders:
            await sender.ping(
                host=receiver.local_contact.host,
                port=receiver.local_contact.port,
            )

        # ----------------------------------------
        # 2. Подготовка подписанных записей
        # ----------------------------------------

        address = f"127.0.0.1:{ports[0]}"

        records = {
            version: create_node_record(
                identity=owner.identity,
                addresses=[address],
                sequence_number=version,
                ttl_seconds=300,
            )
            for version in range(1, 11)
        }

        key = node_record_key(records[1].node_id)

        assert all(
            node_record_key(record.node_id) == key
            for record in records.values()
        )

        # ----------------------------------------
        # 3. Начальное состояние
        # ----------------------------------------

        initial_response = await send_store_rpc(
            owner,
            receiver,
            records[1],
        )

        assert classify_store_response(
            initial_response
        ) is True

        assert (
            await receiver.dht_storage.get(key)
            == records[1]
        )

        print("\nINITIAL: version 1 stored")

        # ----------------------------------------
        # 4. Конкурентные обновления
        # ----------------------------------------

        rng = random.Random(2026)

        for round_number in range(1, 6):
            versions = list(range(2, 11))
            rng.shuffle(versions)

            start_event = asyncio.Event()

            async def send_version(index, version):
                await start_event.wait()

                sender = senders[
                    index % len(senders)
                ]

                response = await send_store_rpc(
                    sender,
                    receiver,
                    records[version],
                )

                accepted = classify_store_response(
                    response
                )

                return (
                    version,
                    accepted,
                    response.message_type,
                )

            tasks = [
                asyncio.create_task(
                    send_version(index, version)
                )
                for index, version in enumerate(versions)
            ]

            # Все задачи одновременно получают
            # разрешение начать отправку.
            start_event.set()

            # Не скрываем исключения.
            # Любая неожиданная сетевая ошибка
            # должна приводить к падению теста.
            results = await asyncio.gather(*tasks)

            accepted_versions = [
                version
                for version, accepted, _ in results
                if accepted
            ]

            rejected_versions = [
                version
                for version, accepted, _ in results
                if not accepted
            ]

            # Версия 10 должна быть принята:
            # она не устаревшая и не конфликтует
            # с уже сохранённой версией 10.
            assert 10 in accepted_versions, (
                f"Round {round_number}: "
                f"version 10 was rejected. "
                f"Results: {results}"
            )

            # Отказ для максимальной версии
            # недопустим. Для младших версий
            # отказ может быть нормальным.
            assert all(
                version < 10
                for version in rejected_versions
            )

            # ------------------------------------
            # 5. Проверка итогового состояния
            # ------------------------------------

            stored = await receiver.dht_storage.get(key)

            assert stored is not None

            assert stored.sequence_number == 10, (
                f"Round {round_number}: "
                f"expected version 10, "
                f"got {stored.sequence_number}"
            )

            assert stored == records[10]

            received = await owner.find_value(
                receiver.local_contact,
                key,
            )

            assert received == records[10]

            print(
                f"ROUND {round_number}: "
                f"version 10 preserved, "
                f"accepted={len(accepted_versions)}/9, "
                f"rejected={len(rejected_versions)}/9"
            )

        # ----------------------------------------
        # 6. Проверка отката после конкуренции
        # ----------------------------------------

        for version in range(1, 10):
            response = await send_store_rpc(
                owner,
                receiver,
                records[version],
            )

            accepted = classify_store_response(
                response
            )

            assert accepted is False, (
                f"Old version {version} was accepted"
            )

            stored = await receiver.dht_storage.get(key)

            assert stored == records[10]

        print("OLD VERSIONS: rejected")

        # ----------------------------------------
        # 7. Идемпотентность актуальной версии
        # ----------------------------------------

        response = await send_store_rpc(
            owner,
            receiver,
            records[10],
        )

        assert classify_store_response(
            response
        ) is True

        stored = await receiver.dht_storage.get(key)

        assert stored == records[10]

        received = await owner.find_value(
            receiver.local_contact,
            key,
        )

        assert received == records[10]

        print("LATEST VERSION: idempotent")
        print("CONCURRENT STORE: passed")

    finally:
        for node in reversed(started):
            await node.stop()
