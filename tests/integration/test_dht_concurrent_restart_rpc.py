
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
    Настоящий TCP STORE_REQUEST.

    Возвращаем исходный ответ сервера,
    включая ERROR.
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


def store_result(response):
    """
    True: сервер подтвердил сохранение.
    False: сервер отказал.

    Неизвестный ответ — ошибка теста.
    """

    if response.message_type == MessageType.ERROR:
        return False

    assert response.message_type == MessageType.STORE_RESPONSE, (
        f"Unexpected response: {response.message_type}"
    )

    result = decode_store_response(response.payload)

    assert isinstance(result, dict)
    assert isinstance(result.get("stored"), bool)

    return result["stored"]


@pytest.mark.asyncio
async def test_concurrent_updates_survive_restart(tmp_path):
    """
    Проверяет:
    1. Конкурентную запись версий 2-10.
    2. Сохранение максимальной версии.
    3. Полную остановку получателя.
    4. Восстановление данных с диска.
    5. Доступность через FIND_VALUE.
    6. Защиту от отката после перезапуска.
    7. Приём новой версии после перезапуска.
    """

    ports = set()

    while len(ports) < 4:
        ports.add(free_port())

    ports = list(ports)

    configs = [
        make_config(tmp_path / f"node{i}", ports[i])
        for i in range(4)
    ]

    nodes = [Node(config) for config in configs]

    owner = nodes[0]
    senders = nodes[:3]
    receiver = nodes[3]

    started = []
    restarted_receiver = None

    try:
        # -----------------------------------
        # 1. Запуск узлов
        # -----------------------------------

        for node in nodes:
            await node.start()
            started.append(node)

        for sender in senders:
            await sender.ping(
                host=receiver.local_contact.host,
                port=receiver.local_contact.port,
            )

        # -----------------------------------
        # 2. Подготовка версий
        # -----------------------------------

        address = f"127.0.0.1:{ports[0]}"

        records = {
            version: create_node_record(
                identity=owner.identity,
                addresses=[address],
                sequence_number=version,
                ttl_seconds=300,
            )
            for version in range(1, 12)
        }

        key = node_record_key(records[1].node_id)

        assert all(
            node_record_key(record.node_id) == key
            for record in records.values()
        )

        # -----------------------------------
        # 3. Начальная версия
        # -----------------------------------

        response = await send_store_rpc(
            owner, receiver, records[1]
        )

        assert store_result(response) is True

        assert (
            await receiver.dht_storage.get(key)
            == records[1]
        )

        print("\nINITIAL: version 1 stored")

        # -----------------------------------
        # 4. Конкурентные обновления
        # -----------------------------------

        versions = list(range(2, 11))
        random.Random(2026).shuffle(versions)

        start_event = asyncio.Event()

        async def send_version(index, version):
            await start_event.wait()

            sender = senders[index % len(senders)]

            response = await send_store_rpc(
                sender,
                receiver,
                records[version],
            )

            return version, store_result(response)

        tasks = [
            asyncio.create_task(
                send_version(index, version)
            )
            for index, version in enumerate(versions)
        ]

        start_event.set()

        results = await asyncio.gather(*tasks)

        accepted = [
            version
            for version, successful in results
            if successful
        ]

        assert 10 in accepted, (
            f"Newest version rejected: {results}"
        )

        stored = await receiver.dht_storage.get(key)

        assert stored == records[10]

        assert await owner.find_value(
            receiver.local_contact, key
        ) == records[10]

        print(
            "CONCURRENT UPDATE: version 10 preserved"
        )

        # -----------------------------------
        # 5. Полная остановка получателя
        # -----------------------------------

        original_node_id = receiver.identity.node_id

        await receiver.stop()
        started.remove(receiver)

        print("RECEIVER: stopped")

        # Создаём новый объект Node.
        # Используем тот же каталог состояния
        # и тот же порт.

        restarted_receiver = Node(configs[3])

        await restarted_receiver.start()
        started.append(restarted_receiver)

        assert (
            restarted_receiver.identity.node_id
            == original_node_id
        )

        print("RECEIVER: restarted with same identity")

        # -----------------------------------
        # 6. Восстановление с диска
        # -----------------------------------

        restored = await restarted_receiver.dht_storage.get(
            key
        )

        assert restored is not None, (
            "Record disappeared after restart"
        )

        assert restored.sequence_number == 10

        assert restored == records[10]

        print("DISK RESTORE: version 10 recovered")

        # -----------------------------------
        # 7. Проверка через TCP
        # -----------------------------------

        await owner.ping(
            host=restarted_receiver.local_contact.host,
            port=restarted_receiver.local_contact.port,
        )

        received = await owner.find_value(
            restarted_receiver.local_contact,
            key,
        )

        assert received == records[10]

        print("FIND_VALUE: version 10 available")

        # -----------------------------------
        # 8. Защита от отката
        # -----------------------------------

        for version in range(1, 10):
            response = await send_store_rpc(
                owner,
                restarted_receiver,
                records[version],
            )

            assert store_result(response) is False, (
                f"Rollback to version {version} accepted"
            )

            stored = await restarted_receiver.dht_storage.get(
                key
            )

            assert stored == records[10]

        print("ROLLBACK AFTER RESTART: rejected")

        # -----------------------------------
        # 9. Повторное сохранение версии 10
        # -----------------------------------

        response = await send_store_rpc(
            owner,
            restarted_receiver,
            records[10],
        )

        assert store_result(response) is True

        assert (
            await restarted_receiver.dht_storage.get(key)
            == records[10]
        )

        print("IDENTICAL REPLAY: accepted")

        # -----------------------------------
        # 10. Новая версия после перезапуска
        # -----------------------------------

        response = await send_store_rpc(
            owner,
            restarted_receiver,
            records[11],
        )

        assert store_result(response) is True

        assert (
            await restarted_receiver.dht_storage.get(key)
            == records[11]
        )

        received = await owner.find_value(
            restarted_receiver.local_contact,
            key,
        )

        assert received == records[11]

        print("NEW UPDATE: version 11 accepted")
        print("CONCURRENT RESTART: passed")

    finally:
        for node in reversed(started):
            await node.stop()
