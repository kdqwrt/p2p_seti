
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


OWNER_COUNT = 4
MAX_VERSION = 5


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
    Отправляет настоящий STORE_REQUEST по TCP.

    Возвращает исходный ответ сервера.
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
    True: STORE подтверждён.
    False: сервер вернул отказ.

    Неизвестный ответ приводит к падению теста.
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


async def assert_all_latest(owners, receiver, records, keys):
    """
    Проверяет все ключи через локальное хранилище
    и настоящий FIND_VALUE RPC.
    """

    for index, owner in enumerate(owners):
        key = keys[index]
        expected = records[index][MAX_VERSION]

        stored = await receiver.dht_storage.get(key)

        assert stored == expected, (
            f"Owner {index}: wrong stored record"
        )

        received = await owner.find_value(
            receiver.local_contact,
            key,
        )

        assert received == expected, (
            f"Owner {index}: wrong FIND_VALUE result"
        )


@pytest.mark.asyncio
async def test_multi_key_concurrent_updates_survive_restart(
    tmp_path,
):
    """
    Проверяет:
    1. Четыре независимых DHT-ключа.
    2. Конкурентные обновления через TCP.
    3. Сохранение максимальной версии каждого ключа.
    4. Восстановление всех записей после перезапуска.
    5. Независимую защиту каждого ключа от отката.
    6. Обновление одного ключа без изменения остальных.
    """

    ports = set()

    while len(ports) < OWNER_COUNT + 1:
        ports.add(free_port())

    ports = list(ports)

    configs = [
        make_config(
            tmp_path / f"node{i}",
            ports[i],
        )
        for i in range(OWNER_COUNT + 1)
    ]

    nodes = [Node(config) for config in configs]

    owners = nodes[:OWNER_COUNT]
    receiver = nodes[OWNER_COUNT]

    started = []

    try:
        # ------------------------------------
        # 1. Запуск узлов
        # ------------------------------------

        for node in nodes:
            await node.start()
            started.append(node)

        for owner in owners:
            await owner.ping(
                host=receiver.local_contact.host,
                port=receiver.local_contact.port,
            )

        # ------------------------------------
        # 2. Независимые подписанные записи
        # ------------------------------------

        records = {}
        keys = {}

        for index, owner in enumerate(owners):
            address = f"127.0.0.1:{ports[index]}"

            records[index] = {
                version: create_node_record(
                    identity=owner.identity,
                    addresses=[address],
                    sequence_number=version,
                    ttl_seconds=300,
                )
                for version in range(1, MAX_VERSION + 2)
            }

            keys[index] = node_record_key(
                records[index][1].node_id
            )

            assert all(
                node_record_key(record.node_id)
                == keys[index]
                for record in records[index].values()
            )

        # Разные владельцы должны иметь разные ключи.
        assert len(set(keys.values())) == OWNER_COUNT

        print("\nKEYS: 4 independent owners")

        # ------------------------------------
        # 3. Первоначальная запись
        # ------------------------------------

        for index, owner in enumerate(owners):
            response = await send_store_rpc(
                owner,
                receiver,
                records[index][1],
            )

            assert store_result(response) is True

            assert (
                await receiver.dht_storage.get(keys[index])
                == records[index][1]
            )

        assert await receiver.dht_storage.count() == OWNER_COUNT

        print("INITIAL: 4 records stored")

        # ------------------------------------
        # 4. Конкурентные обновления
        # ------------------------------------

        operations = [
            (index, version)
            for index in range(OWNER_COUNT)
            for version in range(1, MAX_VERSION + 1)
        ]

        random.Random(2026).shuffle(operations)

        start_event = asyncio.Event()

        async def send_update(index, version):
            await start_event.wait()

            response = await send_store_rpc(
                owners[index],
                receiver,
                records[index][version],
            )

            return index, version, store_result(response)

        tasks = [
            asyncio.create_task(
                send_update(index, version)
            )
            for index, version in operations
        ]

        start_event.set()

        results = await asyncio.gather(*tasks)

        assert len(results) == OWNER_COUNT * MAX_VERSION

        # Каждая максимальная версия должна
        # успешно пройти хотя бы один STORE.
        for index in range(OWNER_COUNT):
            assert (
                index,
                MAX_VERSION,
                True,
            ) in results, (
                f"Owner {index}: newest version rejected"
            )

        await assert_all_latest(
            owners,
            receiver,
            records,
            keys,
        )

        assert await receiver.dht_storage.count() == OWNER_COUNT

        print("CONCURRENT STORE: 4 latest versions preserved")

        # ------------------------------------
        # 5. Перезапуск получателя
        # ------------------------------------

        original_node_id = receiver.identity.node_id

        await receiver.stop()
        started.remove(receiver)

        print("RECEIVER: stopped")

        # Новый объект с прежним состоянием.
        restarted_receiver = Node(configs[OWNER_COUNT])

        await restarted_receiver.start()
        started.append(restarted_receiver)

        assert (
            restarted_receiver.identity.node_id
            == original_node_id
        )

        print("RECEIVER: restarted")

        # ------------------------------------
        # 6. Восстановление всех ключей
        # ------------------------------------

        assert (
            await restarted_receiver.dht_storage.count()
            == OWNER_COUNT
        )

        await assert_all_latest(
            owners,
            restarted_receiver,
            records,
            keys,
        )

        print("DISK RESTORE: all 4 records recovered")

        # ------------------------------------
        # 7. Защита от отката после перезапуска
        # ------------------------------------

        for index, owner in enumerate(owners):
            for version in range(1, MAX_VERSION):
                response = await send_store_rpc(
                    owner,
                    restarted_receiver,
                    records[index][version],
                )

                assert store_result(response) is False, (
                    f"Rollback accepted: "
                    f"owner={index}, version={version}"
                )

                assert (
                    await restarted_receiver.dht_storage.get(
                        keys[index]
                    )
                    == records[index][MAX_VERSION]
                )

        await assert_all_latest(
            owners,
            restarted_receiver,
            records,
            keys,
        )

        print("ROLLBACK: all 4 keys protected")

        # ------------------------------------
        # 8. Обновляем только одного владельца
        # ------------------------------------

        response = await send_store_rpc(
            owners[0],
            restarted_receiver,
            records[0][MAX_VERSION + 1],
        )

        assert store_result(response) is True

        assert (
            await restarted_receiver.dht_storage.get(keys[0])
            == records[0][MAX_VERSION + 1]
        )

        # Остальные ключи должны остаться
        # на прежней версии.
        for index in range(1, OWNER_COUNT):
            stored = await restarted_receiver.dht_storage.get(
                keys[index]
            )

            assert stored == records[index][MAX_VERSION]

            received = await owners[index].find_value(
                restarted_receiver.local_contact,
                keys[index],
            )

            assert received == records[index][MAX_VERSION]

        received = await owners[0].find_value(
            restarted_receiver.local_contact,
            keys[0],
        )

        assert received == records[0][MAX_VERSION + 1]

        assert (
            await restarted_receiver.dht_storage.count()
            == OWNER_COUNT
        )

        print("ISOLATED UPDATE: other keys unchanged")
        print("MULTI-KEY CONCURRENT RESTART: passed")

    finally:
        for node in reversed(started):
            await node.stop()
