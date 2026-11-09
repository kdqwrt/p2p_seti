
import secrets
import socket

import pytest
import asyncio
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
from p2p.protocol.messages import decode_error
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
        connect_timeout_ms=1000,
        read_timeout_ms=5000,
        ping_timeout_ms=5000,
        max_frame_payload=65536,
        protocol_version=1,
        log_level="WARNING",
        log_file=str(state_dir / "node.log"),
    )


async def raw_store(sender, receiver, record):
    """
    Отправляет настоящий TCP STORE_REQUEST
    и возвращает исходный Frame ответа.
    """

    key = node_record_key(record.node_id)
    request_id = secrets.token_bytes(16)

    request = Frame(
        version=sender.config.protocol_version,
        message_type=MessageType.STORE_REQUEST,
        flags=0,
        request_id=request_id,
        payload=encode_store_request(key, record),
    )

    response = await send_request(
        host=receiver.local_contact.host,
        port=receiver.local_contact.port,
        frame=request,
        connect_timeout=sender.config.connect_timeout,
        response_timeout=sender.config.read_timeout_ms / 1000,
        protocol_version=sender.config.protocol_version,
        max_frame_payload=sender.config.max_frame_payload,
        local_node_label=sender.local_node_label,
    )

    assert response.request_id == request_id
    assert response.version == request.version

    return response


def assert_store_success(response, expected_key):
    assert response.message_type == MessageType.STORE_RESPONSE, (
        f"Expected STORE_RESPONSE, "
        f"got {response.message_type.name}"
    )

    payload = decode_store_response(response.payload)

    assert payload["key"] == expected_key
    assert payload["stored"] is True


def assert_error_code(response, expected_code):
    assert response.message_type == MessageType.ERROR, (
        f"Expected ERROR {expected_code}, "
        f"got {response.message_type.name}"
    )

    error = decode_error(response.payload)

    assert error["code"] == expected_code, (
        f"Expected {expected_code}, "
        f"got {error['code']}: {error['message']}"
    )

    assert isinstance(error["message"], str)
    assert error["message"]

    return error


@pytest.mark.asyncio
async def test_store_returns_exact_error_codes(tmp_path):
    """
    Проверяет настоящие TCP-ответы:

    STORE_RESPONSE:
        успешная запись;
        идентичный повтор.

    ERROR ROLLBACK:
        попытка сохранить старую версию.

    ERROR CONFLICT:
        другой контент с тем же sequence_number.

    Также проверяет сохранность актуальной записи.
    """

    ports = set()

    while len(ports) < 2:
        ports.add(free_port())

    ports = list(ports)

    owner = Node(
        make_config(tmp_path / "owner", ports[0])
    )

    receiver = Node(
        make_config(tmp_path / "receiver", ports[1])
    )

    started = []

    try:
        await owner.start()
        started.append(owner)

        await receiver.start()
        started.append(receiver)

        address = f"127.0.0.1:{ports[0]}"

        old_record = create_node_record(
            identity=owner.identity,
            addresses=[address],
            sequence_number=1,
            ttl_seconds=180,
        )

        current_record = create_node_record(
            identity=owner.identity,
            addresses=[address],
            sequence_number=2,
            ttl_seconds=180,
        )

        # Тот же владелец, тот же номер версии,
        # но другое содержимое.
        conflicting_record = create_node_record(
            identity=owner.identity,
            addresses=sorted([
                f"127.0.0.1:{ports[0]}",
                "127.0.0.1:19001",
            ]),
            sequence_number=2,
            ttl_seconds=180,
        )

        key = node_record_key(current_record.node_id)

        assert node_record_key(old_record.node_id) == key
        assert node_record_key(conflicting_record.node_id) == key

        assert conflicting_record != current_record

        # ----------------------------------
        # 1. Сохранение версии 1
        # ----------------------------------

        response = await raw_store(
            owner, receiver, old_record
        )

        assert_store_success(response, key)

        assert (
            await receiver.dht_storage.get(key)
            == old_record
        )

        print("\nVERSION 1: stored")

        # ----------------------------------
        # 2. Обновление до версии 2
        # ----------------------------------

        response = await raw_store(
            owner, receiver, current_record
        )

        assert_store_success(response, key)

        assert (
            await receiver.dht_storage.get(key)
            == current_record
        )

        print("VERSION 2: stored")

        # ----------------------------------
        # 3. Откат до версии 1
        # ----------------------------------

        response = await raw_store(
            owner, receiver, old_record
        )

        error = assert_error_code(
            response, "ROLLBACK"
        )

        assert error["message"] == "Older record version"

        assert (
            await receiver.dht_storage.get(key)
            == current_record
        )

        print("ROLLBACK: exact error code confirmed")

        # ----------------------------------
        # 4. Конфликт версии 2
        # ----------------------------------

        response = await raw_store(
            owner, receiver, conflicting_record
        )

        error = assert_error_code(
            response, "CONFLICT"
        )

        assert error["message"] == "Conflicting record"

        assert (
            await receiver.dht_storage.get(key)
            == current_record
        )

        print("CONFLICT: exact error code confirmed")

        # ----------------------------------
        # 5. Идентичный повтор версии 2
        # ----------------------------------

        response = await raw_store(
            owner, receiver, current_record
        )

        assert_store_success(response, key)

        assert (
            await receiver.dht_storage.get(key)
            == current_record
        )

        print("IDENTICAL REPLAY: accepted")

        # ----------------------------------
        # 6. Проверка FIND_VALUE
        # ----------------------------------

        received = await owner.find_value(
            receiver.local_contact,
            key,
        )

        assert received == current_record

        assert await receiver.dht_storage.count() == 1

        print("FIND_VALUE: current version preserved")
        print("STORE ERROR CODES: passed")

    finally:
        for node in reversed(started):
            await node.stop()



@pytest.mark.asyncio
async def test_concurrent_store_versions_keep_newest(tmp_path):
    """
    20 конкурентных STORE-запросов одного ключа.

    Проверяет:
    - реальные TCP RPC;
    - корректность каждого ответа;
    - сохранение максимальной версии;
    - отсутствие неожиданных ошибок;
    - FIND_VALUE;
    - сохранность после перезапуска.
    """

    ports = set()
    while len(ports) < 2:
        ports.add(free_port())

    owner_port, receiver_port = sorted(ports)

    owner_config = make_config(
        tmp_path / "owner",
        owner_port,
    )
    receiver_config = make_config(
        tmp_path / "receiver",
        receiver_port,
    )

    owner = Node(owner_config)
    receiver = Node(receiver_config)

    owner_started = False
    receiver_started = False

    try:
        await owner.start()
        owner_started = True

        await receiver.start()
        receiver_started = True

        max_version = 20
        address = f"127.0.0.1:{owner_port}"

        records = {
            version: create_node_record(
                identity=owner.identity,
                addresses=[address],
                sequence_number=version,
                ttl_seconds=180,
            )
            for version in range(1, max_version + 1)
        }

        key = node_record_key(
            records[max_version].node_id
        )

        assert all(
            node_record_key(record.node_id) == key
            for record in records.values()
        )

        # Запускаем запросы одновременно.
        # Порядок отправки намеренно перемешан.
        order = [
            12, 3, 19, 1, 7,
            15, 4, 20, 9, 2,
            18, 5, 14, 6, 11,
            8, 17, 10, 16, 13,
        ]

        assert sorted(order) == list(
            range(1, max_version + 1)
        )

        async def send_version(version):
            response = await raw_store(
                owner,
                receiver,
                records[version],
            )
            return version, response

        results = await asyncio.gather(
            *(
                send_version(version)
                for version in order
            )
        )

        successful = []
        rollbacks = []

        for version, response in results:
            if response.message_type == MessageType.STORE_RESPONSE:
                assert_store_success(response, key)
                successful.append(version)

            elif response.message_type == MessageType.ERROR:
                error = assert_error_code(
                    response,
                    "ROLLBACK",
                )
                assert (
                    error["message"]
                    == "Older record version"
                )
                rollbacks.append(version)

            else:
                pytest.fail(
                    f"Version {version}: unexpected "
                    f"response {response.message_type.name}"
                )

        # Максимальная версия не может быть отклонена
        # как устаревшая в этой группе запросов.
        assert max_version in successful

        # Каждый запрос должен получить допустимый ответ.
        assert len(successful) + len(rollbacks) == max_version

        # После всех конкурентных запросов
        # должна остаться максимальная версия.
        stored = await receiver.dht_storage.get(key)

        assert stored == records[max_version]
        assert stored.sequence_number == max_version
        assert await receiver.dht_storage.count() == 1

        # Проверка через настоящий FIND_VALUE RPC.
        found = await owner.find_value(
            receiver.local_contact,
            key,
        )

        assert found == records[max_version]

        print(
            f"\nCONCURRENT STORE: {max_version} requests"
        )
        print(f"SUCCESSFUL: {len(successful)}")
        print(f"ROLLBACK: {len(rollbacks)}")
        print(
            f"FINAL VERSION: {stored.sequence_number}"
        )

        # Останавливаем получателя и создаём
        # новый объект Node с тем же каталогом.
        await receiver.stop()
        receiver_started = False

        restarted_receiver = Node(receiver_config)
        receiver = restarted_receiver

        await receiver.start()
        receiver_started = True

        persisted = await receiver.dht_storage.get(key)

        assert persisted == records[max_version]
        assert persisted.sequence_number == max_version

        found_after_restart = await owner.find_value(
            receiver.local_contact,
            key,
        )

        assert found_after_restart == records[max_version]

        # Повторная отправка старой версии
        # после перезапуска.
        response = await raw_store(
            owner,
            receiver,
            records[1],
        )

        assert_error_code(response, "ROLLBACK")

        assert (
            await receiver.dht_storage.get(key)
            == records[max_version]
        )

        print("RESTART: version 20 preserved")
        print("POST-RESTART ROLLBACK: rejected")
        print("CONCURRENT VERSION TEST: passed")

    finally:
        if receiver_started:
            await receiver.stop()

        if owner_started:
            await owner.stop()



@pytest.mark.asyncio
async def test_refresh_published_record_not_due(tmp_path):
    """
    Запись не обновляется раньше половины TTL.
    """
    port = free_port()
    owner = Node(make_config(tmp_path / "owner", port))

    record = create_node_record(
        identity=owner.identity,
        addresses=[f"127.0.0.1:{port}"],
        sequence_number=1,
        ttl_seconds=180,
    )

    await owner.published_registry.add(record)

    refreshed = await owner.refresh_published_record(record)

    assert refreshed is False

    key = node_record_key(record.node_id)
    current = await owner.published_registry.get(key)

    assert current == record
    assert current.sequence_number == 1
