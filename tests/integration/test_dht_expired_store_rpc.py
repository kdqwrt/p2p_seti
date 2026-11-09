
import asyncio
import secrets
import socket
import time

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



async def raw_store(sender, receiver, record, payload=None):
    """
    Отправляет STORE_REQUEST через настоящий TCP.

    payload можно сформировать заранее,
    пока запись ещё действительна.
    """

    key = node_record_key(record.node_id)
    request_id = secrets.token_bytes(16)

    if payload is None:
        payload = encode_store_request(key, record)

    request = Frame(
        version=sender.config.protocol_version,
        message_type=MessageType.STORE_REQUEST,
        flags=0,
        request_id=request_id,
        payload=payload,
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

    return response



def assert_store_rejected(response):
    """
    Допустимы два корректных способа отказа:
    ERROR либо STORE_RESPONSE со stored=False.
    """

    if response.message_type == MessageType.ERROR:
        return

    assert response.message_type == MessageType.STORE_RESPONSE

    result = decode_store_response(response.payload)

    assert result["stored"] is False


@pytest.mark.asyncio
async def test_expired_record_rejected_over_tcp(tmp_path):
    """
    Проверяет:
    1. Отклонение просроченной записи по TCP.
    2. Отсутствие записи в DHT.
    3. Успешное сохранение действующей записи.
    4. Защиту новой записи от повторной отправки
       старой просроченной версии.
    """

    ports = set()

    while len(ports) < 2:
        ports.add(free_port())

    ports = list(ports)

    sender = Node(
        make_config(tmp_path / "sender", ports[0])
    )

    receiver = Node(
        make_config(tmp_path / "receiver", ports[1])
    )

    started = []

    try:
        # 1. Запускаем оба TCP-сервера.
        for node in (sender, receiver):
            await node.start()
            started.append(node)

        # Проверяем сетевую доступность.
        await sender.ping(
            host=receiver.local_contact.host,
            port=receiver.local_contact.port,
        )

        # 2. Создаём корректно подписанную запись
        # с коротким сроком действия.
        expired_record = create_node_record(
            identity=sender.identity,
            addresses=[f"127.0.0.1:{ports[0]}"],
            sequence_number=1,
            ttl_seconds=2,
        )

        key = node_record_key(expired_record.node_id)


        expired_payload = encode_store_request(
            key,
            expired_record,
        )

        # Ждём фактического истечения TTL.
        delay = max(
            0.0,
            expired_record.expires_at - time.time() + 1.1,
        )

        await asyncio.sleep(delay)

        assert time.time() >= expired_record.expires_at

        response = await raw_store(
            sender,
            receiver,
            expired_record,
            payload=expired_payload,
        )

        assert_store_rejected(response)

        # 4. У получателя не должно быть записи.
        assert await receiver.dht_storage.get(key) is None

        received = await sender.find_value(
            receiver.local_contact,
            key,
        )

        assert received is None

        print("\nEXPIRED STORE: rejected")

        # 5. Создаём действующую новую версию.
        valid_record = create_node_record(
            identity=sender.identity,
            addresses=[f"127.0.0.1:{ports[0]}"],
            sequence_number=2,
            ttl_seconds=180,
        )

        assert valid_record.sequence_number == 2
        assert node_record_key(valid_record.node_id) == key

        # Штатная отправка действующей записи.
        stored = await sender.store_record(
            receiver.local_contact,
            valid_record,
        )

        assert stored is True

        assert (
            await receiver.dht_storage.get(key)
            == valid_record
        )

        received = await sender.find_value(
            receiver.local_contact,
            key,
        )

        assert received == valid_record

        print("VALID STORE: accepted")

        # 6. Повторно отправляем старую запись.
        # Используем payload, сформированный до истечения TTL.
        response = await raw_store(
            sender,
            receiver,
            expired_record,
            payload=expired_payload,
        )

        assert_store_rejected(response)

        # 7. Проверяем, что новая запись не изменена.
        stored_record = await receiver.dht_storage.get(key)

        assert stored_record == valid_record
        assert stored_record.sequence_number == 2

        received = await sender.find_value(
            receiver.local_contact,
            key,
        )

        assert received == valid_record

        print("EXPIRED REPLAY: rejected")
        print("VALID RECORD: preserved")

    finally:
        for node in reversed(started):
            await node.stop()
