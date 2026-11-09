
import secrets

import pytest

from p2p.node.node import Node
from p2p.dht.records import (
    create_node_record,
    node_record_key,
)
from p2p.dht.messages import encode_store_request
from p2p.protocol.constants import MessageType
from p2p.protocol.frame import Frame
from p2p.protocol.messages import decode_error
from p2p.transport.connection import send_request

from test_dht_store_error_codes_rpc import free_port, make_config


async def send_raw_store(sender, receiver, record):
    """Отправляет STORE_REQUEST через настоящий TCP."""

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
        response_timeout=sender.config.ping_timeout,
        protocol_version=sender.config.protocol_version,
        max_frame_payload=sender.config.max_frame_payload,
        local_node_label=sender.local_node_label,
    )

    assert response.request_id == request_id
    return response


@pytest.mark.asyncio
async def test_store_rollback_and_conflict_over_tcp(tmp_path):
    ports = []

    while len(ports) < 2:
        port = free_port()
        if port not in ports:
            ports.append(port)

    sender = Node(make_config(tmp_path / "sender", ports[0]))
    receiver = Node(make_config(tmp_path / "receiver", ports[1]))

    started = []

    try:
        await sender.start()
        started.append(sender)

        await receiver.start()
        started.append(receiver)

        record5 = create_node_record(
            identity=sender.identity,
            addresses=[f"127.0.0.1:{ports[0]}"],
            sequence_number=5,
            ttl_seconds=180,
        )

        record6 = create_node_record(
            identity=sender.identity,
            addresses=[f"127.0.0.1:{ports[0]}"],
            sequence_number=6,
            ttl_seconds=180,
        )

        conflicting6 = create_node_record(
            identity=sender.identity,
            addresses=["127.0.0.1:9999"],
            sequence_number=6,
            ttl_seconds=180,
        )

        key = node_record_key(record6.node_id)

        # 1. Сохраняем актуальную версию.
        response = await send_raw_store(
            sender, receiver, record6
        )

        assert response.message_type == MessageType.STORE_RESPONSE
        assert await receiver.dht_storage.get(key) == record6

        # 2. Пытаемся вернуть старую версию.
        response = await send_raw_store(
            sender, receiver, record5
        )

        assert response.message_type == MessageType.ERROR

        error = decode_error(response.payload)
        assert error["code"] == "ROLLBACK"

        assert await receiver.dht_storage.get(key) == record6

        # 3. Отправляем конфликтующую версию 6.
        response = await send_raw_store(
            sender, receiver, conflicting6
        )

        assert response.message_type == MessageType.ERROR

        error = decode_error(response.payload)
        assert error["code"] == "CONFLICT"

        assert await receiver.dht_storage.get(key) == record6

        # 4. Повторяем идентичную версию 6.
        response = await send_raw_store(
            sender, receiver, record6
        )

        assert response.message_type == MessageType.STORE_RESPONSE
        assert await receiver.dht_storage.get(key) == record6

    finally:
        for node in reversed(started):
            await node.stop()
