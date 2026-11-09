
import socket

import pytest

from p2p.config import NodeConfig
from p2p.node.node import Node
from p2p.dht.records import create_node_record, node_record_key
from p2p.dht.messages import (
    encode_store_request,
    decode_store_response,
)
from p2p.protocol.constants import MessageType
from p2p.protocol.frame import Frame
from p2p.transport.connection import send_request

import secrets


def free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
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


def assert_rejected(response):
    if response.message_type == MessageType.ERROR:
        return

    assert response.message_type == MessageType.STORE_RESPONSE
    result = decode_store_response(response.payload)
    assert result["stored"] is False


@pytest.mark.asyncio
async def test_sequence_rollback_and_conflict_over_tcp(tmp_path):
    ports = set()
    while len(ports) < 2:
        ports.add(free_port())
    ports = list(ports)

    sender = Node(make_config(tmp_path / "sender", ports[0]))
    receiver = Node(make_config(tmp_path / "receiver", ports[1]))
    started = []

    try:
        for node in (sender, receiver):
            await node.start()
            started.append(node)

        await sender.ping(
            host=receiver.local_contact.host,
            port=receiver.local_contact.port,
        )

        address = f"127.0.0.1:{ports[0]}"

        # Все записи принадлежат одному владельцу.
        # TTL достаточно большой, чтобы не мешать
        # проверке sequence_number.
        version_1 = create_node_record(
            identity=sender.identity,
            addresses=[address],
            sequence_number=1,
            ttl_seconds=180,
        )

        version_2 = create_node_record(
            identity=sender.identity,
            addresses=[address],
            sequence_number=2,
            ttl_seconds=180,
        )

        key = node_record_key(version_1.node_id)
        assert node_record_key(version_2.node_id) == key

        # 1. Первоначальное сохранение.
        assert await sender.store_record(
            receiver.local_contact, version_1
        ) is True

        assert await receiver.dht_storage.get(key) == version_1

        # 2. Обновление до версии 2.
        assert await sender.store_record(
            receiver.local_contact, version_2
        ) is True

        assert await receiver.dht_storage.get(key) == version_2

        print("\nUPDATE: version 1 -> version 2")

        # 3. Replay старой, но ещё действующей версии.
        response = await send_store_rpc(
            sender, receiver, version_1
        )
        assert_rejected(response)

        assert await receiver.dht_storage.get(key) == version_2
        assert await sender.find_value(
            receiver.local_contact, key
        ) == version_2

        print("ROLLBACK: rejected")

        # 4. Повторная отправка идентичной версии 2.
        response = await send_store_rpc(
            sender, receiver, version_2
        )

        assert response.message_type == MessageType.STORE_RESPONSE
        assert decode_store_response(response.payload)["stored"] is True
        assert await receiver.dht_storage.get(key) == version_2

        print("IDENTICAL REPLAY: accepted")

        # 5. Конфликт: тот же sequence_number,
        # но другое содержимое, подписанное владельцем.
        conflicting_version_2 = create_node_record(
            identity=sender.identity,
            addresses=[address, "127.0.0.1:65530"],
            sequence_number=2,
            ttl_seconds=180,
        )

        assert node_record_key(
            conflicting_version_2.node_id
        ) == key

        assert (
            conflicting_version_2.to_bytes()
            != version_2.to_bytes()
        )

        response = await send_store_rpc(
            sender, receiver, conflicting_version_2
        )
        assert_rejected(response)

        assert await receiver.dht_storage.get(key) == version_2
        assert await sender.find_value(
            receiver.local_contact, key
        ) == version_2

        print("SAME-SEQUENCE CONFLICT: rejected")

        # 6. Более новая версия должна приниматься.
        version_3 = create_node_record(
            identity=sender.identity,
            addresses=[address],
            sequence_number=3,
            ttl_seconds=180,
        )

        assert await sender.store_record(
            receiver.local_contact, version_3
        ) is True

        assert await receiver.dht_storage.get(key) == version_3
        assert await sender.find_value(
            receiver.local_contact, key
        ) == version_3

        print("UPDATE: version 3 accepted")

    finally:
        for node in reversed(started):
            await node.stop()
