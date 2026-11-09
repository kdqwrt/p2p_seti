
import asyncio
import time

import pytest

from p2p.node.node import Node
from p2p.dht.records import create_node_record, node_record_key
from p2p.protocol.constants import MessageType
from p2p.protocol.messages import decode_error

from test_dht_store_error_codes_rpc import free_port, make_config
from test_dht_store_rollback_conflict_rpc import send_raw_store


@pytest.mark.asyncio
async def test_store_rollback_rejected_after_restart_over_tcp(tmp_path):
    ports = []

    while len(ports) < 2:
        port = free_port()
        if port not in ports:
            ports.append(port)

    sender = Node(make_config(tmp_path / "sender", ports[0]))
    receiver_config = make_config(tmp_path / "receiver", ports[1])
    receiver = Node(receiver_config)

    started = []

    try:
        await sender.start()
        started.append(sender)

        await receiver.start()
        started.append(receiver)

        # Версия 5 остаётся действительной достаточно долго.
        record5 = create_node_record(
            identity=sender.identity,
            addresses=[f"127.0.0.1:{ports[0]}"],
            sequence_number=5,
            ttl_seconds=180,
        )

        # Версия 6 имеет короткий TTL.
        record6 = create_node_record(
            identity=sender.identity,
            addresses=[f"127.0.0.1:{ports[0]}"],
            sequence_number=6,
            ttl_seconds=3,
        )

        key = node_record_key(record5.node_id)

        # 1. Сохраняем версию 5 через TCP.
        response = await send_raw_store(sender, receiver, record5)
        assert response.message_type == MessageType.STORE_RESPONSE

        # 2. Обновляем до версии 6.
        response = await send_raw_store(sender, receiver, record6)
        assert response.message_type == MessageType.STORE_RESPONSE

        assert await receiver.dht_storage.get(key) == record6

        # 3. Дожидаемся истечения TTL версии 6.
        delay = max(
            0.0,
            record6.expires_at - time.time() + 1.1,
        )
        await asyncio.sleep(delay)

        await receiver.dht_storage.cleanup()

        assert await receiver.dht_storage.get(key) is None

        # 4. Перезапускаем реплику.
        await receiver.stop()
        started.remove(receiver)

        restarted = Node(receiver_config)
        await restarted.start()
        started.append(restarted)

        assert await restarted.dht_storage.get(key) is None

        # 5. Пытаемся вернуть версию 5 через TCP.
        assert record5.expires_at > int(time.time())

        response = await send_raw_store(
            sender,
            restarted,
            record5,
        )

        # 6. Проверяем код ошибки.
        assert response.message_type == MessageType.ERROR

        error = decode_error(response.payload)
        assert error["code"] == "ROLLBACK"

        # Старая версия не появилась в хранилище.
        assert await restarted.dht_storage.get(key) is None

        # Журнал по-прежнему хранит максимальную версию.
        assert restarted.dht_storage._versions[key][0] == 6

        print("\nROLLBACK AFTER RESTART: rejected over TCP")

    finally:
        for node in reversed(started):
            await node.stop()
