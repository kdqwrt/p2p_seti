
import asyncio
import time

import pytest

from p2p.node.node import Node
from p2p.dht.records import create_node_record, node_record_key
from p2p.protocol.constants import MessageType
from p2p.protocol.messages import decode_error
from p2p.dht.messages import decode_store_response

from test_dht_concurrent_store_rpc import (
    free_port,
    make_config,
    send_store_rpc,
)


@pytest.mark.asyncio
async def test_concurrent_same_sequence_conflict_rpc(tmp_path):
    ports = set()

    while len(ports) < 2:
        ports.add(free_port())

    sender_port, receiver_port = list(ports)

    sender = Node(
        make_config(tmp_path / "sender", sender_port)
    )
    receiver = Node(
        make_config(tmp_path / "receiver", receiver_port)
    )

    started = []

    try:
        await sender.start()
        started.append(sender)

        await receiver.start()
        started.append(receiver)

        now = int(time.time())

        records = [
            create_node_record(
                identity=sender.identity,
                addresses=[f"127.0.0.1:{9101 + index}"],
                sequence_number=20,
                ttl_seconds=180,
                now=now,
            )
            for index in range(3)
        ]

        key = node_record_key(records[0].node_id)

        assert len({
            record.to_bytes()
            for record in records
        }) == 3

        start_event = asyncio.Event()

        async def send_record(record):
            await start_event.wait()
            return await send_store_rpc(
                sender,
                receiver,
                record,
            )

        tasks = [
            asyncio.create_task(send_record(record))
            for record in records
        ]

        start_event.set()

        responses = await asyncio.gather(*tasks)

        accepted = []
        rejected = []

        for record, response in zip(records, responses):
            if response.message_type == MessageType.STORE_RESPONSE:
                result = decode_store_response(response.payload)

                assert result["stored"] is True
                assert result["key"] == key

                accepted.append(record)

            else:
                assert response.message_type == MessageType.ERROR

                error = decode_error(response.payload)
                assert error["code"] == "CONFLICT"

                rejected.append(record)

        # Ровно одна версия должна быть принята.
        assert len(accepted) == 1
        assert len(rejected) == 2

        winner = accepted[0]

        # Хранилище должно содержать победившую запись.
        stored = await receiver.dht_storage.get(key)

        assert stored == winner
        assert stored.sequence_number == 20

        # Проверяем результат через настоящий FIND_VALUE RPC.
        received = await sender.find_value(
            receiver.local_contact,
            key,
        )

        assert received == winner

        # Повторная отправка победителя разрешена.
        response = await send_store_rpc(
            sender,
            receiver,
            winner,
        )

        assert response.message_type == MessageType.STORE_RESPONSE

        # Все конфликтующие записи по-прежнему отклоняются.
        for record in rejected:
            response = await send_store_rpc(
                sender,
                receiver,
                record,
            )

            assert response.message_type == MessageType.ERROR

            error = decode_error(response.payload)
            assert error["code"] == "CONFLICT"

        assert await receiver.dht_storage.get(key) == winner

        print("\nCONCURRENT CONFLICT RPC: passed")

    finally:
        for node in reversed(started):
            await node.stop()
