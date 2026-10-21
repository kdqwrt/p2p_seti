
import asyncio
import struct

import pytest

from p2p.protocol.constants import PROTOCOL_VERSION
from p2p.protocol.contact import Contact
from p2p.transport.server import TCPServer


@pytest.mark.asyncio
async def test_slow_client_cannot_keep_incomplete_frame_open():
    contact = Contact(
        node_id=b"",
        identity_algorithm="stage1-none",
        identity_public_key=b"",
        host="127.0.0.1",
        port=9101,
    )

    server = TCPServer(
        host="127.0.0.1",
        port=0,
        local_contact=contact,
        read_timeout=0.2,
        protocol_version=PROTOCOL_VERSION,
        max_frame_payload=65_536,
    )

    await server.start()

    try:
        port = server._server.sockets[0].getsockname()[1]

        reader, writer = await asyncio.open_connection(
            "127.0.0.1", port
        )

        try:
            header = struct.pack(
                "!BBH16sI",
                PROTOCOL_VERSION,
                1,
                0,
                b"\x01" * 16,
                100,
            )

            # Отправляем заголовок и часть payload.
            writer.write(header + b"a")
            await writer.drain()

            # Продолжаем медленно передавать байты.
            # Каждая задержка меньше read_timeout.
            for _ in range(3):
                await asyncio.sleep(0.08)

                if writer.is_closing():
                    break

                writer.write(b"a")
                await writer.drain()

            # Сервер должен закрыть соединение,
            # несмотря на поступление новых байтов.
            result = await asyncio.wait_for(
                reader.read(1),
                timeout=1.0,
            )

            assert result == b""

        finally:
            writer.close()
            await writer.wait_closed()

        # Проверяем, что сервер продолжает принимать клиентов.
        reader2, writer2 = await asyncio.open_connection(
            "127.0.0.1", port
        )

        writer2.close()
        await writer2.wait_closed()

    finally:
        await server.stop()
