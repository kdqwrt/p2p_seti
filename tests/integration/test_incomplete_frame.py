import asyncio
import struct

import pytest

from p2p.protocol.constants import PROTOCOL_VERSION
from p2p.protocol.contact import Contact
from p2p.transport.server import TCPServer


@pytest.mark.asyncio
async def test_disconnect_during_incomplete_frame():
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
        read_timeout=1.0,
        protocol_version=PROTOCOL_VERSION,
        max_frame_payload=65_536,
    )

    await server.start()

    try:
        # Получаем фактический порт тестового сервера.
        port = server._server.sockets[0].getsockname()[1]

        # Первое соединение: отправляем неполный кадр.
        reader, writer = await asyncio.open_connection(
            "127.0.0.1", port
        )

        # Заголовок обещает 100 байт полезной нагрузки.
        header = struct.pack(
            "!BBH16sI",
            PROTOCOL_VERSION,
            1,
            0,
            b"\x01" * 16,
            100,
        )

        writer.write(header + b"partial")
        await writer.drain()

        # Клиент отключается, не передав остальные байты.
        writer.close()
        await writer.wait_closed()

        # Даём серверу обработать отключение.
        await asyncio.sleep(0.05)

        # Проверяем, что сервер всё ещё принимает соединения.
        reader2, writer2 = await asyncio.open_connection(
            "127.0.0.1", port
        )

        writer2.close()
        await writer2.wait_closed()

    finally:
        await server.stop()