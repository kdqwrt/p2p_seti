
import asyncio
import secrets
import time

import pytest

from p2p.protocol.constants import MessageType, PROTOCOL_VERSION
from p2p.protocol.frame import Frame
from p2p.transport.connection import RpcError, send_request


@pytest.mark.asyncio
async def test_rpc_response_has_total_timeout():
    async def slow_server(reader, writer):
        try:
            # Получаем запрос от клиента.
            await reader.read(4096)

            # Отправляем отдельные байты медленно.
            # Полный корректный ответ не формируется.
            for _ in range(10):
                writer.write(b"\x01")
                await writer.drain()
                await asyncio.sleep(0.08)

        except (ConnectionError, OSError):
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    server = await asyncio.start_server(
        slow_server,
        "127.0.0.1",
        0,
    )

    try:
        port = server.sockets[0].getsockname()[1]

        request = Frame(
            version=PROTOCOL_VERSION,
            message_type=MessageType.PING,
            flags=0,
            request_id=secrets.token_bytes(16),
            payload=b"",
        )

        start = time.monotonic()

        with pytest.raises(
            RpcError,
            match="Timed out waiting for response",
        ):
            await send_request(
                host="127.0.0.1",
                port=port,
                frame=request,
                connect_timeout=1.0,
                response_timeout=0.2,
                protocol_version=PROTOCOL_VERSION,
                max_frame_payload=65_536,
            )

        elapsed = time.monotonic() - start

        # Проверяем, что ожидание не продлевалось
        # при получении очередного байта.
        assert elapsed < 0.6

    finally:
        server.close()
        await server.wait_closed()
