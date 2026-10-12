import asyncio
import secrets

import pytest

from p2p.protocol.codec import encode_frame
from p2p.protocol.constants import (
    PROTOCOL_VERSION,
    MessageType,
)
from p2p.protocol.frame import Frame
from p2p.transport.connection import (
    RpcError,
    send_request,
)


@pytest.mark.asyncio
async def test_wrong_request_id_is_rejected():
    async def fake_server(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        # Получаем запрос клиента.
        await reader.read(4096)

        # Намеренно создаём ответ с ДРУГИМ request_id.
        wrong_response = Frame(
            version=PROTOCOL_VERSION,
            message_type=MessageType.PONG,
            flags=0,
            request_id=secrets.token_bytes(16),
            payload=b"",
        )

        writer.write(
            encode_frame(wrong_response)
        )

        await writer.drain()

        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(
        fake_server,
        "127.0.0.1",
        0,
    )

    port = server.sockets[0].getsockname()[1]

    request = Frame(
        version=PROTOCOL_VERSION,
        message_type=MessageType.PING,
        flags=0,
        request_id=secrets.token_bytes(16),
        payload=b"",
    )

    try:
        with pytest.raises(
            RpcError,
            match="request_id does not match",
        ):
            await send_request(
                host="127.0.0.1",
                port=port,
                frame=request,
            )

    finally:
        server.close()
        await server.wait_closed()