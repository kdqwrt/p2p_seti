import asyncio
import secrets

import pytest

from p2p.protocol.codec import encode_frame
from p2p.protocol.constants import (
    PROTOCOL_VERSION,
    MessageType,
)
from p2p.protocol.contact import Contact
from p2p.protocol.frame import Frame
from p2p.protocol.messages import decode_error
from p2p.transport.server import TCPServer
from p2p.transport.stream_decoder import StreamDecoder


@pytest.mark.asyncio
async def test_invalid_ping_payload_does_not_crash_server():
    local_contact = Contact(
        node_id=b"",
        identity_algorithm="stage1-none",
        identity_public_key=b"",
        host="127.0.0.1",
        port=9101,
    )

    server = TCPServer(
        host="127.0.0.1",
        port=0,
        local_contact=local_contact,
        read_timeout=5.0,
        protocol_version=PROTOCOL_VERSION,
        max_frame_payload=65_536,
    )

    await server.start()

    port = server._server.sockets[0].getsockname()[1]

    try:
        reader, writer = await asyncio.open_connection(
            "127.0.0.1",
            port,
        )

        request_id = secrets.token_bytes(16)

        invalid_ping = Frame(
            version=PROTOCOL_VERSION,
            message_type=MessageType.PING,
            flags=0,
            request_id=request_id,

            # 0xC1 запрещён в MessagePack.
            payload=b"\xc1",
        )

        writer.write(
            encode_frame(invalid_ping)
        )
        await writer.drain()

        data = await asyncio.wait_for(
            reader.read(4096),
            timeout=1.0,
        )

        decoder = StreamDecoder()
        frames = decoder.feed_data(data)

        assert len(frames) == 1

        error_frame = frames[0]

        assert error_frame.message_type == MessageType.ERROR
        assert error_frame.request_id == request_id

        error = decode_error(error_frame.payload)

        assert error["code"] == "INVALID_PAYLOAD"
        assert error["message"] == "Payload validation failed"

        # После ERROR сервер должен закрыть соединение.
        remaining = await asyncio.wait_for(
            reader.read(),
            timeout=1.0,
        )

        assert remaining == b""

        writer.close()
        await writer.wait_closed()

        # Ошибка одного клиента не должна остановить узел.
        assert server._server is not None
        assert server._server.is_serving()

    finally:
        await server.stop()