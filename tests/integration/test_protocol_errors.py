import asyncio
import secrets

import pytest

from p2p.protocol.codec import (
    HEADER_STRUCT,
    encode_frame,
)
from p2p.protocol.constants import (
    PROTOCOL_VERSION,
    MessageType,
)
from p2p.protocol.contact import Contact
from p2p.protocol.frame import Frame
from p2p.protocol.messages import (
    decode_error,
    encode_pong,
)
from p2p.transport.server import TCPServer
from p2p.transport.stream_decoder import StreamDecoder


def make_contact(port: int) -> Contact:
    return Contact(
        node_id=b"",
        identity_algorithm="stage1-none",
        identity_public_key=b"",
        host="127.0.0.1",
        port=port,
    )


async def start_test_server() -> tuple[TCPServer, int]:
    server = TCPServer(
        host="127.0.0.1",
        port=0,
        local_contact=make_contact(9101),
    )

    await server.start()

    port = server._server.sockets[0].getsockname()[1]

    return server, port


@pytest.mark.asyncio
async def test_disconnect_inside_frame_does_not_stop_server():
    server, port = await start_test_server()

    try:
        reader, writer = await asyncio.open_connection(
            "127.0.0.1",
            port,
        )

        frame = Frame(
            version=PROTOCOL_VERSION,
            message_type=MessageType.PING,
            flags=0,
            request_id=secrets.token_bytes(16),
            payload=b"some incomplete payload",
        )

        encoded = encode_frame(frame)

        # Передаём не весь кадр и обрываем TCP.
        writer.write(encoded[:-5])
        await writer.drain()

        writer.close()
        await writer.wait_closed()

        # Даём обработчику сервера получить EOF.
        await asyncio.sleep(0.05)

        assert server._server is not None
        assert server._server.is_serving()

    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_unknown_version_does_not_stop_server():
    server, port = await start_test_server()

    try:
        reader, writer = await asyncio.open_connection(
            "127.0.0.1",
            port,
        )

        header = HEADER_STRUCT.pack(
            PROTOCOL_VERSION + 1,
            int(MessageType.PING),
            0,
            secrets.token_bytes(16),
            0,
        )

        writer.write(header)
        await writer.drain()

        # При неизвестной версии безопаснее закрыть соединение:
        # сервер не обязан отвечать кадром неизвестной версии.
        data = await asyncio.wait_for(
            reader.read(),
            timeout=1.0,
        )

        assert data == b""

        writer.close()
        await writer.wait_closed()

        assert server._server is not None
        assert server._server.is_serving()

    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_unknown_message_type_does_not_stop_server():
    server, port = await start_test_server()

    try:
        reader, writer = await asyncio.open_connection(
            "127.0.0.1",
            port,
        )

        header = HEADER_STRUCT.pack(
            PROTOCOL_VERSION,
            0x55,
            0,
            secrets.token_bytes(16),
            0,
        )

        writer.write(header)
        await writer.drain()

        data = await asyncio.wait_for(
            reader.read(),
            timeout=1.0,
        )

        assert data == b""

        writer.close()
        await writer.wait_closed()

        assert server._server is not None
        assert server._server.is_serving()

    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_response_as_request_returns_error():
    server, port = await start_test_server()

    try:
        reader, writer = await asyncio.open_connection(
            "127.0.0.1",
            port,
        )

        request_id = secrets.token_bytes(16)

        invalid_request = Frame(
            version=PROTOCOL_VERSION,
            message_type=MessageType.PONG,
            flags=0,
            request_id=request_id,
            payload=encode_pong(
                responder=make_contact(9102),
                ping_timestamp_ms=100,
                responder_timestamp_ms=200,
            ),
        )

        writer.write(
            encode_frame(invalid_request)
        )
        await writer.drain()

        data = await asyncio.wait_for(
            reader.read(4096),
            timeout=1.0,
        )

        decoder = StreamDecoder()
        frames = decoder.feed_data(data)

        assert len(frames) == 1

        response = frames[0]

        assert response.message_type == MessageType.ERROR
        assert response.request_id == request_id

        error = decode_error(response.payload)

        assert error["code"] == "INVALID_DIRECTION"

        writer.close()
        await writer.wait_closed()

    finally:
        await server.stop()