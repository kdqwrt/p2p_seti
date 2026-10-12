import secrets
import struct

import pytest

from p2p.protocol.codec import (
    HEADER_STRUCT,
    ProtocolError,
    decode_header,
    encode_frame,
)
from p2p.protocol.constants import (
    MAX_FRAME_PAYLOAD,
    PROTOCOL_VERSION,
    MessageType,
)
from p2p.protocol.frame import Frame
from p2p.transport.stream_decoder import StreamDecoder


def test_oversized_payload_length_is_rejected():
    request_id = secrets.token_bytes(16)

    header = HEADER_STRUCT.pack(
        PROTOCOL_VERSION,
        int(MessageType.PING),
        0,
        request_id,
        MAX_FRAME_PAYLOAD + 1,
    )

    decoder = StreamDecoder()

    with pytest.raises(ProtocolError, match="Payload is too large"):
        decoder.feed_data(header)


def test_unknown_protocol_version_is_rejected():
    request_id = secrets.token_bytes(16)

    unknown_version = PROTOCOL_VERSION + 1

    header = HEADER_STRUCT.pack(
        unknown_version,
        int(MessageType.PING),
        0,
        request_id,
        0,
    )

    decoder = StreamDecoder()

    with pytest.raises(
        ProtocolError,
        match="Unsupported protocol version",
    ):
        decoder.feed_data(header)


def test_unknown_message_type_is_rejected():
    request_id = secrets.token_bytes(16)

    unknown_type = 0x55

    header = HEADER_STRUCT.pack(
        PROTOCOL_VERSION,
        unknown_type,
        0,
        request_id,
        0,
    )

    decoder = StreamDecoder()

    with pytest.raises(
        ProtocolError,
        match="Unknown message type",
    ):
        decoder.feed_data(header)


def test_encode_rejects_oversized_payload():
    frame = Frame(
        version=PROTOCOL_VERSION,
        message_type=MessageType.PING,
        flags=0,
        request_id=secrets.token_bytes(16),
        payload=b"x" * (MAX_FRAME_PAYLOAD + 1),
    )

    with pytest.raises(
        ProtocolError,
        match="Payload is too large",
    ):
        encode_frame(frame)

def test_disconnect_in_middle_of_frame_leaves_no_complete_message():
    frame = Frame(
        version=PROTOCOL_VERSION,
        message_type=MessageType.PING,
        flags=0,
        request_id=secrets.token_bytes(16),
        payload=b"complete payload",
    )

    encoded = encode_frame(frame)

    decoder = StreamDecoder()

    # Имитируем ситуацию:
    # TCP-соединение оборвалось до получения полного кадра.
    incomplete_data = encoded[:-5]

    frames = decoder.feed_data(incomplete_data)

    assert frames == []
    assert decoder.buffered_bytes > 0

    # При закрытии соединения транспорт освобождает
    # состояние декодера.
    decoder.reset()

    assert decoder.buffered_bytes == 0

def test_encode_respects_custom_max_payload():
    frame = Frame(
        version=PROTOCOL_VERSION,
        message_type=MessageType.PING,
        flags=0,
        request_id=secrets.token_bytes(16),
        payload=b"A" * 101,
    )

    with pytest.raises(
        ProtocolError,
        match="Payload is too large",
    ):
        encode_frame(
            frame,
            max_payload=100,
        )

def test_encode_respects_expected_protocol_version():
    frame = Frame(
        version=PROTOCOL_VERSION,
        message_type=MessageType.PING,
        flags=0,
        request_id=secrets.token_bytes(16),
        payload=b"",
    )

    with pytest.raises(
        ProtocolError,
        match="Unsupported protocol version",
    ):
        encode_frame(
            frame,
            expected_version=2,
        )