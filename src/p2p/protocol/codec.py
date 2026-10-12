import struct

from .constants import (
    HEADER_SIZE,
    MAX_FRAME_PAYLOAD,
    PROTOCOL_VERSION,
    MessageType,
)
from .frame import Frame


HEADER_STRUCT = struct.Struct("!BBH16sI")


class ProtocolError(Exception):
    """Ошибка протокола передачи данных."""


def encode_frame(
    frame: Frame,
    expected_version: int = PROTOCOL_VERSION,
    max_payload: int = MAX_FRAME_PAYLOAD,
) -> bytes:
    payload_length = len(frame.payload)

    if frame.version != expected_version:
        raise ProtocolError(
            f"Unsupported protocol version: {frame.version}"
        )

    if payload_length > max_payload:
        raise ProtocolError(
            f"Payload is too large: {payload_length} bytes"
        )

    header = HEADER_STRUCT.pack(
        frame.version,
        int(frame.message_type),
        frame.flags,
        frame.request_id,
        payload_length,
    )

    return header + frame.payload

def decode_header(
    header: bytes,
    expected_version: int = PROTOCOL_VERSION,
    max_payload: int = MAX_FRAME_PAYLOAD,
) -> tuple[int, MessageType, int, bytes, int]:

    if len(header) != HEADER_SIZE:
        raise ProtocolError(
            f"Invalid header size: expected {HEADER_SIZE}, got {len(header)}"
        )

    (
        version,
        message_type_raw,
        flags,
        request_id,
        payload_length,
    ) = HEADER_STRUCT.unpack(header)

    if version != expected_version:
        raise ProtocolError(
            f"Unsupported protocol version: {version}"
        )

    if payload_length > max_payload:
        raise ProtocolError(
            f"Payload is too large: {payload_length} bytes"
        )

    try:
        message_type = MessageType(message_type_raw)

    except ValueError as exc:
        raise ProtocolError(
            f"Unknown message type: {message_type_raw}"
        ) from exc

    return (
        version,
        message_type,
        flags,
        request_id,
        payload_length,
    )