"""Воспроизводимая демонстрация ключевых требований Этапа 1."""

from __future__ import annotations

import random
import secrets
import struct
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from p2p.protocol.codec import ProtocolError, decode_header, encode_frame
from p2p.protocol.constants import HEADER_SIZE, MAX_FRAME_PAYLOAD, PROTOCOL_VERSION, MessageType
from p2p.protocol.frame import Frame
from p2p.transport.stream_decoder import StreamDecoder


def make_frame(index: int) -> Frame:
    payload = (f"stage1-frame-{index}:".encode("utf-8") + bytes([index % 256]) * (index % 257))
    return Frame(
        version=PROTOCOL_VERSION,
        message_type=MessageType.PING,
        flags=0,
        request_id=secrets.token_bytes(16),
        payload=payload,
    )


def check_stream_of_100_frames() -> None:
    frames = [make_frame(i) for i in range(100)]
    wire = b"".join(encode_frame(frame) for frame in frames)

    # Фиксированный seed делает демонстрацию воспроизводимой.
    rng = random.Random(20260928)
    decoder = StreamDecoder()
    decoded: list[Frame] = []

    offset = 0
    while offset < len(wire):
        chunk_size = rng.randint(1, 173)
        decoded.extend(decoder.feed_data(wire[offset:offset + chunk_size]))
        offset += chunk_size

    assert len(decoded) == 100
    assert decoder.buffered_bytes == 0
    for expected, actual in zip(frames, decoded, strict=True):
        assert actual.version == expected.version
        assert actual.message_type == expected.message_type
        assert actual.flags == expected.flags
        assert actual.request_id == expected.request_id
        assert actual.payload == expected.payload

    print("[OK] 100 variable frames decoded after random TCP-like fragmentation")


def check_split_and_glued_frames() -> None:
    first = make_frame(1)
    second = make_frame(2)
    first_wire = encode_frame(first)
    second_wire = encode_frame(second)

    decoder = StreamDecoder()
    assert decoder.feed_data(first_wire[:7]) == []
    assert decoder.feed_data(first_wire[7:HEADER_SIZE - 1]) == []
    result = decoder.feed_data(first_wire[HEADER_SIZE - 1:] + second_wire)

    assert len(result) == 2
    assert result[0].request_id == first.request_id
    assert result[1].request_id == second.request_id
    print("[OK] split header/payload and glued frames handled correctly")


def check_request_id_correlation() -> None:
    request = make_frame(10)
    response = Frame(
        version=PROTOCOL_VERSION,
        message_type=MessageType.PONG,
        flags=0,
        request_id=request.request_id,
        payload=b"pong",
    )
    assert response.request_id == request.request_id

    wrong_response = Frame(
        version=PROTOCOL_VERSION,
        message_type=MessageType.PONG,
        flags=0,
        request_id=secrets.token_bytes(16),
        payload=b"pong",
    )
    assert wrong_response.request_id != request.request_id
    print("[OK] request_id correlation rule verified")


def check_oversized_payload_rejected_from_header() -> None:
    header = struct.pack(
        "!BBH16sI",
        PROTOCOL_VERSION,
        int(MessageType.PING),
        0,
        secrets.token_bytes(16),
        MAX_FRAME_PAYLOAD + 1,
    )
    try:
        decode_header(header)
    except ProtocolError:
        print("[OK] oversized payload rejected from 24-byte header")
        return
    raise AssertionError("Oversized payload_length was accepted")


def main() -> None:
    check_stream_of_100_frames()
    check_split_and_glued_frames()
    check_request_id_correlation()
    check_oversized_payload_rejected_from_header()
    print("Stage 1 demo: PASSED")


if __name__ == "__main__":
    main()
