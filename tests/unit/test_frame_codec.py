import secrets
import random
from p2p.protocol.codec import HEADER_STRUCT, encode_frame
from p2p.protocol.constants import (
    HEADER_SIZE,
    PROTOCOL_VERSION,
    MessageType,
)
from p2p.protocol.frame import Frame
from p2p.transport.stream_decoder import StreamDecoder



def test_header_size_is_24_bytes():
    assert HEADER_STRUCT.size == HEADER_SIZE
    assert HEADER_SIZE == 24


def test_encode_ping_frame():
    request_id = secrets.token_bytes(16)
    payload = b'{"timestamp_ms":123456789}'

    frame = Frame(
        version=PROTOCOL_VERSION,
        message_type=MessageType.PING,
        flags=0,
        request_id=request_id,
        payload=payload,
    )

    encoded = encode_frame(frame)

    assert len(encoded) == HEADER_SIZE + len(payload)

    assert encoded[0] == PROTOCOL_VERSION
    assert encoded[1] == MessageType.PING

def test_encode_decode_complete_frame():
    request_id = secrets.token_bytes(16)
    payload = b"hello p2p"

    original = Frame(
        version=PROTOCOL_VERSION,
        message_type=MessageType.PING,
        flags=0,
        request_id=request_id,
        payload=payload,
    )

    encoded = encode_frame(original)

    decoder = StreamDecoder()
    frames = decoder.feed_data(encoded)

    assert len(frames) == 1

    decoded = frames[0]

    assert decoded.version == original.version
    assert decoded.message_type == original.message_type
    assert decoded.flags == original.flags
    assert decoded.request_id == original.request_id
    assert decoded.payload == original.payload

def test_partial_header():
    request_id = secrets.token_bytes(16)

    frame = Frame(
        version=PROTOCOL_VERSION,
        message_type=MessageType.PING,
        flags=0,
        request_id=request_id,
        payload=b"hello",
    )

    encoded = encode_frame(frame)

    decoder = StreamDecoder()

    frames = decoder.feed_data(encoded[:10])

    assert frames == []
    assert decoder.buffered_bytes == 10

    frames = decoder.feed_data(encoded[10:])

    assert len(frames) == 1
    assert frames[0].request_id == request_id
    assert frames[0].payload == b"hello"

def test_partial_payload():
    request_id = secrets.token_bytes(16)
    payload = b"this payload arrives in parts"

    frame = Frame(
        version=PROTOCOL_VERSION,
        message_type=MessageType.PING,
        flags=0,
        request_id=request_id,
        payload=payload,
    )

    encoded = encode_frame(frame)

    decoder = StreamDecoder()

    split_position = HEADER_SIZE + 5

    frames = decoder.feed_data(
        encoded[:split_position]
    )

    assert frames == []

    frames = decoder.feed_data(
        encoded[split_position:]
    )

    assert len(frames) == 1
    assert frames[0].payload == payload

def test_multiple_frames_in_single_chunk():
    frame1 = Frame(
        version=PROTOCOL_VERSION,
        message_type=MessageType.PING,
        flags=0,
        request_id=secrets.token_bytes(16),
        payload=b"first",
    )

    frame2 = Frame(
        version=PROTOCOL_VERSION,
        message_type=MessageType.PONG,
        flags=0,
        request_id=secrets.token_bytes(16),
        payload=b"second",
    )

    data = encode_frame(frame1) + encode_frame(frame2)

    decoder = StreamDecoder()

    frames = decoder.feed_data(data)

    assert len(frames) == 2

    assert frames[0].message_type == MessageType.PING
    assert frames[0].payload == b"first"

    assert frames[1].message_type == MessageType.PONG
    assert frames[1].payload == b"second"

    assert decoder.buffered_bytes == 0

def test_100_frames_with_random_fragmentation():
    frames = []

    for i in range(100):
        frame = Frame(
            version=PROTOCOL_VERSION,
            message_type=MessageType.PING,
            flags=0,
            request_id=secrets.token_bytes(16),
            payload=f"message-{i}".encode(),
        )

        frames.append(frame)

    stream = b"".join(
        encode_frame(frame)
        for frame in frames
    )

    decoder = StreamDecoder()

    decoded_frames = []

    position = 0

    while position < len(stream):
        chunk_size = random.randint(1, 50)

        chunk = stream[
            position:position + chunk_size
        ]

        decoded_frames.extend(
            decoder.feed_data(chunk)
        )

        position += chunk_size

    assert len(decoded_frames) == 100

    for original, decoded in zip(
        frames,
        decoded_frames,
        strict=True,
    ):
        assert decoded.version == original.version
        assert decoded.message_type == original.message_type
        assert decoded.flags == original.flags
        assert decoded.request_id == original.request_id
        assert decoded.payload == original.payload

    assert decoder.buffered_bytes == 0