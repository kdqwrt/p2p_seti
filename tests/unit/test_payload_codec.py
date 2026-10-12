import pytest

from p2p.protocol.contact import Contact
from p2p.protocol.messages import (
    decode_error,
    decode_ping,
    decode_pong,
    encode_error,
    encode_ping,
    encode_pong,
)
from p2p.protocol.payload_codec import (
    PayloadError,
    encode_payload,
)


@pytest.fixture
def contact() -> Contact:
    return Contact(
        node_id=b"",
        identity_algorithm="stage1-none",
        identity_public_key=b"",
        host="127.0.0.1",
        port=9101,
    )


def test_ping_payload_roundtrip(contact):
    encoded = encode_ping(
        sender=contact,
        timestamp_ms=123456789,
    )

    decoded = decode_ping(encoded)

    assert decoded["sender"] == contact
    assert decoded["timestamp_ms"] == 123456789


def test_pong_payload_roundtrip(contact):
    encoded = encode_pong(
        responder=contact,
        ping_timestamp_ms=123456789,
        responder_timestamp_ms=987654321,
    )

    decoded = decode_pong(encoded)

    assert decoded["responder"] == contact
    assert decoded["ping_timestamp_ms"] == 123456789
    assert decoded["responder_timestamp_ms"] == 987654321


def test_ping_rejects_invalid_msgpack():
    with pytest.raises(PayloadError):
        decode_ping(b"\xc1")


def test_ping_rejects_wrong_timestamp_type(contact):
    payload = encode_payload(
        {
            "sender": contact.to_dict(),
            "timestamp_ms": "not-an-integer",
        }
    )

    with pytest.raises(
        PayloadError,
        match="must be an integer",
    ):
        decode_ping(payload)


def test_ping_rejects_unknown_fields(contact):
    payload = encode_payload(
        {
            "sender": contact.to_dict(),
            "timestamp_ms": 123,
            "unexpected": True,
        }
    )

    with pytest.raises(
        PayloadError,
        match="invalid fields",
    ):
        decode_ping(payload)


def test_timestamp_rejects_negative_value(contact):
    with pytest.raises(
        PayloadError,
        match="out of uint64 range",
    ):
        encode_ping(
            sender=contact,
            timestamp_ms=-1,
        )


def test_timestamp_rejects_value_above_uint64(contact):
    with pytest.raises(
        PayloadError,
        match="out of uint64 range",
    ):
        encode_ping(
            sender=contact,
            timestamp_ms=2**64,
        )


def test_error_payload_roundtrip():
    encoded = encode_error(
        code="INVALID_PAYLOAD",
        message="Payload validation failed",
    )

    decoded = decode_error(encoded)

    assert decoded == {
        "code": "INVALID_PAYLOAD",
        "message": "Payload validation failed",
    }


def test_error_rejects_unknown_fields():
    payload = encode_payload(
        {
            "code": "TEST",
            "message": "test",
            "secret": "must not be here",
        }
    )

    with pytest.raises(
        PayloadError,
        match="invalid fields",
    ):
        decode_error(payload)