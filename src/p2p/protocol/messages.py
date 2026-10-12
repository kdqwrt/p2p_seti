from p2p.protocol.contact import Contact
from p2p.protocol.payload_codec import (
    PayloadError,
    decode_payload,
    encode_payload,
)


MAX_TIMESTAMP_MS = 0xFFFFFFFFFFFFFFFF
MAX_ERROR_CODE_LENGTH = 64
MAX_ERROR_MESSAGE_LENGTH = 256


def _validate_timestamp(
    value: int,
    field_name: str,
) -> None:
    if type(value) is not int:
        raise PayloadError(
            f"{field_name} must be an integer"
        )

    if not 0 <= value <= MAX_TIMESTAMP_MS:
        raise PayloadError(
            f"{field_name} is out of uint64 range"
        )


def encode_ping(
    sender: Contact,
    timestamp_ms: int,
) -> bytes:
    _validate_timestamp(timestamp_ms, "timestamp_ms")

    return encode_payload(
        {
            "sender": sender.to_dict(),
            "timestamp_ms": timestamp_ms,
        }
    )


def decode_ping(data: bytes) -> dict:
    payload = decode_payload(data)

    if set(payload.keys()) != {
        "sender",
        "timestamp_ms",
    }:
        raise PayloadError(
            "PING payload contains invalid fields"
        )

    sender = Contact.from_dict(payload["sender"])

    timestamp_ms = payload["timestamp_ms"]
    _validate_timestamp(
        timestamp_ms,
        "PING timestamp_ms",
    )

    return {
        "sender": sender,
        "timestamp_ms": timestamp_ms,
    }


def encode_pong(
    responder: Contact,
    ping_timestamp_ms: int,
    responder_timestamp_ms: int,
) -> bytes:
    _validate_timestamp(
        ping_timestamp_ms,
        "ping_timestamp_ms",
    )
    _validate_timestamp(
        responder_timestamp_ms,
        "responder_timestamp_ms",
    )

    return encode_payload(
        {
            "responder": responder.to_dict(),
            "ping_timestamp_ms": ping_timestamp_ms,
            "responder_timestamp_ms": responder_timestamp_ms,
        }
    )


def decode_pong(data: bytes) -> dict:
    payload = decode_payload(data)

    if set(payload.keys()) != {
        "responder",
        "ping_timestamp_ms",
        "responder_timestamp_ms",
    }:
        raise PayloadError(
            "PONG payload contains invalid fields"
        )

    responder = Contact.from_dict(
        payload["responder"]
    )

    ping_timestamp_ms = payload["ping_timestamp_ms"]
    responder_timestamp_ms = payload[
        "responder_timestamp_ms"
    ]

    _validate_timestamp(
        ping_timestamp_ms,
        "PONG ping_timestamp_ms",
    )
    _validate_timestamp(
        responder_timestamp_ms,
        "PONG responder_timestamp_ms",
    )

    return {
        "responder": responder,
        "ping_timestamp_ms": ping_timestamp_ms,
        "responder_timestamp_ms": responder_timestamp_ms,
    }


def encode_error(
    code: str,
    message: str,
) -> bytes:
    if not isinstance(code, str):
        raise PayloadError(
            "ERROR code must be a string"
        )

    if not code or len(code) > MAX_ERROR_CODE_LENGTH:
        raise PayloadError(
            "ERROR code has invalid length"
        )

    if not isinstance(message, str):
        raise PayloadError(
            "ERROR message must be a string"
        )

    if not message or len(message) > MAX_ERROR_MESSAGE_LENGTH:
        raise PayloadError(
            "ERROR message has invalid length"
        )

    return encode_payload(
        {
            "code": code,
            "message": message,
        }
    )


def decode_error(data: bytes) -> dict:
    payload = decode_payload(data)

    if set(payload.keys()) != {
        "code",
        "message",
    }:
        raise PayloadError(
            "ERROR payload contains invalid fields"
        )

    code = payload["code"]
    message = payload["message"]

    if not isinstance(code, str):
        raise PayloadError(
            "ERROR code must be a string"
        )

    if not code or len(code) > MAX_ERROR_CODE_LENGTH:
        raise PayloadError(
            "ERROR code has invalid length"
        )

    if not isinstance(message, str):
        raise PayloadError(
            "ERROR message must be a string"
        )

    if not message or len(message) > MAX_ERROR_MESSAGE_LENGTH:
        raise PayloadError(
            "ERROR message has invalid length"
        )

    return payload