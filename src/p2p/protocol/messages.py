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


def _validate_find_node_id(value: bytes) -> None:
    if not isinstance(value, bytes) or len(value) != 32:
        raise PayloadError(
            "target_node_id must be exactly 32 bytes"
        )


def encode_find_node_request(
    sender: Contact,
    target_node_id: bytes,
) -> bytes:
    """Кодирует запрос поиска ближайших узлов."""
    _validate_find_node_id(target_node_id)

    return encode_payload({
        "sender": sender.to_dict(),
        "target_node_id": target_node_id,
    })


def decode_find_node_request(data: bytes) -> dict:
    """Декодирует и проверяет запрос FIND_NODE."""
    payload = decode_payload(data)

    if set(payload) != {"sender", "target_node_id"}:
        raise PayloadError(
            "FIND_NODE_REQUEST payload contains invalid fields"
        )

    sender = Contact.from_dict(payload["sender"])
    target_node_id = payload["target_node_id"]
    _validate_find_node_id(target_node_id)

    return {
        "sender": sender,
        "target_node_id": target_node_id,
    }


def encode_find_node_response(
    responder: Contact,
    target_node_id: bytes,
    contacts: list[Contact],
) -> bytes:
    """Кодирует ответ со списком ближайших контактов."""
    _validate_find_node_id(target_node_id)

    if not isinstance(contacts, list):
        raise PayloadError("contacts must be a list")

    if len(contacts) > 256:
        raise PayloadError("Too many contacts")

    return encode_payload({
        "responder": responder.to_dict(),
        "target_node_id": target_node_id,
        "contacts": [
            contact.to_dict() for contact in contacts
        ],
    })


def decode_find_node_response(data: bytes) -> dict:
    """Декодирует ответ FIND_NODE."""
    payload = decode_payload(data)

    if set(payload) != {
        "responder", "target_node_id", "contacts"
    }:
        raise PayloadError(
            "FIND_NODE_RESPONSE payload contains invalid fields"
        )

    responder = Contact.from_dict(payload["responder"])
    target_node_id = payload["target_node_id"]
    _validate_find_node_id(target_node_id)

    raw_contacts = payload["contacts"]

    if not isinstance(raw_contacts, list):
        raise PayloadError("contacts must be a list")

    if len(raw_contacts) > 256:
        raise PayloadError("Too many contacts")

    contacts = [
        Contact.from_dict(item)
        for item in raw_contacts
    ]

    return {
        "responder": responder,
        "target_node_id": target_node_id,
        "contacts": contacts,
    }
