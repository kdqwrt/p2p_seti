from p2p.dht.records import node_record_key
from p2p.dht.records import (
    NodeRecord,
    RecordError,
    validate_node_record,
)
from p2p.protocol.payload_codec import (
    PayloadError,
    decode_payload,
    encode_payload,
)


def _check_fields(payload: dict, expected: set[str]) -> None:
    if set(payload) != expected:
        raise PayloadError("Invalid DHT message fields")


def _check_key(key: bytes) -> None:
    if not isinstance(key, bytes) or len(key) != 32:
        raise PayloadError("DHT key must be 32 bytes")


def encode_store_request(
    key: bytes,
    record: NodeRecord,
) -> bytes:
    _check_key(key)

    validate_node_record(record)

    if key != node_record_key(record.node_id):
        raise PayloadError("DHT key does not match NodeRecord")

    return encode_payload({
        "key": key,
        "record": record.to_bytes(),
    })


def decode_store_request(data: bytes) -> dict:
    payload = decode_payload(data)
    _check_fields(payload, {"key", "record"})

    key = payload["key"]
    _check_key(key)

    raw_record = payload["record"]

    if not isinstance(raw_record, bytes):
        raise PayloadError("Record must be bytes")

    try:
        record = NodeRecord.from_bytes(raw_record)
        validate_node_record(record)
    except RecordError as exc:
        raise PayloadError("Invalid signed NodeRecord") from exc

    if key != node_record_key(record.node_id):
        raise PayloadError("DHT key does not match NodeRecord")
    return {
        "key": key,
        "record": record,
    }


def encode_store_response(
    key: bytes,
    stored: bool,
) -> bytes:
    _check_key(key)

    if type(stored) is not bool:
        raise PayloadError("Stored must be boolean")

    return encode_payload({
        "key": key,
        "stored": stored,
    })


def decode_store_response(data: bytes) -> dict:
    payload = decode_payload(data)
    _check_fields(payload, {"key", "stored"})

    _check_key(payload["key"])

    if type(payload["stored"]) is not bool:
        raise PayloadError("Invalid stored flag")

    return payload


def encode_find_value_request(key: bytes) -> bytes:
    _check_key(key)

    return encode_payload({
        "key": key,
    })


def decode_find_value_request(data: bytes) -> dict:
    payload = decode_payload(data)
    _check_fields(payload, {"key"})

    _check_key(payload["key"])
    return payload


def encode_find_value_response(
    key: bytes,
    record: NodeRecord | None,
) -> bytes:
    _check_key(key)

    return encode_payload({
        "key": key,
        "found": record is not None,
        "record": record.to_bytes() if record else None,
    })


def decode_find_value_response(data: bytes) -> dict:
    payload = decode_payload(data)

    _check_fields(payload, {"key", "found", "record"})
    _check_key(payload["key"])

    if type(payload["found"]) is not bool:
        raise PayloadError("Invalid found flag")

    if payload["found"]:
        if not isinstance(payload["record"], bytes):
            raise PayloadError("Found record must be bytes")

        try:
            record = NodeRecord.from_bytes(payload["record"])
            validate_node_record(record)
        except RecordError as exc:
            raise PayloadError("Invalid returned NodeRecord") from exc
    else:
        if payload["record"] is not None:
            raise PayloadError("Unexpected record")
        record = None

    return {
        "key": payload["key"],
        "found": payload["found"],
        "record": record,
    }
