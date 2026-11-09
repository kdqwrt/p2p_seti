
from p2p.dht.alias_records import (
    AliasRecord,
    AliasRecordError,
    alias_record_key,
    validate_alias_record,
)
from p2p.protocol.payload_codec import (
    PayloadError,
    decode_payload,
    encode_payload,
)


MAX_ALIAS_RESULTS = 8


def _fields(payload: dict, expected: set[str]) -> None:
    if type(payload) is not dict or set(payload) != expected:
        raise PayloadError("Invalid alias message fields")


def _key(key: bytes) -> None:
    if type(key) is not bytes or len(key) != 32:
        raise PayloadError("Alias DHT key must be 32 bytes")


def _record_from_bytes(raw: bytes) -> AliasRecord:
    if type(raw) is not bytes:
        raise PayloadError("Alias record must be bytes")

    try:
        record = AliasRecord.from_bytes(raw)
        validate_alias_record(record)
        return record
    except AliasRecordError as exc:
        raise PayloadError("Invalid signed AliasRecord") from exc


def encode_store_alias_request(record: AliasRecord) -> bytes:
    try:
        validate_alias_record(record)
        key = alias_record_key(record.alias)
    except AliasRecordError as exc:
        raise PayloadError("Invalid AliasRecord") from exc

    return encode_payload({
        "key": key,
        "record": record.to_bytes(),
    })


def decode_store_alias_request(data: bytes) -> dict:
    payload = decode_payload(data)
    _fields(payload, {"key", "record"})
    _key(payload["key"])

    record = _record_from_bytes(payload["record"])

    if payload["key"] != alias_record_key(record.alias):
        raise PayloadError("Alias DHT key mismatch")

    return {
        "key": payload["key"],
        "record": record,
    }


def encode_store_alias_response(
    key: bytes,
    stored: bool,
) -> bytes:
    _key(key)
    if type(stored) is not bool:
        raise PayloadError("Stored must be boolean")

    return encode_payload({
        "key": key,
        "stored": stored,
    })


def decode_store_alias_response(data: bytes) -> dict:
    payload = decode_payload(data)
    _fields(payload, {"key", "stored"})
    _key(payload["key"])

    if type(payload["stored"]) is not bool:
        raise PayloadError("Invalid stored flag")

    return payload


def encode_find_alias_request(alias: str) -> bytes:
    try:
        key = alias_record_key(alias)
    except AliasRecordError as exc:
        raise PayloadError("Invalid alias") from exc

    return encode_payload({
        "alias": alias,
        "key": key,
    })


def decode_find_alias_request(data: bytes) -> dict:
    payload = decode_payload(data)
    _fields(payload, {"alias", "key"})
    _key(payload["key"])

    try:
        expected = alias_record_key(payload["alias"])
    except AliasRecordError as exc:
        raise PayloadError("Invalid alias") from exc

    if payload["key"] != expected:
        raise PayloadError("Alias DHT key mismatch")

    return payload


def encode_find_alias_response(
    alias: str,
    records: list[AliasRecord],
) -> bytes:
    try:
        key = alias_record_key(alias)
    except AliasRecordError as exc:
        raise PayloadError("Invalid alias") from exc

    if (
        type(records) is not list
        or len(records) > MAX_ALIAS_RESULTS
    ):
        raise PayloadError("Too many alias records")

    owners = set()
    raw_records = []

    for record in records:
        try:
            validate_alias_record(record)
        except AliasRecordError as exc:
            raise PayloadError("Invalid AliasRecord") from exc

        if record.alias != alias:
            raise PayloadError("Alias mismatch")

        if record.node_id in owners:
            raise PayloadError("Duplicate alias owner")

        owners.add(record.node_id)
        raw_records.append(record.to_bytes())

    return encode_payload({
        "alias": alias,
        "key": key,
        "records": raw_records,
    })


def decode_find_alias_response(data: bytes) -> dict:
    payload = decode_payload(data)
    _fields(payload, {"alias", "key", "records"})
    _key(payload["key"])

    try:
        expected = alias_record_key(payload["alias"])
    except AliasRecordError as exc:
        raise PayloadError("Invalid alias") from exc

    if payload["key"] != expected:
        raise PayloadError("Alias DHT key mismatch")

    raw_records = payload["records"]

    if (
        type(raw_records) is not list
        or len(raw_records) > MAX_ALIAS_RESULTS
    ):
        raise PayloadError("Invalid alias records list")

    records = []
    owners = set()

    for raw in raw_records:
        record = _record_from_bytes(raw)

        if record.alias != payload["alias"]:
            raise PayloadError("Unexpected alias")

        if record.node_id in owners:
            raise PayloadError("Duplicate alias owner")

        owners.add(record.node_id)
        records.append(record)

    return {
        "alias": payload["alias"],
        "key": payload["key"],
        "records": records,
    }
