import msgpack


class PayloadError(Exception):
    """Ошибка сериализации или проверки payload."""


def encode_payload(payload: dict) -> bytes:
    if not isinstance(payload, dict):
        raise PayloadError(
            "Payload must be a dictionary"
        )

    try:
        return msgpack.packb(
            payload,
            use_bin_type=True,
        )
    except (TypeError, ValueError) as exc:
        raise PayloadError(
            "Failed to encode payload"
        ) from exc


def decode_payload(data: bytes) -> dict:
    try:
        payload = msgpack.unpackb(
            data,
            raw=False,
            strict_map_key=True,
        )
    except (
        msgpack.UnpackException,
        ValueError,
        TypeError,
    ) as exc:
        raise PayloadError(
            "Failed to decode payload"
        ) from exc

    if not isinstance(payload, dict):
        raise PayloadError(
            "Decoded payload must be a dictionary"
        )

    return payload