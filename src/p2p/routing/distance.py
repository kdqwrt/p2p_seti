
NODE_ID_SIZE = 32
NODE_ID_BITS = 256


def validate_node_id(node_id: bytes) -> None:
    """Проверяет формат 256-битного идентификатора."""
    if not isinstance(node_id, bytes):
        raise ValueError("NodeID must be bytes")

    if len(node_id) != NODE_ID_SIZE:
        raise ValueError("NodeID must be exactly 32 bytes")


def xor_distance(first: bytes, second: bytes) -> int:
    """Возвращает XOR-расстояние между двумя NodeID."""
    validate_node_id(first)
    validate_node_id(second)

    return int.from_bytes(first, "big") ^ int.from_bytes(
        second, "big"
    )


def bucket_index(local_id: bytes, remote_id: bytes) -> int:
    """
    Определяет индекс bucket по старшему установленному биту.

    Результат: 0..255.
    Одинаковые NodeID не имеют bucket.
    """
    distance = xor_distance(local_id, remote_id)

    if distance == 0:
        raise ValueError("Local NodeID cannot have a bucket")

    return distance.bit_length() - 1
