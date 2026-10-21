
import hashlib

import pytest
from cryptography.exceptions import InvalidSignature

from p2p.identity.identity import (
    IdentityError,
    calculate_node_id,
    load_or_create_identity,
)


def test_identity_persists_after_restart(tmp_path):
    """T2.1: идентичность сохраняется после перезапуска."""
    state_dir = tmp_path / "node1"

    first = load_or_create_identity(state_dir)
    second = load_or_create_identity(state_dir)

    assert first.node_id == second.node_id
    assert first.public_key == second.public_key
    assert len(first.node_id) == 32

    assert (state_dir / "identity_ed25519.key").exists()


def test_different_nodes_have_different_ids(tmp_path):
    """T2.2: независимые узлы получают разные ключи."""
    node1 = load_or_create_identity(tmp_path / "node1")
    node2 = load_or_create_identity(tmp_path / "node2")

    assert node1.node_id != node2.node_id
    assert node1.public_key != node2.public_key


def test_node_id_matches_public_key(tmp_path):
    """T2.3: NodeID связан с публичным ключом."""
    identity = load_or_create_identity(tmp_path / "node1")

    expected = hashlib.sha256(
        identity.public_key
    ).digest()

    assert identity.node_id == expected
    assert calculate_node_id(identity.public_key) == expected


def test_invalid_public_key_length():
    """Публичный ключ неверной длины отклоняется."""
    with pytest.raises(IdentityError):
        calculate_node_id(b"invalid")


def test_signature_verification(tmp_path):
    """Корректная подпись проходит проверку."""
    identity = load_or_create_identity(tmp_path / "node1")

    message = b"test-message"
    signature = identity.sign(message)

    identity.verify(signature, message)


def test_modified_message_rejected(tmp_path):
    """Изменённое сообщение не проходит проверку подписи."""
    identity = load_or_create_identity(tmp_path / "node1")

    signature = identity.sign(b"original")

    with pytest.raises(InvalidSignature):
        identity.verify(signature, b"modified")


def test_corrupted_private_key_rejected(tmp_path):
    """Повреждённый ключ не заменяется новым."""
    state_dir = tmp_path / "node1"
    state_dir.mkdir()

    key_path = state_dir / "identity_ed25519.key"
    key_path.write_bytes(b"broken-key")

    with pytest.raises(IdentityError):
        load_or_create_identity(state_dir)

    assert key_path.read_bytes() == b"broken-key"
