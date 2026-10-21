
import pytest

from p2p.identity.identity import load_or_create_identity
from p2p.protocol.contact import Contact, validate_contact
from p2p.protocol.payload_codec import PayloadError


def make_contact(identity):
    return Contact(
        node_id=identity.node_id,
        identity_algorithm="ed25519",
        identity_public_key=identity.public_key,
        host="127.0.0.1",
        port=9101,
    )


def test_valid_ed25519_contact(tmp_path):
    identity = load_or_create_identity(tmp_path / "node1")
    contact = make_contact(identity)

    validate_contact(contact)
    assert Contact.from_dict(contact.to_dict()) == contact


def test_public_key_substitution_rejected(tmp_path):
    first = load_or_create_identity(tmp_path / "node1")
    second = load_or_create_identity(tmp_path / "node2")

    forged = Contact(
        node_id=first.node_id,
        identity_algorithm="ed25519",
        identity_public_key=second.public_key,
        host="127.0.0.1",
        port=9101,
    )

    with pytest.raises(PayloadError):
        validate_contact(forged)


def test_invalid_node_id_length_rejected(tmp_path):
    identity = load_or_create_identity(tmp_path / "node1")

    forged = Contact(
        node_id=b"\x00" * 16,
        identity_algorithm="ed25519",
        identity_public_key=identity.public_key,
        host="127.0.0.1",
        port=9101,
    )

    with pytest.raises(PayloadError):
        validate_contact(forged)


def test_unsupported_algorithm_rejected(tmp_path):
    identity = load_or_create_identity(tmp_path / "node1")

    contact = Contact(
        node_id=identity.node_id,
        identity_algorithm="unknown",
        identity_public_key=identity.public_key,
        host="127.0.0.1",
        port=9101,
    )

    with pytest.raises(PayloadError):
        validate_contact(contact)
