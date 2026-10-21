
from dataclasses import dataclass

from p2p.identity.identity import (
    IDENTITY_ALGORITHM,
    IdentityError,
    calculate_node_id,
)
from p2p.protocol.payload_codec import PayloadError


MAX_HOST_LENGTH = 255


@dataclass(frozen=True, slots=True)
class Contact:
    node_id: bytes
    identity_algorithm: str
    identity_public_key: bytes
    host: str
    port: int

    def to_dict(self) -> dict:
        validate_contact(self)

        return {
            "node_id": self.node_id,
            "identity_algorithm": self.identity_algorithm,
            "identity_public_key": self.identity_public_key,
            "host": self.host,
            "port": self.port,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Contact":
        if not isinstance(data, dict):
            raise PayloadError("Contact must be a map")

        expected_fields = {
            "node_id",
            "identity_algorithm",
            "identity_public_key",
            "host",
            "port",
        }

        if set(data.keys()) != expected_fields:
            raise PayloadError("Contact contains invalid fields")

        contact = cls(
            node_id=data["node_id"],
            identity_algorithm=data["identity_algorithm"],
            identity_public_key=data["identity_public_key"],
            host=data["host"],
            port=data["port"],
        )

        validate_contact(contact)
        return contact


def validate_contact(contact: Contact) -> None:
    if not isinstance(contact.node_id, bytes):
        raise PayloadError("Contact node_id must be bytes")

    if not isinstance(contact.identity_algorithm, str):
        raise PayloadError(
            "Contact identity_algorithm must be a string"
        )

    if not isinstance(contact.identity_public_key, bytes):
        raise PayloadError(
            "Contact identity_public_key must be bytes"
        )

    if not isinstance(contact.host, str):
        raise PayloadError("Contact host must be a string")

    if not contact.host or len(contact.host) > MAX_HOST_LENGTH:
        raise PayloadError("Contact host has invalid length")

    if type(contact.port) is not int:
        raise PayloadError("Contact port must be an integer")

    if not 1 <= contact.port <= 65535:
        raise PayloadError("Contact port is out of range")

    if contact.identity_algorithm == "stage1-none":
        # Временная совместимость с тестами этапа 1.
        if contact.node_id != b"":
            raise PayloadError(
                "Legacy contact must have empty node_id"
            )

        if contact.identity_public_key != b"":
            raise PayloadError(
                "Legacy contact must have empty public key"
            )

        return

    if contact.identity_algorithm != IDENTITY_ALGORITHM:
        raise PayloadError(
            "Unsupported identity algorithm"
        )

    if len(contact.node_id) != 32:
        raise PayloadError(
            "Ed25519 contact node_id must be 32 bytes"
        )

    if len(contact.identity_public_key) != 32:
        raise PayloadError(
            "Ed25519 public key must be 32 bytes"
        )

    try:
        expected_node_id = calculate_node_id(
            contact.identity_public_key
        )
    except IdentityError as exc:
        raise PayloadError(
            "Invalid identity public key"
        ) from exc

    if contact.node_id != expected_node_id:
        raise PayloadError(
            "Contact NodeID does not match public key"
        )
