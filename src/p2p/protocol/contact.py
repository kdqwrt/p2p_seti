from dataclasses import dataclass

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
            raise PayloadError(
                "Contact contains invalid fields"
            )

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

    # Полная длина NodeID будет обязательна после появления
    # криптографической identity на этапе 2.
    if len(contact.node_id) > 32:
        raise PayloadError(
            "Contact node_id must not exceed 32 bytes"
        )

    if not isinstance(contact.identity_algorithm, str):
        raise PayloadError(
            "Contact identity_algorithm must be a string"
        )

    if len(contact.identity_algorithm) > 32:
        raise PayloadError(
            "Contact identity_algorithm is too long"
        )

    if not isinstance(contact.identity_public_key, bytes):
        raise PayloadError(
            "Contact identity_public_key must be bytes"
        )

    if len(contact.identity_public_key) > 128:
        raise PayloadError(
            "Contact identity_public_key is too large"
        )

    if not isinstance(contact.host, str):
        raise PayloadError("Contact host must be a string")

    if not contact.host or len(contact.host) > MAX_HOST_LENGTH:
        raise PayloadError("Contact host has invalid length")

    if type(contact.port) is not int:
        raise PayloadError("Contact port must be an integer")

    if not 1 <= contact.port <= 65535:
        raise PayloadError("Contact port is out of range")