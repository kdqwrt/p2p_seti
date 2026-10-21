
import hashlib
import os
from dataclasses import dataclass
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)


IDENTITY_ALGORITHM = "ed25519"
PRIVATE_KEY_FILENAME = "identity_ed25519.key"

PUBLIC_KEY_SIZE = 32
NODE_ID_SIZE = 32


class IdentityError(Exception):
    """Ошибка загрузки или проверки идентичности узла."""


def calculate_node_id(public_key: bytes) -> bytes:
    """
    Вычисляет NodeID из публичного ключа Ed25519.

    Каноническое представление:
    32 байта публичного ключа в формате Raw.
    """
    if not isinstance(public_key, bytes):
        raise IdentityError("Public key must be bytes")

    if len(public_key) != PUBLIC_KEY_SIZE:
        raise IdentityError(
            "Ed25519 public key must be 32 bytes"
        )

    try:
        Ed25519PublicKey.from_public_bytes(public_key)
    except ValueError as exc:
        raise IdentityError(
            "Invalid Ed25519 public key"
        ) from exc

    return hashlib.sha256(public_key).digest()


@dataclass(frozen=True, slots=True)
class NodeIdentity:
    """Долговременная криптографическая идентичность узла."""

    private_key: Ed25519PrivateKey
    public_key: bytes
    node_id: bytes

    @classmethod
    def from_private_key(
        cls,
        private_key: Ed25519PrivateKey,
    ) -> "NodeIdentity":
        public_key = private_key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )

        return cls(
            private_key=private_key,
            public_key=public_key,
            node_id=calculate_node_id(public_key),
        )

    def sign(self, data: bytes) -> bytes:
        """Подписывает данные закрытым ключом."""
        return self.private_key.sign(data)

    def verify(self, signature: bytes, data: bytes) -> None:
        """Проверяет подпись собственным публичным ключом."""
        public_key = Ed25519PublicKey.from_public_bytes(
            self.public_key
        )
        public_key.verify(signature, data)


def load_or_create_identity(
    state_dir: str | Path,
) -> NodeIdentity:
    """
    Загружает существующую идентичность или создаёт новую.

    Закрытый ключ хранится в индивидуальном каталоге узла.
    """
    directory = Path(state_dir)
    directory.mkdir(parents=True, exist_ok=True)

    key_path = directory / PRIVATE_KEY_FILENAME

    if key_path.exists():
        try:
            private_key_bytes = key_path.read_bytes()

            private_key = (
                Ed25519PrivateKey.from_private_bytes(
                    private_key_bytes
                )
            )

        except (OSError, ValueError) as exc:
            raise IdentityError(
                "Cannot load existing identity key"
            ) from exc

        return NodeIdentity.from_private_key(private_key)

    private_key = Ed25519PrivateKey.generate()

    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PrivateFormat.Raw,
        encryption_algorithm=serialization.NoEncryption(),
    )

    # Создаём новый файл без перезаписи существующего.
    # На POSIX используем права 0600.
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL

    try:
        fd = os.open(key_path, flags, 0o600)
    except FileExistsError as exc:
        raise IdentityError(
            "Identity key was created concurrently"
        ) from exc

    try:
        with os.fdopen(fd, "wb") as file:
            file.write(private_key_bytes)
    except OSError as exc:
        raise IdentityError(
            "Cannot save identity key"
        ) from exc

    return NodeIdentity.from_private_key(private_key)
