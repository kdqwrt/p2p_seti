
from dataclasses import dataclass, field

from p2p.protocol.contact import Contact


@dataclass
class KBucket:
    """Ограниченный список контактов с LRU-порядком."""

    capacity: int
    contacts: list[Contact] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.capacity <= 0:
            raise ValueError("Bucket capacity must be positive")

    def find(self, node_id: bytes) -> Contact | None:
        for contact in self.contacts:
            if contact.node_id == node_id:
                return contact
        return None

    def is_full(self) -> bool:
        return len(self.contacts) >= self.capacity

    def oldest(self) -> Contact | None:
        """Возвращает наименее недавно подтверждённый контакт."""
        return self.contacts[0] if self.contacts else None

    def touch(self, contact: Contact) -> None:
        """Обновляет контакт и переносит его в конец списка."""
        self.contacts = [
            item for item in self.contacts
            if item.node_id != contact.node_id
        ]
        self.contacts.append(contact)

    def add(self, contact: Contact) -> bool:
        """
        Добавляет контакт в неполный bucket.

        Существующий контакт обновляется.
        При заполненном bucket новый контакт не добавляется.
        """
        existing = self.find(contact.node_id)

        if existing is not None:
            self.touch(contact)
            return True

        if self.is_full():
            return False

        self.contacts.append(contact)
        return True

    def remove(self, node_id: bytes) -> bool:
        before = len(self.contacts)
        self.contacts = [
            item for item in self.contacts
            if item.node_id != node_id
        ]
        return len(self.contacts) < before
