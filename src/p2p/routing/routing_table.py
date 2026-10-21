
import asyncio
from collections.abc import Awaitable, Callable

from p2p.protocol.contact import Contact, validate_contact
from p2p.protocol.payload_codec import PayloadError
from p2p.routing.distance import (
    NODE_ID_BITS,
    bucket_index,
    validate_node_id,
    xor_distance,
)
from p2p.routing.k_bucket import KBucket


PingCallback = Callable[[Contact], Awaitable[bool]]


class RoutingTable:
    """Таблица маршрутизации Kademlia с 256 bucket."""

    def __init__(
        self,
        local_node_id: bytes,
        k_bucket_size: int,
    ) -> None:
        validate_node_id(local_node_id)

        if k_bucket_size <= 0:
            raise ValueError("K bucket size must be positive")

        self.local_node_id = local_node_id
        self.k_bucket_size = k_bucket_size

        self.buckets = [
            KBucket(capacity=k_bucket_size)
            for _ in range(NODE_ID_BITS)
        ]

        # Каждый bucket блокируется независимо.
        self._locks = [
            asyncio.Lock()
            for _ in range(NODE_ID_BITS)
        ]

    def get_bucket(self, node_id: bytes) -> KBucket:
        index = bucket_index(self.local_node_id, node_id)
        return self.buckets[index]

    def get_contact(self, node_id: bytes) -> Contact | None:
        validate_node_id(node_id)

        if node_id == self.local_node_id:
            return None

        return self.get_bucket(node_id).find(node_id)

    async def add_contact(
        self,
        contact: Contact,
        ping_callback: PingCallback,
        *,
        verified: bool = True,
    ) -> bool:
        """
        Добавляет контакт в таблицу маршрутизации.

        verified=True означает, что вызывающий код уже
        подтвердил доступность контакта сетевым обменом.

        verified=False означает, что контакт получен из
        непроверенного источника. Такой контакт не добавляется.

        ping_callback проверяет старейший контакт при
        переполнении bucket.
        """
        validate_contact(contact)

        if contact.identity_algorithm != "ed25519":
            raise PayloadError(
                "Routing table requires Ed25519 identity"
            )

        if contact.node_id == self.local_node_id:
            return False

        if not verified:
            return False

        index = bucket_index(
            self.local_node_id,
            contact.node_id,
        )

        async with self._locks[index]:
            bucket = self.buckets[index]
            existing = bucket.find(contact.node_id)

            if existing is not None:
                if (
                    existing.identity_public_key
                    != contact.identity_public_key
                ):
                    raise PayloadError(
                        "Conflicting identity public key"
                    )

                if (
                    existing.host != contact.host
                    or existing.port != contact.port
                ):
                    return False

                bucket.touch(contact)
                return True

            if not bucket.is_full():
                return bucket.add(contact)

            oldest = bucket.oldest()

            if oldest is None:
                return bucket.add(contact)

            try:
                alive = await ping_callback(oldest)
            except Exception:
                alive = False

            if alive:
                # Старый узел ответил: переносим его
                # в конец LRU и не добавляем новый.
                bucket.touch(oldest)
                return False

            # Старый узел недоступен.
            bucket.remove(oldest.node_id)
            return bucket.add(contact)

    def closest(
        self,
        target_node_id: bytes,
        limit: int | None = None,
    ) -> list[Contact]:
        """Возвращает ближайшие контакты по XOR-расстоянию."""
        validate_node_id(target_node_id)

        if limit is None:
            limit = self.k_bucket_size

        if limit < 0:
            raise ValueError("Limit cannot be negative")

        contacts = [
            contact
            for bucket in self.buckets
            for contact in bucket.contacts
        ]

        contacts.sort(
            key=lambda contact: xor_distance(
                contact.node_id,
                target_node_id,
            )
        )

        return contacts[:limit]

    def size(self) -> int:
        """Возвращает общее количество контактов."""
        return sum(
            len(bucket.contacts)
            for bucket in self.buckets
        )
