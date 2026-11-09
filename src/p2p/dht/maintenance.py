
import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from p2p.dht.records import (
    NodeRecord,
    node_record_key,
    validate_node_record,
)
from p2p.protocol.contact import Contact
from p2p.routing.distance import xor_distance


logger = logging.getLogger(__name__)


FindValueCallback = Callable[
    [Contact, bytes],
    Awaitable[NodeRecord | None],
]

StoreCallback = Callable[
    [Contact, NodeRecord],
    Awaitable[bool],
]


@dataclass(slots=True)
class MaintenanceResult:
    key: bytes
    checked: int
    existing: int
    repaired: int
    confirmed: int
    required: int
    successful: bool
    fully_replicated: bool


class ReplicaMaintenance:
    """
    Проверяет и восстанавливает копии записи
    на ближайших узлах DHT.
    """

    def __init__(
        self,
        *,
        find_value: FindValueCallback,
        store: StoreCallback,
        replication_factor: int = 3,
        required_confirmations: int = 2,
    ):
        if replication_factor <= 0:
            raise ValueError(
                "replication_factor must be positive"
            )

        if not (
            1 <= required_confirmations <= replication_factor
        ):
            raise ValueError(
                "Invalid required_confirmations"
            )

        self.find_value = find_value
        self.store = store
        self.replication_factor = replication_factor
        self.required_confirmations = required_confirmations

    async def maintain(
        self,
        record: NodeRecord,
        contacts: list[Contact],
    ) -> MaintenanceResult:
        """Выполняет один цикл обслуживания записи."""

        validate_node_record(record)

        key = node_record_key(record.node_id)

        # Убираем дубликаты и сортируем по XOR.
        unique = {
            contact.node_id: contact
            for contact in contacts
        }

        candidates = sorted(
            unique.values(),
            key=lambda contact: xor_distance(
                contact.node_id,
                key,
            ),
        )

        checked = 0
        existing = 0
        repaired = 0
        confirmed = 0


        async def check_one(
            contact: Contact,
        ) -> tuple[Contact, bool, bool]:
            try:
                remote_record = await self.find_value(
                    contact,
                    key,
                )

                if remote_record is None:
                    return contact, False, False

                # Нужная версия уже хранится на узле.
                if remote_record == record:
                    return contact, True, False

                # Удалённый узел хранит более новую
                # версию. Перезаписывать её нельзя.
                if (
                    remote_record.node_id == record.node_id
                    and remote_record.sequence_number
                    > record.sequence_number
                ):
                    return contact, False, True

                return contact, False, False

            except Exception as exc:
                logger.warning(
                    "event=replica_check_failed "
                    "peer_node_id=%s error=%s",
                    contact.node_id.hex(),
                    exc,
                )

                return contact, False, False


        # Проверяем всех кандидатов параллельно.
        responses = await asyncio.gather(
            *(check_one(contact) for contact in candidates)
        )

        checked = len(responses)

        # Уже существующие копии.

        existing_contacts = {
            contact.node_id
            for contact, has_record, _ in responses
            if has_record
        }

        newer_contacts = {
            contact.node_id
            for contact, _, has_newer in responses
            if has_newer
        }


        existing = min(
            len(existing_contacts),
            self.replication_factor,
        )

        confirmed = existing


        for contact, has_record, has_newer in responses:
            if confirmed >= self.replication_factor:
                break

            if has_record or has_newer:
                continue


            try:
                stored = await self.store(
                    contact,
                    record,
                )

                if stored is True:
                    repaired += 1
                    confirmed += 1

            except Exception as exc:
                logger.warning(
                    "event=replica_repair_failed "
                    "peer_node_id=%s error=%s",
                    contact.node_id.hex(),
                    exc,
                )

        return MaintenanceResult(
            key=key,
            checked=checked,
            existing=existing,
            repaired=repaired,
            confirmed=confirmed,
            required=self.required_confirmations,
            successful=(
                confirmed >= self.required_confirmations
            ),
            fully_replicated=(
                confirmed >= self.replication_factor
            ),
        )
