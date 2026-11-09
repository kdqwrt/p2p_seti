
import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from p2p.dht.records import NodeRecord, node_record_key
from p2p.protocol.contact import Contact, validate_contact
from p2p.routing.distance import xor_distance

logger = logging.getLogger(__name__)

StoreCallback = Callable[
    [Contact, NodeRecord],
    Awaitable[bool],
]


@dataclass(slots=True)
class ReplicationResult:
    key: bytes
    attempted: int
    confirmed: int
    required: int
    successful: bool
    confirmed_nodes: list[bytes]
    failed_nodes: list[bytes]

    @property
    def fully_replicated(self) -> bool:
        """Все три копии созданы, если R=3."""
        return self.confirmed >= 3


async def replicate_record(
    *,
    record: NodeRecord,
    contacts: list[Contact],
    store: StoreCallback,
    replication_factor: int = 3,
    required_confirmations: int = 2,
) -> ReplicationResult:
    """
    Сохраняет запись на ближайших узлах.

    Сначала отправляет STORE первым R контактам.
    Если подтверждений меньше R, пробует резервные.
    """

    if replication_factor <= 0:
        raise ValueError("replication_factor must be positive")

    if not 1 <= required_confirmations <= replication_factor:
        raise ValueError("Invalid required_confirmations")

    key = node_record_key(record.node_id)

    unique: dict[bytes, Contact] = {}

    for contact in contacts:
        validate_contact(contact)
        unique[contact.node_id] = contact

    candidates = sorted(
        unique.values(),
        key=lambda contact: xor_distance(
            contact.node_id,
            key,
        ),
    )

    confirmed_nodes: list[bytes] = []
    failed_nodes: list[bytes] = []
    attempted = 0

    async def store_one(
        contact: Contact,
    ) -> tuple[bytes, bool]:
        try:
            result = await store(contact, record)
            return contact.node_id, result is True

        except Exception as exc:
            logger.warning(
                "event=replica_store_failed "
                "peer_node_id=%s error=%s",
                contact.node_id.hex(),
                exc,
            )
            return contact.node_id, False

    # Обрабатываем контакты партиями.
    # Первая партия — R узлов.
    # Последующие — только для недостающих копий.
    offset = 0

    while (
        len(confirmed_nodes) < replication_factor
        and offset < len(candidates)
    ):
        missing = replication_factor - len(confirmed_nodes)

        batch = candidates[offset:offset + missing]
        offset += len(batch)

        attempted += len(batch)

        responses = await asyncio.gather(
            *(store_one(contact) for contact in batch)
        )

        for node_id, confirmed in responses:
            if confirmed:
                confirmed_nodes.append(node_id)
            else:
                failed_nodes.append(node_id)

    return ReplicationResult(
        key=key,
        attempted=attempted,
        confirmed=len(confirmed_nodes),
        required=required_confirmations,
        successful=(
            len(confirmed_nodes) >= required_confirmations
        ),
        confirmed_nodes=confirmed_nodes,
        failed_nodes=failed_nodes,
    )
