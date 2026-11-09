
import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from p2p.dht.alias_records import AliasRecord
from p2p.protocol.contact import Contact


@dataclass(frozen=True, slots=True)
class AliasReplicationResult:
    attempted: int
    confirmed: int
    required: int
    replication_factor: int

    @property
    def successful(self) -> bool:
        return self.confirmed >= self.required

    @property
    def fully_replicated(self) -> bool:
        return self.confirmed >= self.replication_factor


async def replicate_alias(
    record: AliasRecord,
    contacts: list[Contact],
    store: Callable[[Contact, AliasRecord], Awaitable[bool]],
    replication_factor: int = 3,
    required_confirmations: int = 2,
) -> AliasReplicationResult:
    if replication_factor < 1:
        raise ValueError("Invalid replication factor")

    if not 1 <= required_confirmations <= replication_factor:
        raise ValueError("Invalid confirmation quorum")

    # Не отправляем запись одному NodeID несколько раз.
    unique = []
    seen = set()

    for contact in contacts:
        if contact.node_id in seen:
            continue
        seen.add(contact.node_id)
        unique.append(contact)

    confirmed = 0
    attempted = 0

    # Последовательно пробуем кандидатов.
    # При отказе одного узла используем следующий.
    for contact in unique:
        if confirmed >= replication_factor:
            break

        attempted += 1

        try:
            if await store(contact, record) is True:
                confirmed += 1
        except asyncio.CancelledError:
            raise
        except Exception:
            # Один недоступный узел не должен
            # останавливать всю репликацию.
            continue

    return AliasReplicationResult(
        attempted=attempted,
        confirmed=confirmed,
        required=required_confirmations,
        replication_factor=replication_factor,
    )
