
import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
import logging
from p2p.protocol.contact import Contact, validate_contact
from p2p.routing.distance import validate_node_id, xor_distance

logger = logging.getLogger(__name__)

FindNodeCallback = Callable[
    [Contact, bytes],
    Awaitable[list[Contact]],
]

VerifyCallback = Callable[
    [Contact],
    Awaitable[bool],
]

@dataclass(slots=True)
class LookupTraceEvent:
    iteration: int
    peer_node_id: str
    status: str
    returned_contacts: list[str]
    duration_ms: float

@dataclass(slots=True)
class LookupResult:
    target_node_id: bytes
    closest_contacts: list[Contact]
    found: bool
    rpc_count: int
    iterations: int
    timeouts: int
    duration_ms: float
    trace: list[LookupTraceEvent] | None = None

    # Диагностика проверок обнаруженных контактов
    verification_count: int = 0
    verification_failures: int = 0
    verification_duration_ms: float = 0.0
    failed_contacts: list[str] | None = None


async def iterative_lookup(
    *,
    local_node_id: bytes,
    target_node_id: bytes,
    initial_contacts: list[Contact],
    k: int,
    alpha: int,
    find_node: FindNodeCallback,
    verify_contact: VerifyCallback,
    collect_trace: bool = False,
) -> LookupResult:
    """Итеративный поиск узла по XOR-расстоянию."""

    validate_node_id(local_node_id)
    validate_node_id(target_node_id)

    if k <= 0 or alpha <= 0:
        raise ValueError("k and alpha must be positive")

    started = time.perf_counter()

    known: dict[bytes, Contact] = {}
    queried: set[bytes] = set()
    failed: set[bytes] = set()
    verified: set[bytes] = set()

    rpc_count = 0
    iterations = 0
    timeouts = 0
    verification_count = 0
    verification_failures = 0
    verification_duration_ms = 0.0
    failed_contacts: list[str] = []

    trace_events: list[LookupTraceEvent] = []

    for contact in initial_contacts:
        validate_contact(contact)

        if contact.node_id != local_node_id:
            known[contact.node_id] = contact
            verified.add(contact.node_id)

    def nearest() -> list[Contact]:
        return sorted(
            (
                contact
                for node_id, contact in known.items()
                if node_id not in failed
            ),
            key=lambda contact: xor_distance(
                contact.node_id,
                target_node_id,
            ),
        )[:k]

    async def query_peer(
            contact: Contact,
    ) -> tuple[Contact, list[Contact] | None, bool]:

        request_started = time.perf_counter()

        try:
            contacts = await find_node(
                contact,
                target_node_id,
            )

            if collect_trace:
                trace_events.append(
                    LookupTraceEvent(
                        iteration=iterations,
                        peer_node_id=contact.node_id.hex(),
                        status="success",
                        returned_contacts=[
                            item.node_id.hex()
                            for item in contacts
                        ],
                        duration_ms=round(
                            (time.perf_counter() - request_started) * 1000,
                            3,
                        ),
                    )
                )

            return contact, contacts, False

        except (OSError, TimeoutError, ConnectionError):
            if collect_trace:
                trace_events.append(
                    LookupTraceEvent(
                        iteration=iterations,
                        peer_node_id=contact.node_id.hex(),
                        status="timeout_or_connection_error",
                        returned_contacts=[],
                        duration_ms=round(
                            (time.perf_counter() - request_started) * 1000,
                            3,
                        ),
                    )
                )

            return contact, None, True

        except Exception as exc:
            logger.warning(
                "event=find_node_failed peer_node_id=%s error=%s",
                contact.node_id.hex(),
                exc,
            )

            if collect_trace:
                trace_events.append(
                    LookupTraceEvent(
                        iteration=iterations,
                        peer_node_id=contact.node_id.hex(),
                        status="error",
                        returned_contacts=[],
                        duration_ms=round(
                            (time.perf_counter() - request_started) * 1000,
                            3,
                        ),
                    )
                )

            return contact, None, False

    while True:
        candidates = [
            contact
            for contact in nearest()
            if contact.node_id not in queried
        ]

        if not candidates:
            break

        batch = candidates[:alpha]
        iterations += 1

        for contact in batch:
            queried.add(contact.node_id)

        rpc_count += len(batch)

        responses = await asyncio.gather(
            *(query_peer(contact) for contact in batch)
        )

        discovered: dict[bytes, Contact] = {}

        for peer, contacts, timed_out in responses:
            if contacts is None:
                failed.add(peer.node_id)
                if timed_out:
                    timeouts += 1
                continue

            for contact in contacts:
                validate_contact(contact)

                if contact.node_id == local_node_id:
                    continue

                if contact.node_id not in known:
                    discovered[contact.node_id] = contact

        verification_semaphore = asyncio.Semaphore(alpha)

        async def verify_candidate(
                contact: Contact,
        ) -> tuple[Contact, bool, float]:

            async with verification_semaphore:
                verification_started = time.perf_counter()

                try:
                    is_verified = await verify_contact(contact)

                except Exception as exc:
                    logger.warning(
                        "event=contact_verification_failed "
                        "peer_node_id=%s error=%s",
                        contact.node_id.hex(),
                        exc,
                    )
                    is_verified = False

                duration_ms = (
                                      time.perf_counter() - verification_started
                              ) * 1000

                return contact, is_verified, duration_ms

        if discovered:
            checks = await asyncio.gather(
                *(
                    verify_candidate(contact)
                    for contact in discovered.values()
                )
            )

            for contact, is_verified, duration_ms in checks:
                verification_count += 1
                verification_duration_ms += duration_ms

                if is_verified:
                    known[contact.node_id] = contact
                    verified.add(contact.node_id)

                else:
                    verification_failures += 1
                    failed_contacts.append(contact.node_id.hex())
                    failed.add(contact.node_id)

    closest = [
        contact
        for contact in nearest()
        if contact.node_id in verified
    ]

    return LookupResult(
        target_node_id=target_node_id,
        closest_contacts=closest,
        found=any(
            contact.node_id == target_node_id
            for contact in closest
        ),
        rpc_count=rpc_count,
        iterations=iterations,
        timeouts=timeouts,
        duration_ms=(
            time.perf_counter() - started
        ) * 1000,
        trace=trace_events if collect_trace else None,
        verification_count=verification_count,
        verification_failures=verification_failures,
        verification_duration_ms=round(
            verification_duration_ms, 3
        ),
        failed_contacts=failed_contacts,
    )
