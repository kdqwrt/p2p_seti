
import logging
import secrets
import time

from p2p.config import NodeConfig
from p2p.identity.identity import (
    IDENTITY_ALGORITHM,
    load_or_create_identity,
)
from p2p.protocol.constants import MessageType
from p2p.protocol.contact import Contact, validate_contact
from p2p.protocol.frame import Frame
from p2p.protocol.messages import (
    decode_pong,
    encode_ping,
    encode_find_node_request,
    decode_find_node_response,
)
from p2p.protocol.payload_codec import PayloadError
from p2p.transport.connection import RpcError, send_request
from p2p.transport.server import TCPServer
from p2p.routing.routing_table import RoutingTable
from p2p.routing.lookup import LookupResult, iterative_lookup
from p2p.dht.storage import DHTStorage
from p2p.dht.records import (
    NodeRecord,
    create_node_record,
    node_record_key,
    validate_node_record,
    MAX_TTL_SECONDS,
    UINT64_MAX,
)
from p2p.dht.replication import ReplicationResult, replicate_record
from p2p.dht.messages import (
    encode_store_request,
    decode_store_response,
    encode_find_value_request,
    decode_find_value_response,
)
from p2p.dht.published_registry import PublishedRecordRegistry
from p2p.dht.maintenance import ReplicaMaintenance
import asyncio

from p2p.dht.alias_storage import AliasStorage
from p2p.dht.alias_records import (
    AliasRecord,
    alias_record_key,
    validate_alias_record,
)
from p2p.dht.alias_messages import (
    encode_store_alias_request,
    decode_store_alias_response,
    encode_find_alias_request,
    decode_find_alias_response,
)

logger = logging.getLogger(__name__)


class Node:


    def __init__(self, config: NodeConfig) -> None:
        self.config = config


        self.identity = load_or_create_identity(
            config.node_state_dir
        )

        self.local_node_label = self.identity.node_id.hex()[:12]

        self.local_contact = Contact(
            node_id=self.identity.node_id,
            identity_algorithm=IDENTITY_ALGORITHM,
            identity_public_key=self.identity.public_key,
            host=config.listen_host,
            port=config.listen_port,
        )

        validate_contact(self.local_contact)
        self.routing_table = RoutingTable(
            local_node_id=self.identity.node_id,
            k_bucket_size=config.k_bucket_size,
        )

        self._outbound_peers: set[tuple[str, int]] = set()
        self._pending_verifications: set[bytes] = set()
        self._verification_tasks: set[asyncio.Task] = set()

        self.dht_storage = DHTStorage(config.node_state_dir)
        self.alias_storage = AliasStorage(config.node_state_dir)
        self.published_registry = PublishedRecordRegistry(
            config.node_state_dir
        )


        self.replica_maintenance = ReplicaMaintenance(
            find_value=self.find_value,
            store=self.store_record,
            replication_factor=3,
            required_confirmations=2,
        )


        self._maintenance_task: asyncio.Task | None = None
        self._maintenance_interval = 5.0


        self._maintenance_lock = asyncio.Lock()
        self._refresh_lock = asyncio.Lock()


        self.server = TCPServer(
            host=config.listen_host,
            port=config.listen_port,
            local_contact=self.local_contact,
            read_timeout=config.read_timeout_ms / 1000,
            protocol_version=config.protocol_version,
            max_frame_payload=config.max_frame_payload,
            local_node_label=self.local_node_label,
            on_ping=self._on_incoming_ping,
            on_find_node=self._find_closest_contacts,
            on_store=self.dht_storage.put,
            on_find_value=self.dht_storage.get,
            on_store_alias=self.alias_storage.put,
            on_find_alias=self.alias_storage.get_all,
        )



    async def start(self) -> None:

        await self.dht_storage.load()
        await self.alias_storage.load()
        await self.published_registry.load()
        await self.server.start()


        logger.info(
            "local_node=%s direction=LOCAL connection_id=- "
            "rpc_id=- event=node_started listen=%s:%d",
            self.local_node_label,
            self.config.listen_host,
            self.config.listen_port,
        )

        logger.info(
            "State directory: %s",
            self.config.node_state_dir,
        )

        logger.info(
            "Identity algorithm=%s NodeID=%s",
            IDENTITY_ALGORITHM,
            self.identity.node_id.hex(),
        )

        logger.info(
            "Protocol version=%d max_payload=%d K=%d alpha=%d",
            self.config.protocol_version,
            self.config.max_frame_payload,
            self.config.k_bucket_size,
            self.config.alpha,
        )

        await self._bootstrap()


        if self._maintenance_task is None or self._maintenance_task.done():
            self._maintenance_task = asyncio.create_task(
                self._maintenance_loop(),
                name=f"maintenance-{self.local_node_label}",
            )

    async def run(self) -> None:

        await self.server.serve_forever()

    async def stop(self) -> None:

        if self._maintenance_task is not None:
            self._maintenance_task.cancel()

            await asyncio.gather(
                self._maintenance_task,
                return_exceptions=True,
            )

            self._maintenance_task = None

        for task in self._verification_tasks:
            task.cancel()

        if self._verification_tasks:
            await asyncio.gather(
                *self._verification_tasks,
                return_exceptions=True,
            )

        await self.server.stop()

        logger.info(
            "local_node=%s direction=LOCAL connection_id=- "
            "rpc_id=- event=node_stopped",
            self.local_node_label,
        )



    def _find_closest_contacts(
        self,
        target_node_id: bytes,
    ) -> list[Contact]:


        contacts = self.routing_table.closest(
            target_node_id,
            limit=self.config.k_bucket_size,
        )

        logger.info(
            "local_node=%s event=find_node_request "
            "target_node_id=%s contacts_count=%d",
            self.local_node_label,
            target_node_id.hex(),
            len(contacts),
        )

        return contacts


    def _on_incoming_ping(self, contact: Contact) -> None:


        if contact.node_id == self.identity.node_id:
            return

        if self.routing_table.get_contact(contact.node_id) is not None:
            return

        if contact.node_id in self._pending_verifications:
            return

        self._pending_verifications.add(contact.node_id)

        task = asyncio.create_task(
            self._verify_incoming_contact(contact)
        )

        self._verification_tasks.add(task)
        task.add_done_callback(self._verification_tasks.discard)

    async def _verify_incoming_contact(
        self,
        contact: Contact,
    ) -> None:


        try:
            await self.ping(
                host=contact.host,
                port=contact.port,
                expected_node_id=contact.node_id,
            )

            logger.info(
                "local_node=%s event=incoming_peer_verified "
                "peer_node_id=%s routing_size=%d",
                self.local_node_label,
                contact.node_id.hex(),
                self.routing_table.size(),
            )

        except (OSError, RpcError) as exc:
            logger.warning(
                "local_node=%s event=incoming_peer_rejected "
                "peer_node_id=%s error=%s",
                self.local_node_label,
                contact.node_id.hex(),
                exc,
            )

        finally:
            self._pending_verifications.discard(contact.node_id)



    async def ping(
            self,
            host: str,
            port: int,
            *,
            update_routing: bool = True,
            expected_node_id: bytes | None = None,
    ) -> float:

        request_id = secrets.token_bytes(16)
        timestamp_ms = int(time.time() * 1000)

        payload = encode_ping(
            sender=self.local_contact,
            timestamp_ms=timestamp_ms,
        )

        request = Frame(
            version=self.config.protocol_version,
            message_type=MessageType.PING,
            flags=0,
            request_id=request_id,
            payload=payload,
        )

        started = time.perf_counter()

        self._outbound_peers.add((host, port))

        response = await send_request(
            host=host,
            port=port,
            frame=request,
            connect_timeout=self.config.connect_timeout,
            response_timeout=self.config.ping_timeout,
            protocol_version=self.config.protocol_version,
            max_frame_payload=self.config.max_frame_payload,
            local_node_label=self.local_node_label,
        )

        elapsed_ms = (time.perf_counter() - started) * 1000

        if response.message_type != MessageType.PONG:
            raise RpcError(
                "Expected PONG, "
                f"received {response.message_type.name}"
            )

        try:
            pong = decode_pong(response.payload)
        except PayloadError as exc:
            raise RpcError(
                f"Invalid PONG payload: {exc}"
            ) from exc

        if pong["ping_timestamp_ms"] != timestamp_ms:
            raise RpcError(
                "PONG contains unexpected ping_timestamp_ms"
            )


        try:
            remote_contact = pong["responder"]
            validate_contact(remote_contact)
        except (PayloadError, KeyError, TypeError) as exc:
            raise RpcError(
                f"Invalid responder contact: {exc}"
            ) from exc

        if remote_contact.identity_algorithm != IDENTITY_ALGORITHM:
            raise RpcError("Responder must use Ed25519")

        if remote_contact.host != host or remote_contact.port != port:
            raise RpcError("PONG responder endpoint mismatch")

        if remote_contact.node_id == self.identity.node_id:
            raise RpcError("Cannot add local node as remote peer")

        if (
                expected_node_id is not None
                and remote_contact.node_id != expected_node_id
        ):
            raise RpcError("Unexpected responder NodeID")

        if update_routing:

            async def ping_old_contact(contact: Contact) -> bool:
                try:
                    await self.ping(
                        host=contact.host,
                        port=contact.port,
                        update_routing=False,
                        expected_node_id=contact.node_id,
                    )
                    return True
                except (OSError, RpcError):
                    return False

            added = await self.routing_table.add_contact(
                remote_contact,
                ping_old_contact,
                verified=True,
            )

            logger.info(
                "local_node=%s event=routing_contact_observed "
                "peer_node_id=%s added=%s routing_size=%d",
                self.local_node_label,
                remote_contact.node_id.hex(),
                added,
                self.routing_table.size(),
            )




        logger.info(
            "local_node=%s direction=RPC connection_id=- "
            "rpc_id=%s event=ping_success peer=%s:%d rtt_ms=%.2f",
            self.local_node_label,
            request_id.hex(),
            host,
            port,
            elapsed_ms,
        )

        return elapsed_ms



    async def find_node(
        self,
        contact: Contact,
        target_node_id: bytes,
    ) -> list[Contact]:


        validate_contact(contact)

        if not isinstance(target_node_id, bytes) or len(target_node_id) != 32:
            raise ValueError("target_node_id must be 32 bytes")

        request_id = secrets.token_bytes(16)

        request = Frame(
            version=self.config.protocol_version,
            message_type=MessageType.FIND_NODE_REQUEST,
            flags=0,
            request_id=request_id,
            payload=encode_find_node_request(
                sender=self.local_contact,
                target_node_id=target_node_id,
            ),
        )

        response = await send_request(
            host=contact.host,
            port=contact.port,
            frame=request,
            connect_timeout=self.config.connect_timeout,
            response_timeout=self.config.ping_timeout,
            protocol_version=self.config.protocol_version,
            max_frame_payload=self.config.max_frame_payload,
            local_node_label=self.local_node_label,
        )

        if response.request_id != request_id:
            raise RpcError("FIND_NODE request_id mismatch")

        if response.message_type != MessageType.FIND_NODE_RESPONSE:
            raise RpcError(
                f"Expected FIND_NODE_RESPONSE, "
                f"received {response.message_type.name}"
            )

        try:
            result = decode_find_node_response(response.payload)

            responder = result["responder"]
            validate_contact(responder)

            if responder.node_id != contact.node_id:
                raise RpcError("Unexpected responder NodeID")

            if (
                responder.host != contact.host
                or responder.port != contact.port
            ):
                raise RpcError("Responder endpoint mismatch")

            if result["target_node_id"] != target_node_id:
                raise RpcError("FIND_NODE target mismatch")

            contacts = result["contacts"]

            if len(contacts) > self.config.k_bucket_size:
                raise RpcError("Too many contacts in FIND_NODE response")

            seen = set()
            previous_distance = -1

            for candidate in contacts:
                validate_contact(candidate)

                if candidate.node_id == self.identity.node_id:
                    continue

                if candidate.node_id in seen:
                    raise RpcError("Duplicate contact in response")

                seen.add(candidate.node_id)

                distance = int.from_bytes(
                    bytes(
                        a ^ b
                        for a, b in zip(
                            candidate.node_id,
                            target_node_id,
                        )
                    ),
                    "big",
                )

                if distance < previous_distance:
                    raise RpcError("Contacts are not XOR-sorted")

                previous_distance = distance

        except (PayloadError, ValueError, TypeError, KeyError) as exc:
            raise RpcError(
                f"Invalid FIND_NODE response: {exc}"
            ) from exc

        logger.info(
            "local_node=%s event=find_node_success "
            "peer_node_id=%s target_node_id=%s contacts_count=%d",
            self.local_node_label,
            contact.node_id.hex(),
            target_node_id.hex(),
            len(contacts),
        )

        contacts = [
            candidate
            for candidate in contacts
            if candidate.node_id != self.identity.node_id
        ]

        return contacts



    async def store_record(
        self,
        contact: Contact,
        record: NodeRecord,
    ) -> bool:


        validate_contact(contact)
        validate_node_record(record)

        key = node_record_key(record.node_id)
        request_id = secrets.token_bytes(16)

        request = Frame(
            version=self.config.protocol_version,
            message_type=MessageType.STORE_REQUEST,
            flags=0,
            request_id=request_id,
            payload=encode_store_request(key, record),
        )

        response = await send_request(
            host=contact.host,
            port=contact.port,
            frame=request,
            connect_timeout=self.config.connect_timeout,
            response_timeout=self.config.ping_timeout,
            protocol_version=self.config.protocol_version,
            max_frame_payload=self.config.max_frame_payload,
            local_node_label=self.local_node_label,
        )

        if response.request_id != request_id:
            raise RpcError("STORE request_id mismatch")

        if response.message_type != MessageType.STORE_RESPONSE:
            raise RpcError(
                f"Expected STORE_RESPONSE, "
                f"received {response.message_type.name}"
            )

        try:
            result = decode_store_response(response.payload)
        except PayloadError as exc:
            raise RpcError(
                f"Invalid STORE response: {exc}"
            ) from exc

        if result["key"] != key:
            raise RpcError("STORE response key mismatch")

        if not result["stored"]:
            raise RpcError("Remote node did not store the record")

        logger.info(
            "local_node=%s event=dht_store_success "
            "peer_node_id=%s key=%s",
            self.local_node_label,
            contact.node_id.hex(),
            key.hex(),
        )

        return True


    async def find_value(
        self,
        contact: Contact,
        key: bytes,
    ) -> NodeRecord | None:


        validate_contact(contact)

        if not isinstance(key, bytes) or len(key) != 32:
            raise ValueError("DHT key must be exactly 32 bytes")

        request_id = secrets.token_bytes(16)

        request = Frame(
            version=self.config.protocol_version,
            message_type=MessageType.FIND_VALUE_REQUEST,
            flags=0,
            request_id=request_id,
            payload=encode_find_value_request(key),
        )

        response = await send_request(
            host=contact.host,
            port=contact.port,
            frame=request,
            connect_timeout=self.config.connect_timeout,
            response_timeout=self.config.ping_timeout,
            protocol_version=self.config.protocol_version,
            max_frame_payload=self.config.max_frame_payload,
            local_node_label=self.local_node_label,
        )

        if response.request_id != request_id:
            raise RpcError("FIND_VALUE request_id mismatch")

        if response.message_type != MessageType.FIND_VALUE_RESPONSE:
            raise RpcError(
                f"Expected FIND_VALUE_RESPONSE, "
                f"received {response.message_type.name}"
            )

        try:
            result = decode_find_value_response(response.payload)
        except PayloadError as exc:
            raise RpcError(
                f"Invalid FIND_VALUE response: {exc}"
            ) from exc

        if result["key"] != key:
            raise RpcError("FIND_VALUE response key mismatch")

        record = result["record"]

        if record is not None:
            if node_record_key(record.node_id) != key:
                raise RpcError("FIND_VALUE returned incorrect record key")

        logger.info(
            "local_node=%s event=dht_find_value "
            "peer_node_id=%s key=%s found=%s",
            self.local_node_label,
            contact.node_id.hex(),
            key.hex(),
            record is not None,
        )

        return record



    async def publish_record(
        self,
        record: NodeRecord,
    ) -> ReplicationResult:


        validate_node_record(record)

        key = node_record_key(record.node_id)

        replication_factor = 3
        required_confirmations = 2


        lookup = await self.lookup_node(
            key,
            candidate_limit=max(
                self.config.k_bucket_size,
                replication_factor + 2,
            ),
        )

        result = await replicate_record(
            record=record,
            contacts=lookup.closest_contacts,
            store=self.store_record,
            replication_factor=replication_factor,
            required_confirmations=required_confirmations,
        )

        if result.successful:
            registered = await self.published_registry.add(record)

            if not registered:
                logger.warning(
                    "local_node=%s event=published_record_not_registered "
                    "key=%s sequence_number=%d",
                    self.local_node_label,
                    key.hex(),
                    record.sequence_number,
                )

        logger.info(
            "local_node=%s event=dht_replication "
            "key=%s attempted=%d confirmed=%d "
            "required=%d successful=%s "
            "fully_replicated=%s",
            self.local_node_label,
            key.hex(),
            result.attempted,
            result.confirmed,
            result.required,
            result.successful,
            result.fully_replicated,
        )

        return result




    async def lookup_node(
        self,
        target_node_id: bytes,
        *,
        collect_trace: bool = False,
        candidate_limit: int | None = None,
    ) -> LookupResult:
        if candidate_limit is None:
            candidate_limit = self.config.k_bucket_size

        if candidate_limit <= 0:
            raise ValueError("candidate_limit must be positive")

        initial_contacts = self.routing_table.closest(
            target_node_id,
            limit=candidate_limit,
        )

        async def find_node_callback(
            contact: Contact,
            target: bytes,
        ) -> list[Contact]:
            return await self.find_node(
                contact=contact,
                target_node_id=target,
            )

        async def verify_callback(contact: Contact) -> bool:
            try:
                await self.ping(
                    host=contact.host,
                    port=contact.port,
                    expected_node_id=contact.node_id,
                )
                return True

            except (OSError, RpcError):
                return False

        result = await iterative_lookup(
            local_node_id=self.identity.node_id,
            target_node_id=target_node_id,
            initial_contacts=initial_contacts,
            k=candidate_limit,
            alpha=self.config.alpha,
            find_node=find_node_callback,
            verify_contact=verify_callback,
            collect_trace=collect_trace,
        )

        logger.info(
            "local_node=%s event=lookup_completed "
            "target_node_id=%s found=%s "
            "rpc_count=%d iterations=%d "
            "timeouts=%d duration_ms=%.2f",
            self.local_node_label,
            target_node_id.hex(),
            result.found,
            result.rpc_count,
            result.iterations,
            result.timeouts,
            result.duration_ms,
        )

        return result

    async def refresh_published_record(
            self,
            record: NodeRecord,
    ) -> bool:
        if record.node_id != self.identity.node_id:
            raise ValueError("Cannot refresh a foreign record")

        async with self._refresh_lock:
            key = node_record_key(record.node_id)

            current = await self.published_registry.get_stored(
                key
            )

            if current != record:
                return False

            now = int(time.time())
            ttl = record.expires_at - record.issued_at

            pending = await self.published_registry.get_pending(
                key
            )

            if pending is not None:
                if (
                        pending.node_id != self.identity.node_id
                        or pending.identity_public_key
                        != self.identity.public_key
                        or pending.sequence_number
                        <= record.sequence_number
                ):
                    logger.error(
                        "event=invalid_pending_refresh key=%s",
                        key.hex(),
                    )
                    return False

                if pending.expires_at > now:
                    # Пока pending действительна,
                    # повторяем те же подписанные байты.
                    new_record = pending

                else:
                    # Просроченную pending нельзя отправлять.
                    # Её sequence_number уже мог попасть
                    # на одну или несколько реплик.
                    if pending.sequence_number >= UINT64_MAX:
                        return False

                    new_record = create_node_record(
                        identity=self.identity,
                        addresses=list(record.addresses),
                        sequence_number=(
                                pending.sequence_number + 1
                        ),
                        ttl_seconds=min(
                            ttl, MAX_TTL_SECONDS
                        ),
                    )

                    await self.published_registry.save_pending(
                        new_record
                    )

            else:
                if now < record.issued_at + ttl / 2:
                    return False

                if record.sequence_number >= UINT64_MAX:
                    return False

                new_record = create_node_record(
                    identity=self.identity,
                    addresses=list(record.addresses),
                    sequence_number=record.sequence_number + 1,
                    ttl_seconds=min(
                        ttl, MAX_TTL_SECONDS
                    ),
                )

                await self.published_registry.save_pending(
                    new_record
                )

            result = await self.publish_record(new_record)

            if not result.successful:
                logger.warning(
                    "event=refresh_pending "
                    "key=%s sequence=%d confirmed=%d",
                    key.hex(),
                    new_record.sequence_number,
                    result.confirmed,
                )
                return False

            registered = await self.published_registry.get(key)

            if registered != new_record:
                logger.error(
                    "event=refresh_registry_mismatch key=%s",
                    key.hex(),
                )
                return False

            await self.published_registry.clear_pending(key)

            logger.info(
                "event=record_refreshed "
                "key=%s old_sequence=%d new_sequence=%d",
                key.hex(),
                record.sequence_number,
                new_record.sequence_number,
            )

            return True

    async def maintain_published_records(self) -> None:


        async with self._maintenance_lock:
            await self.published_registry.cleanup()

            records = await self.published_registry.list_active()

            stored_records = await self.published_registry.list_stored()

            for stored_record in stored_records:
                if (
                        stored_record.expires_at <= int(time.time())
                        and stored_record not in records
                ):
                    records.append(stored_record)

            for record in records:

                if record.node_id != self.identity.node_id:
                    logger.warning(
                        "local_node=%s event=maintenance_skipped_foreign_record "
                        "record_node_id=%s",
                        self.local_node_label,
                        record.node_id.hex(),
                    )
                    continue

                key = node_record_key(record.node_id)

                try:

                    self.published_registry._validate_saved_record(record)


                    refreshed = await self.refresh_published_record(record)

                    if refreshed:
                        logger.info(
                            "event=maintenance_record_refreshed key=%s",
                            key.hex(),
                        )
                        continue

                    if record.expires_at <= int(time.time()):
                        continue


                    lookup = await self.lookup_node(
                        key,
                        candidate_limit=max(
                            self.config.k_bucket_size,
                            5,
                        ),
                    )


                    result = await self.replica_maintenance.maintain(
                        record,
                        lookup.closest_contacts,
                    )

                    logger.info(
                        "local_node=%s event=replica_maintenance "
                        "key=%s checked=%d existing=%d repaired=%d "
                        "confirmed=%d successful=%s fully_replicated=%s",
                        self.local_node_label,
                        key.hex(),
                        result.checked,
                        result.existing,
                        result.repaired,
                        result.confirmed,
                        result.successful,
                        result.fully_replicated,
                    )

                except asyncio.CancelledError:
                    raise

                except ValueError as exc:
                    logger.warning(
                        "local_node=%s event=maintenance_invalid_record "
                        "key=%s error=%s",
                        self.local_node_label,
                        key.hex(),
                        exc,
                    )

                except Exception:
                    logger.exception(
                        "local_node=%s event=replica_maintenance_error "
                        "key=%s",
                        self.local_node_label,
                        key.hex(),
                    )

    async def _maintenance_loop(self) -> None:


        try:
            while True:

                await asyncio.sleep(
                    self._maintenance_interval
                )

                try:
                    await self.maintain_published_records()

                except asyncio.CancelledError:
                    raise

                except Exception:
                    logger.exception(
                        "local_node=%s event=maintenance_cycle_error",
                        self.local_node_label,
                    )

        except asyncio.CancelledError:
            logger.info(
                "local_node=%s event=maintenance_stopped",
                self.local_node_label,
            )
            raise



    async def _bootstrap(self) -> None:

        if not self.config.bootstrap_peers:
            logger.info("No bootstrap peers configured")
            return

        successful_peers = 0

        for peer in self.config.bootstrap_peers:
            host = peer["host"]
            port = peer["port"]

            logger.info(
                "Bootstrap peer %s:%d",
                host,
                port,
            )

            try:
                await self.ping(
                    host=host,
                    port=port,
                )
                successful_peers += 1

            except (OSError, RpcError) as exc:
                logger.warning(
                    "Bootstrap peer %s:%d unavailable: %s",
                    host,
                    port,
                    exc,
                )

        if successful_peers == 0:
            logger.warning(
                "local_node=%s event=bootstrap_failed "
                "reason=no_available_peers",
                self.local_node_label,
            )
            return

        logger.info(
            "local_node=%s event=bootstrap_lookup_started "
            "successful_peers=%d",
            self.local_node_label,
            successful_peers,
        )

        result = await self.lookup_node(
            self.identity.node_id
        )

        logger.info(
            "local_node=%s event=bootstrap_lookup_completed "
            "contacts_count=%d rpc_count=%d "
            "iterations=%d duration_ms=%.2f",
            self.local_node_label,
            len(result.closest_contacts),
            result.rpc_count,
            result.iterations,
            result.duration_ms,
        )


    async def store_alias(
        self,
        contact: Contact,
        record: AliasRecord,
    ) -> bool:
        """Отправляет подписанный псевдоним другому узлу."""
        validate_contact(contact)
        validate_alias_record(record)

        key = alias_record_key(record.alias)
        request_id = secrets.token_bytes(16)

        request = Frame(
            version=self.config.protocol_version,
            message_type=MessageType.STORE_ALIAS_REQUEST,
            flags=0,
            request_id=request_id,
            payload=encode_store_alias_request(record),
        )

        response = await send_request(
            host=contact.host,
            port=contact.port,
            frame=request,
            connect_timeout=self.config.connect_timeout,
            response_timeout=self.config.ping_timeout,
            protocol_version=self.config.protocol_version,
            max_frame_payload=self.config.max_frame_payload,
            local_node_label=self.local_node_label,
        )

        if response.request_id != request_id:
            raise RpcError("STORE_ALIAS request_id mismatch")

        if response.message_type != MessageType.STORE_ALIAS_RESPONSE:
            raise RpcError(
                "Expected STORE_ALIAS_RESPONSE, "
                f"received {response.message_type.name}"
            )

        try:
            result = decode_store_alias_response(response.payload)
        except PayloadError as exc:
            raise RpcError(
                f"Invalid STORE_ALIAS response: {exc}"
            ) from exc

        if result["key"] != key:
            raise RpcError("STORE_ALIAS key mismatch")

        if not result["stored"]:
            raise RpcError("Remote node rejected alias")

        logger.info(
            "event=alias_store_success peer=%s key=%s",
            contact.node_id.hex(),
            key.hex(),
        )
        return True

    async def find_alias(
        self,
        contact: Contact,
        alias: str,
    ) -> list[AliasRecord]:
        """Получает подписанные записи псевдонима от узла."""
        validate_contact(contact)

        key = alias_record_key(alias)
        request_id = secrets.token_bytes(16)

        request = Frame(
            version=self.config.protocol_version,
            message_type=MessageType.FIND_ALIAS_REQUEST,
            flags=0,
            request_id=request_id,
            payload=encode_find_alias_request(alias),
        )

        response = await send_request(
            host=contact.host,
            port=contact.port,
            frame=request,
            connect_timeout=self.config.connect_timeout,
            response_timeout=self.config.ping_timeout,
            protocol_version=self.config.protocol_version,
            max_frame_payload=self.config.max_frame_payload,
            local_node_label=self.local_node_label,
        )

        if response.request_id != request_id:
            raise RpcError("FIND_ALIAS request_id mismatch")

        if response.message_type != MessageType.FIND_ALIAS_RESPONSE:
            raise RpcError(
                "Expected FIND_ALIAS_RESPONSE, "
                f"received {response.message_type.name}"
            )

        try:
            result = decode_find_alias_response(response.payload)
        except PayloadError as exc:
            raise RpcError(
                f"Invalid FIND_ALIAS response: {exc}"
            ) from exc

        if result["key"] != key or result["alias"] != alias:
            raise RpcError("FIND_ALIAS returned unexpected alias")

        logger.info(
            "event=alias_find_success peer=%s alias=%s count=%d",
            contact.node_id.hex(),
            alias,
            len(result["records"]),
        )

        return result["records"]


