
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
from p2p.protocol.messages import decode_pong, encode_ping
from p2p.protocol.payload_codec import PayloadError
from p2p.transport.connection import RpcError, send_request
from p2p.transport.server import TCPServer
from p2p.routing.routing_table import RoutingTable
import asyncio



logger = logging.getLogger(__name__)


class Node:
    """Основной объект P2P-узла."""

    def __init__(self, config: NodeConfig) -> None:
        self.config = config

        # Загружаем существующую идентичность или создаём новую.
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

        self.server = TCPServer(
            host=config.listen_host,
            port=config.listen_port,
            local_contact=self.local_contact,
            read_timeout=config.read_timeout_ms / 1000,
            protocol_version=config.protocol_version,
            max_frame_payload=config.max_frame_payload,
            local_node_label=self.local_node_label,
            on_ping=self._on_incoming_ping,
        )

    async def start(self) -> None:
        """Запускает узел и выполняет начальный bootstrap."""

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

    async def run(self) -> None:
        """Поддерживает работу TCP-сервера."""
        await self.server.serve_forever()

    async def stop(self) -> None:
        """Корректно останавливает узел."""

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


    def _on_incoming_ping(self, contact: Contact) -> None:
        """Планирует обратную проверку входящего узла."""

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
        """Проверяет доступность контакта обратным PING."""

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

        # Проверяем криптографическую структуру контакта.
        # Это пока не аутентификация удалённого узла.
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

    async def _bootstrap(self) -> None:
        """Проверяет доступность bootstrap-узлов."""

        if not self.config.bootstrap_peers:
            logger.info("No bootstrap peers configured")
            return

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

            except (OSError, RpcError) as exc:
                logger.warning(
                    "Bootstrap peer %s:%d unavailable: %s",
                    host,
                    port,
                    exc,
                )
