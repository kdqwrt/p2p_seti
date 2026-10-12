import hashlib
import logging
import secrets
import time
from pathlib import Path
from p2p.protocol.messages import (
    decode_pong,
    encode_ping,
)

from p2p.config import NodeConfig
from p2p.protocol.constants import MessageType
from p2p.protocol.frame import Frame
from p2p.transport.connection import RpcError, send_request
from p2p.transport.server import TCPServer
from p2p.protocol.payload_codec import PayloadError
from p2p.protocol.contact import Contact

logger = logging.getLogger(__name__)


class Node:
    """Основной объект P2P-узла."""

    def __init__(self, config: NodeConfig) -> None:
        self.config = config

        # Временная диагностическая метка Этапа 1. Это не криптографический NodeID.
        endpoint = f"{config.listen_host}:{config.listen_port}".encode("utf-8")
        self.local_node_label = "stage1-" + hashlib.sha256(endpoint).hexdigest()[:12]

        self.local_contact = Contact(
            node_id=b"",
            identity_algorithm="stage1-none",
            identity_public_key=b"",
            host=config.listen_host,
            port=config.listen_port,
        )

        self.server = TCPServer(
            host=config.listen_host,
            port=config.listen_port,
            local_contact=self.local_contact,
            read_timeout=config.read_timeout_ms / 1000,
            protocol_version=config.protocol_version,
            max_frame_payload=config.max_frame_payload,
            local_node_label=self.local_node_label,
        )

    async def start(self) -> None:
        """Запускает узел и выполняет начальный bootstrap."""

        Path(self.config.node_state_dir).mkdir(
            parents=True,
            exist_ok=True,
        )

        await self.server.start()

        logger.info(
            "local_node=%s direction=LOCAL connection_id=- rpc_id=- event=node_started listen=%s:%d",
            self.local_node_label,
            self.config.listen_host,
            self.config.listen_port,
        )

        logger.info(
            "State directory: %s",
            self.config.node_state_dir,
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

        await self.server.stop()

        logger.info(
            "local_node=%s direction=LOCAL connection_id=- rpc_id=- event=node_stopped",
            self.local_node_label,
        )

    async def ping(
        self,
        host: str,
        port: int,
    ) -> float:
        """
        Отправляет PING другому узлу.

        Возвращает RTT в миллисекундах.
        """

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

        elapsed_ms = (
            time.perf_counter() - started
        ) * 1000

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

        logger.info(
            "local_node=%s direction=RPC connection_id=- rpc_id=%s event=ping_success peer=%s:%d rtt_ms=%.2f",
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
            logger.info(
                "No bootstrap peers configured"
            )
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