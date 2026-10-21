import asyncio
import logging
import secrets

from p2p.protocol.codec import ProtocolError, encode_frame
from p2p.protocol.contact import Contact
from p2p.protocol.constants import MessageType
from p2p.protocol.frame import Frame
from p2p.protocol.messages import encode_error
from p2p.protocol.payload_codec import PayloadError
from p2p.rpc.dispatcher import Dispatcher
from p2p.transport.stream_decoder import StreamDecoder
from collections.abc import Callable



logger = logging.getLogger(__name__)


class TCPServer:
    def __init__(
        self,
        host: str,
        port: int,
        local_contact: Contact,
        read_timeout: float,
        protocol_version: int,
        max_frame_payload: int,
        local_node_label: str = "stage1-unknown",
        on_ping: Callable[[Contact], None] | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.read_timeout = read_timeout
        self.protocol_version = protocol_version
        self.max_frame_payload = max_frame_payload
        self.local_node_label = local_node_label
        self._server: asyncio.Server | None = None
        self._dispatcher = Dispatcher(
            local_contact,
            on_ping=on_ping,
        )

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._handle_connection,
            self.host,
            self.port,
        )
        logger.info(
            "local_node=%s direction=LOCAL connection_id=- rpc_id=- event=server_started listen=%s:%s",
            self.local_node_label, self.host, self.port,
        )

    async def serve_forever(self) -> None:
        if self._server is None:
            raise RuntimeError("Server is not started")
        async with self._server:
            await self._server.serve_forever()

    async def stop(self) -> None:
        if self._server is None:
            return
        self._server.close()
        await self._server.wait_closed()
        logger.info(
            "local_node=%s direction=LOCAL connection_id=- rpc_id=- event=server_stopped listen=%s:%s",
            self.local_node_label, self.host, self.port,
        )

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        peer = writer.get_extra_info("peername")
        connection_id = secrets.token_hex(8)

        logger.info(
            "local_node=%s direction=IN connection_id=%s rpc_id=- event=connection_opened peer=%s",
            self.local_node_label, connection_id, peer,
        )

        decoder = StreamDecoder(
            expected_version=self.protocol_version,
            max_payload=self.max_frame_payload,
        )
        close_reason = "normal"

        frame_deadline: float | None = None

        try:
            while True:
                try:
                    loop = asyncio.get_running_loop()

                    if frame_deadline is None:
                        timeout = self.read_timeout
                    else:
                        timeout = frame_deadline - loop.time()

                        if timeout <= 0:
                            raise TimeoutError

                    data = await asyncio.wait_for(
                        reader.read(4096),
                        timeout=timeout,
                    )

                except TimeoutError:
                    close_reason = "read_timeout"

                    logger.warning(
                        "local_node=%s direction=IN connection_id=%s rpc_id=- event=read_timeout peer=%s",
                        self.local_node_label, connection_id, peer,
                    )
                    break

                if not data:
                    if decoder.buffered_bytes > 0:
                        close_reason = "disconnect_inside_frame"
                        logger.warning(
                            "local_node=%s direction=IN connection_id=%s rpc_id=- event=incomplete_frame peer=%s buffered_bytes=%d",
                            self.local_node_label, connection_id, peer, decoder.buffered_bytes,
                        )
                    else:
                        close_reason = "peer_disconnected"
                        logger.info(
                            "local_node=%s direction=IN connection_id=%s rpc_id=- event=peer_disconnected peer=%s",
                            self.local_node_label, connection_id, peer,
                        )
                    break

                try:
                    frames = decoder.feed_data(data)
                except ProtocolError as exc:
                    close_reason = "protocol_error"
                    logger.warning(
                        "local_node=%s direction=IN connection_id=%s rpc_id=- event=decode_error peer=%s error=%s",
                        self.local_node_label, connection_id, peer, exc,
                    )
                    break


                if decoder.buffered_bytes > 0:
                    if frame_deadline is None:
                        frame_deadline = (
                            asyncio.get_running_loop().time()
                            + self.read_timeout
                        )
                else:
                    frame_deadline = None

                for frame in frames:
                    try:
                        await self._handle_frame(frame, writer, connection_id)
                    except PayloadError as exc:
                        close_reason = "invalid_payload"
                        logger.warning(
                            "local_node=%s direction=IN connection_id=%s rpc_id=%s event=invalid_payload peer=%s error=%s",
                            self.local_node_label, connection_id,
                            frame.request_id.hex(), peer, exc,
                        )
                        await self._send_error(
                            writer=writer,
                            request_frame=frame,
                            code="INVALID_PAYLOAD",
                            message="Payload validation failed",
                            connection_id=connection_id,
                        )
                        return
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            logger.info(
                "local_node=%s direction=IN connection_id=%s rpc_id=- event=connection_closed peer=%s reason=%s",
                self.local_node_label, connection_id, peer, close_reason,
            )

    async def _handle_frame(
        self,
        frame: Frame,
        writer: asyncio.StreamWriter,
        connection_id: str,
    ) -> None:
        rpc_id = frame.request_id.hex()
        logger.info(
            "local_node=%s direction=IN connection_id=%s rpc_id=%s event=frame_received type=%s payload_size=%d",
            self.local_node_label, connection_id, rpc_id,
            frame.message_type.name, len(frame.payload),
        )

        response = await self._dispatcher.dispatch(frame)
        if response is None:
            return

        data = encode_frame(
            response,
            expected_version=self.protocol_version,
            max_payload=self.max_frame_payload,
        )
        writer.write(data)
        await writer.drain()

        logger.info(
            "local_node=%s direction=OUT connection_id=%s rpc_id=%s event=frame_sent type=%s payload_size=%d",
            self.local_node_label, connection_id, response.request_id.hex(),
            response.message_type.name, len(response.payload),
        )

    async def _send_error(
        self,
        writer: asyncio.StreamWriter,
        request_frame: Frame,
        code: str,
        message: str,
        connection_id: str,
    ) -> None:
        error_frame = Frame(
            version=request_frame.version,
            message_type=MessageType.ERROR,
            flags=0,
            request_id=request_frame.request_id,
            payload=encode_error(code=code, message=message),
        )
        data = encode_frame(
            error_frame,
            expected_version=self.protocol_version,
            max_payload=self.max_frame_payload,
        )
        writer.write(data)
        await writer.drain()
        logger.info(
            "local_node=%s direction=OUT connection_id=%s rpc_id=%s event=error_sent code=%s",
            self.local_node_label, connection_id, request_frame.request_id.hex(), code,
        )
