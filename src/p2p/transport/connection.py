import asyncio
import logging
import secrets

from p2p.protocol.codec import ProtocolError, encode_frame
from p2p.protocol.frame import Frame
from p2p.transport.stream_decoder import StreamDecoder

logger = logging.getLogger(__name__)


class RpcError(Exception):
    """Ошибка выполнения RPC-запроса."""


async def send_request(
    host: str,
    port: int,
    frame: Frame,
    connect_timeout: float = 3.0,
    response_timeout: float = 5.0,
    protocol_version: int = 1,
    max_frame_payload: int = 65_536,
    local_node_label: str = "stage1-unknown",
) -> Frame:
    writer: asyncio.StreamWriter | None = None
    rpc_id = frame.request_id.hex()
    connection_id = secrets.token_hex(8)

    try:
        logger.info(
            "local_node=%s direction=OUT connection_id=%s rpc_id=%s event=connect_start peer=%s:%d",
            local_node_label, connection_id, rpc_id, host, port,
        )

        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=connect_timeout,
        )

        logger.info(
            "local_node=%s direction=OUT connection_id=%s rpc_id=%s event=connect_ok peer=%s:%d",
            local_node_label, connection_id, rpc_id, host, port,
        )

        data = encode_frame(
            frame,
            expected_version=protocol_version,
            max_payload=max_frame_payload,
        )

        writer.write(data)
        await writer.drain()

        logger.info(
            "local_node=%s direction=OUT connection_id=%s rpc_id=%s event=frame_sent type=%s payload_size=%d",
            local_node_label, connection_id, rpc_id,
            frame.message_type.name, len(frame.payload),
        )

        decoder = StreamDecoder(
            expected_version=protocol_version,
            max_payload=max_frame_payload,
        )

        while True:
            try:
                data = await asyncio.wait_for(
                    reader.read(4096),
                    timeout=response_timeout,
                )
            except TimeoutError as exc:
                logger.warning(
                    "local_node=%s direction=IN connection_id=%s rpc_id=%s event=read_timeout peer=%s:%d",
                    local_node_label, connection_id, rpc_id, host, port,
                )
                raise RpcError("Timed out waiting for response") from exc

            if not data:
                logger.warning(
                    "local_node=%s direction=IN connection_id=%s rpc_id=%s event=connection_closed_before_response peer=%s:%d",
                    local_node_label, connection_id, rpc_id, host, port,
                )
                raise RpcError("Connection closed before response")

            try:
                responses = decoder.feed_data(data)
            except ProtocolError as exc:
                logger.warning(
                    "local_node=%s direction=IN connection_id=%s rpc_id=%s event=decode_error peer=%s:%d error=%s",
                    local_node_label, connection_id, rpc_id, host, port, exc,
                )
                raise RpcError(f"Invalid response: {exc}") from exc

            for response in responses:
                response_rpc_id = response.request_id.hex()
                logger.info(
                    "local_node=%s direction=IN connection_id=%s rpc_id=%s event=frame_received type=%s payload_size=%d",
                    local_node_label, connection_id, response_rpc_id,
                    response.message_type.name, len(response.payload),
                )

                if response.request_id != frame.request_id:
                    logger.warning(
                        "local_node=%s direction=IN connection_id=%s rpc_id=%s event=correlation_error expected_rpc_id=%s",
                        local_node_label, connection_id, response_rpc_id, rpc_id,
                    )
                    raise RpcError("Response request_id does not match request")

                return response

    finally:
        if writer is not None:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

        logger.info(
            "local_node=%s direction=OUT connection_id=%s rpc_id=%s event=connection_closed peer=%s:%d",
            local_node_label, connection_id, rpc_id, host, port,
        )


async def send_frame(
    host: str,
    port: int,
    frame: Frame,
    connect_timeout: float = 3.0,
    protocol_version: int = 1,
    max_frame_payload: int = 65_536,
    local_node_label: str = "stage1-unknown",
) -> None:
    writer: asyncio.StreamWriter | None = None
    rpc_id = frame.request_id.hex()
    connection_id = secrets.token_hex(8)

    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=connect_timeout,
        )
        del reader

        logger.info(
            "local_node=%s direction=OUT connection_id=%s rpc_id=%s event=connect_ok peer=%s:%d",
            local_node_label, connection_id, rpc_id, host, port,
        )

        data = encode_frame(
            frame,
            expected_version=protocol_version,
            max_payload=max_frame_payload,
        )
        writer.write(data)
        await writer.drain()

        logger.info(
            "local_node=%s direction=OUT connection_id=%s rpc_id=%s event=frame_sent type=%s payload_size=%d",
            local_node_label, connection_id, rpc_id,
            frame.message_type.name, len(frame.payload),
        )
    finally:
        if writer is not None:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

        logger.info(
            "local_node=%s direction=OUT connection_id=%s rpc_id=%s event=connection_closed peer=%s:%d",
            local_node_label, connection_id, rpc_id, host, port,
        )
