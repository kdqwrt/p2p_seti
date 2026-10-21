
import time
from collections.abc import Callable

from p2p.protocol.constants import MessageType
from p2p.protocol.contact import Contact
from p2p.protocol.frame import Frame
from p2p.protocol.messages import (
    decode_ping,
    encode_error,
    encode_pong,
)


class Dispatcher:
    """Маршрутизирует входящие запросы к обработчикам."""

    def __init__(
        self,
        local_contact: Contact,
        on_ping: Callable[[Contact], None] | None = None,
    ) -> None:
        self.local_contact = local_contact
        self.on_ping = on_ping

    async def dispatch(self, frame: Frame) -> Frame:
        if frame.message_type == MessageType.PING:
            return self._handle_ping(frame)

        if frame.message_type in {
            MessageType.PONG,
            MessageType.FIND_NODE_RESPONSE,
            MessageType.ERROR,
        }:
            return self._make_error(
                frame,
                code="INVALID_DIRECTION",
                message="Response message is not valid as a request",
            )

        if frame.message_type == MessageType.FIND_NODE_REQUEST:
            return self._make_error(
                frame,
                code="NOT_IMPLEMENTED",
                message="FIND_NODE is not implemented",
            )

        return self._make_error(
            frame,
            code="UNSUPPORTED_MESSAGE",
            message="Unsupported message type",
        )

    def _handle_ping(self, frame: Frame) -> Frame:
        ping = decode_ping(frame.payload)

        if self.on_ping is not None:
            self.on_ping(ping["sender"])

        payload = encode_pong(
            responder=self.local_contact,
            ping_timestamp_ms=ping["timestamp_ms"],
            responder_timestamp_ms=int(time.time() * 1000),
        )

        return Frame(
            version=frame.version,
            message_type=MessageType.PONG,
            flags=0,
            request_id=frame.request_id,
            payload=payload,
        )

    @staticmethod
    def _make_error(
        frame: Frame,
        code: str,
        message: str,
    ) -> Frame:
        return Frame(
            version=frame.version,
            message_type=MessageType.ERROR,
            flags=0,
            request_id=frame.request_id,
            payload=encode_error(
                code=code,
                message=message,
            ),
        )
