from enum import IntEnum

PROTOCOL_VERSION = 1

# Размер фиксированного заголовка кадра:
# version        = 1 байт
# type           = 1 байт
# flags          = 2 байта
# request_id     = 16 байт
# payload_length = 4 байта

HEADER_SIZE = 24

MAX_FRAME_PAYLOAD = 65_536

class MessageType(IntEnum):
    PING = 0x01
    PONG = 0x02
    FIND_NODE_REQUEST = 0x03
    FIND_NODE_RESPONSE = 0x04

    ERROR = 0x7F