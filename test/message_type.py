from enum import Enum


class TypeMessage(Enum):
    UNSUB = '\x00'
    SUB = '\x01'
    ID = '\x10'
    ACK = '\x11'