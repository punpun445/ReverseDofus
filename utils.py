
import struct
from io import BufferedReader


def readInt(stream: BufferedReader) -> int:
    return struct.unpack('>i', stream.read(4))[0]

def readBool(stream: BufferedReader) -> bool:
    return struct.unpack('?', stream.read(1))[0]

def readString(stream: BufferedReader) -> str:
    string_len, = struct.unpack('>h', stream.read(2))
    return stream.read(string_len).decode('utf-8')

def readDouble(stream: BufferedReader) -> float:
    return struct.unpack('>d', stream.read(8))[0]

def readUInt(stream: BufferedReader) -> float:
    return struct.unpack('>I', stream.read(4))[0]