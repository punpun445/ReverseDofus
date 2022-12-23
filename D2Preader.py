

from utils import *
from typing import Dict, List
from dataclasses import dataclass
from os.path import split, join


@dataclass(frozen=True)
class FileStruct:
    offset: int
    length: int
    stream_index: int


class D2PReader:

    def __init__(self, path: str, load_linked=False) -> None:
        
        # attributes
        self._dir, self._to_load_next = split(path)
        
        self._load_linked = load_linked

        self._streams = []
        self._index_table: Dict[str, FileStruct] = dict()

        self._readNext()
    
    def __del__(self) -> None:
        for stream in self._streams:
            stream.close()

    def _readNext(self) -> None:
        """Reads and save the positions and names of data files inside the d2p file.
        If load_linked is true, do the same for following d2p files recursively."""

        path = join(self._dir, self._to_load_next)
        stream = open(path, 'rb')
        stream_index = len(self._streams)

        self._streams.append(stream)

        v_max = readUByte(stream)
        v_min = readUByte(stream)

        stream.seek(-24, 2)

        data_offset = readUInt(stream)
        data_count = readUInt(stream)
        index_offset = readUInt(stream)
        index_count = readUInt(stream)
        properties_offset = readUInt(stream)
        properties_count = readUInt(stream)

        # couldn't figure out how to get all the properties so just get the link
        # to the following d2p file if there is one
        
        stream.seek(properties_offset)
        self._to_load_next = None

        # format: | property_name: string | property_value: string |

        try:
            property_name = readString(stream)
            property_value = readString(stream)

            if property_name == 'link':
                self._to_load_next = property_value

        except UnicodeDecodeError:
            pass
        
        stream.seek(index_offset)

        # format: | file_path: string | file_offset: int | file_length: int | ... index_count times
        for _ in range(index_count):
            file_path = readString(stream)
            file_offset = readInt(stream)
            file_length = readInt(stream)

            self._index_table[file_path] = FileStruct(file_offset + data_offset, file_length, stream_index)


        if self._load_linked and self._to_load_next is not None:
            self._readNext()

    def getFileNames(self) -> List[str]:
        """Returns a list of all loadable files."""
        return self._index_table.keys()

    def loadFile(self, data_path) -> bytes:
        """Returns the content of the file in bytes."""
        file_struct = self._index_table[data_path]

        self._streams[file_struct.stream_index].seek(file_struct.offset)

        return self._streams[file_struct.stream_index].read(file_struct.length)




if __name__ == '__main__':
    pass


