

# TODO: a way to query for multiple ids in a single pass


from typing import Dict

from utils import *

class D2IReader:

    def __init__(self, path: str):
        
        # attributes
        self._stream = open(path, 'rb')

        self._index_table: Dict[int, int] = dict()
        self._index_table_diacritical: Dict[int, int] = dict()
        self._index_table_text: Dict[str, int] = dict()

        # reading accessor data
        self._readAccessorData()

    def __del__(self) -> None:
        self._stream.close()

    def _readAccessorData(self) -> None:
        """Read and save the index table and the text index table in the stream.
        Doesn't read the bytes corresponding to the "text sort indexes"."""

        indexes_pointer = readInt(self._stream)
        self._stream.seek(indexes_pointer)

        indexes_size = readInt(self._stream)

        # format: | id: int | has_diacritical: bool | pointer: int | (pointer_diacritical: int if has_diacritical) | ...
        i = 0
        while i < indexes_size:
            id_ = readInt(self._stream)
            has_diacritical = readBool(self._stream)
            pointer = readInt(self._stream)
            self._index_table[id_] = pointer

            if has_diacritical:
                pointer_diacritical = readInt(self._stream)
                self._index_table_diacritical[id_] = pointer_diacritical
                i += 4
            i += 9

        text_indexes_size = readInt(self._stream)

        # format: | text_id: string | pointer: int | ...
        start_pos = self._stream.tell()
        while self._stream.tell() - start_pos < text_indexes_size:
            text_id = readString(self._stream)
            pointer = readInt(self._stream)
            self._index_table_text[text_id] = pointer

    def queryStandard(self, id_: int) -> str:
        """Query the string with the given id."""
        self._stream.seek(self._index_table[id_])
        return readString(self._stream)

    def queryDiacritical(self, id_: int) -> str:
        """Query the diacritical string with the given id."""
        self._stream.seek(self._index_table_diacritical[id_])
        return readString(self._stream)

    def queryTextId(self, text_id: str) -> str:
        """Query the string with the given text_id."""
        self._stream.seek(self._index_table_text[text_id])
        return readString(self._stream)

    def hasDiacritical(self, id_: int) -> bool:
        """Return wether the id has a corresponding diacritical string."""
        return False if self._index_table_diacritical.get(id_) is None else True


if __name__ == '__main__':
    pass