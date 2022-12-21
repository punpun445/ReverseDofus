

from io import BufferedReader
from typing import Dict

from utils import *

class D2IReader:

    def __init__(self, stream: BufferedReader):
        
        # attributes
        self._stream = stream

        self._index_table: Dict[int, int] = dict()
        self._index_table_diacritical: Dict[int, int] = dict()
        self._index_table_text: Dict[str, int] = dict()

        # reading metadata
        self._readMetaData()

    def _readMetaData(self):

        indexes_pointer = readInt(self._stream)
        stream.seek(indexes_pointer)

        indexes_size = readInt(self._stream)

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

        start_pos = self._stream.tell()
        while stream.tell() - start_pos < text_indexes_size:
            text_id = readString(self._stream)
            pointer = readInt(self._stream)
            self._index_table_text[text_id] = pointer

    def queryStandard(self, id_):
        self._stream.seek(self._index_table[id_])
        return readString(self._stream)

    def queryDiacritical(self, id_):
        self._stream.seek(self._index_table_diacritical[id_])
        return readString(self._stream)

    def queryTextId(self, text_id):
        self._stream.seek(self._index_table_text[text_id])
        return readString(self._stream)


        




if __name__ == '__main__':

    stream = open('./D2I/i18n_fr.d2i', 'rb')
    
    d = D2IReader(stream)

    print(d.queryStandard(209223))

    stream.close()