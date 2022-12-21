
# TODO: function to load all the data in memory

from dataclasses import dataclass
from typing import List, Dict, Any, Callable
from io import BufferedReader

from utils import *



@dataclass(frozen=True)
class _FieldStruct:
    name: str
    type: int

@dataclass(frozen=True)
class _ClassStruct:
    id: int
    name: str
    package_name: str
    fields: List[List[_FieldStruct]]

@dataclass(frozen=True)
class _QueryStruct:
    name: str
    pointer: int
    type: int
    count: int

class InvalidFormat(Exception):
    pass

class D2OReader:

    def __init__(self, stream: BufferedReader) -> None:

        # attributes
        self._stream = stream

        self._index_table: Dict[int, int] = dict()
        self._classes: Dict[int, _ClassStruct] = dict()
        self._query_table: Dict[str ,_QueryStruct] = dict()

        # reading metadata
        self._stream.seek(0)
        header = self._stream.read(3).decode('ascii')
        if header != 'D2O':
            raise InvalidFormat('Couldn\'t recognize the D2O format')     

        self._readIndexTable()
        self._readClasses()
        self._readQueryTable()


    def _readIndexTable(self) -> None:
        """Reads and saves the index table in _index_table.
        Each id maps to a pointer (i.e. a position in the stream)."""

        pointer_table_index = readInt(self._stream)
        self._stream.seek(pointer_table_index)

        index_table_size = readInt(self._stream)

        # format: | id: int | pointer: int | ... index_table_size / 8 times
        for _ in range(index_table_size // 8):
            id_ = readInt(self._stream)
            pointer = readInt(self._stream)
            self._index_table[id_] = pointer

    def _readClasses(self) -> None:
        """Reads and saves the structure of each class if the form of a _ClassStruct object in _classes.
        Each class id maps to a _ClassStruct object"""

        classes_count = readInt(self._stream)

        # format: | id: int | name: string | package_name: string | fields: see _readFields | ... classes_count times
        for _ in range(classes_count):

            id_ = readInt(self._stream)
            name = readString(self._stream)
            package_name = readString(self._stream)

            fields = self._readFields()

            self._classes[id_] = _ClassStruct(id_, name, package_name, fields)

    def _readFields(self) -> List[List[_FieldStruct]]:
        """Reads and returns the fields structure of a class as a list of list of _FieldStruct objects.
        Each field is represented as a list of _FieldStruct objects to account for vector types (code -99) which have an inner type.
        e.g. [[('id', -1)], [('list_of_int', -99), 'inner_type_name', -1], [('a', -99), ('b', -99), ('c', -5)]]"""
        
        fields = []
        fields_count = readInt(self._stream)


        # format: | name: string | type: int | (inner_type: int if type == -99) | ... fields_count times
        # if type == -99, the next integer indicate the inner type (vectors can be nested so keep reading as long as needed).
        for _ in range(fields_count):
            
            name = readString(self._stream)
            type = readInt(self._stream)

            nested_fields = [_FieldStruct(name, type)]

            while nested_fields[-1].type == -99:

                name = readString(self._stream)
                type = readInt(self._stream)
                nested_fields.append(_FieldStruct(name, type))
            
            fields.append(nested_fields)
        
        return fields
    
    def _readQueryTable(self) -> None:
        """Reads and saves the query table in _query_table.
        Each key in the query table is "queryable" and maps to a _QueryStruct object.
        The query table makes it possible to query for all objects sharing some property without having to load the entire dataset."""

        if not len(self._stream.peek()):
            return
        
        query_table_size = readInt(self._stream)
        pointer_offset = self._stream.tell() + query_table_size + 4

        # format: | name: string | pointer: int | type: int | count: int | ... while inside the query table
        while self._stream.tell() < pointer_offset-4:
            name = readString(self._stream)
            pointer = readInt(self._stream) + pointer_offset
            type = readInt(self._stream)
            count = readInt(self._stream)

            self._query_table[name] = _QueryStruct(name, pointer, type, count)

    def _readTypeData(self, nested_types: List[int], i: int) -> Any:
        """Read and return a type field. The function is recursive to handle nested vector types."""
        match nested_types[i]:
            case -1:
                return readInt(self._stream)
            case -2:
                return readBool(self._stream)
            case -3:
                return readString(self._stream)
            case -4:
                return readDouble(self._stream)
            case -5:
                return readInt(self._stream)
            case -6:
                return readUInt(self._stream)
            case -99:
                vector_len = readInt(self._stream)
                vector = []
                for _ in range(vector_len):
                    vector.append(self._readTypeData(nested_types, i+1))
                return vector
            case _:
                return self._readClassData()

    def _readClassData(self) -> Dict[str, Any]:
        """Read and return a class object."""
        class_data = dict()
        id_ = readInt(self._stream)

        for nested_fields in self._classes[id_].fields:
            nested_types = [field.type for field in nested_fields]
            class_data[nested_fields[0].name] = self._readTypeData(nested_types, 0)

        return class_data

    def makeQuery(self, key: str, func: Callable[[Any], bool]) -> List[Dict[str, Any]]:
        """Uses the query table to return all class object filtered by func on the property key."""

        # format: | value1 | list of index table indexes | value2 | list of index table indexes | ... count times
        # where count is the field in the _QueryStruct object

        query_struct = self._query_table[key]
        self._stream.seek(query_struct.pointer)

        query_indexes = []

        for _ in range(query_struct.count):

            # n.b. reusing _readTypeData is justified since queryable types are never vectors or classes
            value = self._readTypeData([query_struct.type], 0)
            vector_size = readInt(self._stream)

            if func(value):
                for _ in range(vector_size // 4):
                    query_indexes.append(readInt(self._stream))
            
            else:
                self._stream.seek(vector_size, 1)
        
        query_result = []
        for query_index in query_indexes:
            self._stream.seek(self._index_table[query_index])
            query_result.append(self._readClassData())

        return query_result

    def getQueryable(self) -> List[_QueryStruct]:
        """Returns the list of queryable key."""
        return list(self._query_table.keys())

    def getPossibleValues(self, key: str) -> List[Any]:
        """Returns a list of all possible values for the given key."""

        query_struct = self._query_table[key]
        self._stream.seek(query_struct.pointer)
        values = []

        for _ in range(query_struct.count):

            value = self._readTypeData([query_struct.type], 0)
            values.append(value)
            vector_size = readInt(self._stream)     
            self._stream.seek(vector_size, 1)

        return values




if __name__ == '__main__':

    stream = open('D2O/Monsters.d2o', 'rb')

    d = D2OReader(stream)

    print(d.makeQuery('grades.intelligence', lambda x: x > 2000)[-1]['nameId'])


    stream.close()

    

