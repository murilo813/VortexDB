import struct
from typing import Optional
from core.config import PAGE_SIZE
from index.offsets import OFFSET_KEY_COUNT

INDEX_HEADER_FMT = "<IQHHQQ"
INDEX_HEADER_SIZE = struct.calcsize(INDEX_HEADER_FMT)

class BTreeNode:
    def __init__(self, page_id: int, buffer: bytearray, is_leaf: Optional[bool] = None):
        self.page_id = page_id
        self.buffer = buffer
        self.header_fmt = INDEX_HEADER_FMT

        if is_leaf is None:
            header = struct.unpack_from(self.header_fmt, self.buffer, 0)
            is_leaf = header[2] == 1

        self._set_type(is_leaf)

    def _set_type(self, is_leaf):
        self.is_leaf = is_leaf
        if is_leaf:
            self.entry_fmt = "III"
        else:
            self.entry_fmt = "II"

        self.entry_size = struct.calcsize(self.entry_fmt)

    def write_header(self, is_leaf, key_count, parent=0, next_pg=0):
        struct.pack_into(
            self.header_fmt,
            self.buffer,
            0,
            self.page_id,
            2,
            1 if is_leaf else 0,
            key_count,
            parent,
            next_pg,
        )

    def get_key(self, index):
        offset = INDEX_HEADER_SIZE + (index * self.entry_size)
        data = struct.unpack_from(self.entry_fmt, self.buffer, offset)
        return data[0]

    def find_slot(self, key):
        key_count = struct.unpack_from("H", self.buffer, OFFSET_KEY_COUNT)[0]
        if key_count == 0:
            return 0

        low = 0
        high = key_count - 1

        while low <= high:
            mid = (low + high) // 2
            mid_key = self.get_key(mid)

            if mid_key == key:
                return mid
            elif mid_key < key:
                low = mid + 1
            else:
                high = mid - 1
        return low

    def insert_entry(self, key, values):
        header = struct.unpack_from(self.header_fmt, self.buffer, 0)
        key_count = struct.unpack_from("H", self.buffer, OFFSET_KEY_COUNT)[0]
        
        if (INDEX_HEADER_SIZE + (key_count + 1) * self.entry_size) > PAGE_SIZE:
            raise ValueError("Página de índice cheia!")

        idx = self.find_slot(key)

        if idx < key_count:
            src_offset = INDEX_HEADER_SIZE + (idx * self.entry_size)
            dst_offset = src_offset + self.entry_size
            size_to_move = (key_count - idx) * self.entry_size
            
            self.buffer[dst_offset : dst_offset + size_to_move] = self.buffer[src_offset : src_offset + size_to_move]

        new_entry_offset = INDEX_HEADER_SIZE + (idx * self.entry_size)
        struct.pack_into(self.entry_fmt, self.buffer, new_entry_offset, key, *values)

        struct.pack_into("H", self.buffer, OFFSET_KEY_COUNT, key_count + 1)
        return True