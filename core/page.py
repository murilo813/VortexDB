from core.config import PAGE_SIZE, DB_MAGIC, DB_VERSION
import struct

mapping = {"heap": 1, "index": 2}

# Configurações do Layout
HEADER_SIZE = 64
SLOT_SIZE = 4  # 2 bytes offset + 2 bytes length

# Slot Status
SLOT_EMPTY = 0
SLOT_ACTIVE = 1


def create_empty_page(page_type, page_id):
    page_type_id = mapping.get(page_type, 1)

    buffer = bytearray(PAGE_SIZE)

    buffer[0:4] = (DB_MAGIC).to_bytes(4, "little")
    buffer[4:6] = (DB_VERSION).to_bytes(2, "little")
    buffer[6:8] = (page_type_id).to_bytes(2, "little")
    buffer[8:16] = (page_id).to_bytes(8, "little")
    buffer[16:20] = (0).to_bytes(4, "little")
    buffer[20:22] = (0).to_bytes(2, "little")
    buffer[22:24] = (HEADER_SIZE).to_bytes(2, "little")
    buffer[24:26] = (PAGE_SIZE).to_bytes(2, "little")
    buffer[26:34] = (0).to_bytes(8, "little")

    return buffer


def read_header(buffer):
    return {
        "page_id": int.from_bytes(buffer[8:16], "little"),
        "slot_count": int.from_bytes(buffer[20:22], "little"),
        "free_start": int.from_bytes(buffer[22:24], "little"),
        "free_end": int.from_bytes(buffer[24:26], "little"),
    }


def _update_header(buffer, slot_count, free_start, free_end):
    buffer[20:22] = slot_count.to_bytes(2, "little")
    buffer[22:24] = free_start.to_bytes(2, "little")
    buffer[24:26] = free_end.to_bytes(2, "little")


def insert_record(buffer, record_bytes):
    header = read_header(buffer)

    record_size = len(record_bytes)
    space_needed = SLOT_SIZE + record_size

    free_space = header["free_end"] - header["free_start"]

    if free_space < space_needed:
        raise ValueError("Page Full")

    new_data_offset = header["free_end"] - record_size

    buffer[new_data_offset : new_data_offset + record_size] = record_bytes

    slot_offset = header["free_start"]

    buffer[slot_offset : slot_offset + 2] = new_data_offset.to_bytes(2, "little")
    buffer[slot_offset + 2 : slot_offset + 4] = record_size.to_bytes(2, "little")

    new_slot_count = header["slot_count"] + 1
    new_free_start = header["free_start"] + SLOT_SIZE
    new_free_end = new_data_offset

    _update_header(buffer, new_slot_count, new_free_start, new_free_end)

    return header["slot_count"]


def get_record(buffer, slot_id):
    header = read_header(buffer)

    if slot_id >= header["slot_count"]:
        return None

    slot_mem_pos = HEADER_SIZE + (slot_id * SLOT_SIZE)

    record_offset = int.from_bytes(buffer[slot_mem_pos : slot_mem_pos + 2], "little")
    record_len = int.from_bytes(buffer[slot_mem_pos + 2 : slot_mem_pos + 4], "little")

    if record_offset == 0:
        return None

    return buffer[record_offset : record_offset + record_len]


def delete_record(buffer, slot_id):
    header = read_header(buffer)

    if slot_id >= header["slot_count"]:
        return False

    slot_mem_pos = HEADER_SIZE + (slot_id * SLOT_SIZE)

    buffer[slot_mem_pos : slot_mem_pos + 2] = (0).to_bytes(2, "little")
    return True


def iter_records(buffer):
    header = read_header(buffer)

    for i in range(header["slot_count"]):
        record = get_record(buffer, i)
        if record:
            yield record


# serialização
def serialize_record(record: dict, schema) -> bytes:
    result = bytearray()

    for column in schema.columns:
        value = record[column.name]

        if column.dtype.name == "TEXT":
            # TEXT é variavel, 2 bytes de tamanho + bytes da string
            text_bytes = value.encode("utf-8")
            result.extend(len(text_bytes).to_bytes(2, "little"))
            result.extend(text_bytes)
        else:
            result.extend(struct.pack(column.dtype.struct_format, value))

    return bytes(result)


def deserialize_record(raw_bytes: bytes, schema) -> dict:
    record = {}
    offset = 0

    for column in schema.columns:
        if column.dtype.name == "TEXT":
            length = int.from_bytes(raw_bytes[offset : offset + 2], "little")
            offset += 2
            record[column.name] = raw_bytes[offset : offset + length].decode("utf-8")
            offset += length
        else:
            fmt = column.dtype.struct_format
            size = struct.calcsize(fmt)
            value = struct.unpack(fmt, raw_bytes[offset : offset + size])[0]
            record[column.name] = value
            offset += size

    return record
