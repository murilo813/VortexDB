from core.config import PAGE_SIZE, DB_MAGIC, DB_VERSION

mapping = {"heap": 1, "index": 2}
mutable_fields = ['flags', 'free_start', 'free_end', 'lsn']

# cria página vazia
def create_empty_page(page_type, page_id): 
    print(f"PAGE: create_empty_page {page_id}")
    page_type_id = mapping.get(page_type)

    buffer = bytearray(8192)

    buffer[0:4] = (DB_MAGIC).to_bytes(4, byteorder='little')
    buffer[4:6] = (DB_VERSION).to_bytes(2, byteorder='little')
    buffer[6:8] = (page_type_id).to_bytes(2, byteorder='little')
    buffer[8:16] = (page_id).to_bytes(8, byteorder='little')
    buffer[16:20] = (0).to_bytes(4, byteorder='little')
    buffer[20:22] = (64).to_bytes(2, byteorder='little')
    buffer[22:24] = (8192).to_bytes(2, byteorder='little')
    buffer[24:32] = (0).to_bytes(8, byteorder='little')
    buffer[32:64] = bytes(32)

    return buffer

# lê o header do disco
def read_header(buffer):
    PageHeader = {}

    # lê o header do buffer
    PageHeader["magic"] = int.from_bytes(buffer[0:4], 'little')
    PageHeader["version"] = int.from_bytes(buffer[4:6], 'little')
    PageHeader["page_type"] = int.from_bytes(buffer[6:8], 'little')
    PageHeader["page_id"] = int.from_bytes(buffer[8:16], 'little')
    PageHeader["flags"] = int.from_bytes(buffer[16:20], 'little')
    PageHeader["free_start"] = int.from_bytes(buffer[20:22], 'little')
    PageHeader["free_end"] = int.from_bytes(buffer[22:24], 'little')
    PageHeader["lsn"] = int.from_bytes(buffer[24:32], 'little')
    PageHeader["reserved"] = buffer[32:64]

    return PageHeader

# escreve o header no buffer
def write_header(PageHeader, buffer):
    buffer[0:4] = PageHeader["magic"].to_bytes(4, byteorder='little')
    buffer[4:6] = PageHeader["version"].to_bytes(2, byteorder='little')
    buffer[6:8] = PageHeader["page_type"].to_bytes(2, byteorder='little')
    buffer[8:16] = PageHeader["page_id"].to_bytes(8, byteorder='little')
    buffer[16:20] = PageHeader["flags"].to_bytes(4, byteorder='little')
    buffer[20:22] = PageHeader["free_start"].to_bytes(2, byteorder='little')
    buffer[22:24] = PageHeader["free_end"].to_bytes(2, byteorder='little')
    buffer[24:32] = PageHeader["lsn"].to_bytes(8, byteorder='little')
    buffer[32:64] = PageHeader["reserved"]

# valida o header
def validate_header(PageHeader):
    if PageHeader["magic"] != magic:
        return False
    elif PageHeader["version"] != DB_VERSION:
        return False
    elif PageHeader["page_type"] not in [heap, index]:
        return False
    elif PageHeader["free_start"] > PageHeader["free_end"]:
        return False
    else:
        return True

# atualiza campos imutáveis
def update_header_field(PageHeader, buffer, updates):
    if "free_start" in updates or "free_end" in updates:
        new_free_start = updates.get("free_start", PageHeader["free_start"]) #é feito essa variacao aqui pra caso nao venha 1 dos dois, assim o código não trava por não ter o campo no dict
        new_free_end = updates.get("free_end", PageHeader["free_end"])
        if new_free_start > new_free_end:
            raise ValueError("free_start não pode ser maior que free_end")

    for field, value in updates.items():
        if field not in mutable_fields:
            raise  ValueError(f"Campo {field} é imutável")
        PageHeader[field] = value

    write_header(PageHeader, buffer)

    return PageHeader

############## REGISTROS ###################

def insert_record(buffer, record_bytes):
    header = read_header(buffer)
    total_size = 1 + 2 + len(record_bytes) # 1 byte flag + 2 bytes tamanho + dados
    if header["free_end"] - header["free_start"] < total_size:
        raise ValueError("O registro não cabe na página")
    
    header["free_end"] -= total_size
    offset = header["free_end"]

    # marca como ativo
    buffer[offset] = 0

    # tamanho do registro
    buffer[offset+1 : offset+3] = len(record_bytes).to_bytes(2, byteorder='little')

    # dados reais
    buffer[offset+3 : offset+3+len(record_bytes)] = record_bytes

    update_header_field(header, buffer, {"free_end": header["free_end"]})

    return offset

def read_record(buffer, offset):
    record_size = int.from_bytes(buffer[offset:offset+2], byteorder='little')
    record_data = buffer[offset+2 : offset+2+record_size]

    return record_data

def delete_record(buffer, offset):
    buffer[offset] = 1

def iter_records(buffer):
    header = read_header(buffer)
    offset = header["free_end"]

    while offset < PAGE_SIZE:
        flag = buffer[offset]
        record_size = int.from_bytes(buffer[offset+1:offset+3], byteorder='little')
        record_data = buffer[offset+3 : offset+3+record_size]

        if flag == 0:
            yield record_data
        
        offset += 1 + 2 + record_size # flag + tamanho + dados