from core.page import create_empty_page, insert_record, iter_records


def test_create_empty_page():
    buffer = create_empty_page("heap", 0)
    # Checa header básico
    assert buffer[6:8] == (1).to_bytes(2, "little")  # heap
    assert len(buffer) == 8192


def test_insert_and_iter_record():
    buffer = create_empty_page("heap", 0)
    record = b"hello"
    offset = insert_record(buffer, record)
    records = list(iter_records(buffer))
    assert record in records
    assert offset >= 0
