from core.page import serialize_record, deserialize_record, create_empty_page, insert_record, iter_records


def test_serialize_roundtrip():
    record = {"id": 1, "name": "alice", "active": True}

    raw = serialize_record(record)
    back = deserialize_record(raw)

    assert back == record


def test_insert_and_iter_records():
    buf = create_empty_page("heap", 0)

    r1 = serialize_record({"id": 1})
    r2 = serialize_record({"id": 2})

    insert_record(buf, r1)
    insert_record(buf, r2)

    rows = [deserialize_record(r) for r in iter_records(buf)]

    assert rows == [{"id": 1}, {"id": 2}]
