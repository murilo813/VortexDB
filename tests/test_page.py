import pytest
from core.page import (
    serialize_record,
    deserialize_record,
    create_empty_page,
    insert_record,
    iter_records,
)
from catalog.schema import Schema, Column, INT, TEXT, BOOL


def test_serialize_roundtrip():
    schema = Schema(
        "test", [Column("id", INT), Column("name", TEXT), Column("active", BOOL)]
    )

    record = {"id": 1, "name": "alice", "active": True}

    raw = serialize_record(record, schema)
    back = deserialize_record(raw, schema)

    assert back == record


def test_insert_and_iter_records():
    schema = Schema("test", [Column("id", INT)])
    buf = create_empty_page("heap", 0)

    r1 = serialize_record({"id": 1}, schema)
    r2 = serialize_record({"id": 2}, schema)

    insert_record(buf, r1)
    insert_record(buf, r2)

    rows = [deserialize_record(r, schema) for r in iter_records(buf)]

    assert rows == [{"id": 1}, {"id": 2}]
