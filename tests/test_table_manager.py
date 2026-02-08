from core.table_manager import TableManager
from core.page_manager import PageManager
from core.buffer_manager import BufferManager
from core.catalog import Catalog
from catalog.schema import Schema, Column, INT


def test_table_insert_scan():
    pm = PageManager()
    bm = BufferManager(pm)
    catalog = Catalog()
    tm = TableManager(bm, catalog)

    schema = Schema("users", [Column("id", INT)])

    tm.create_table("users", schema)

    tm.insert("users", {"id": 1})
    tm.insert("users", {"id": 2})

    rows = list(tm.scan("users"))

    ids = sorted([r["id"] for r in rows])
    assert ids == [1, 2]


def test_table_select_by_id_with_index():
    pm = PageManager()
    bm = BufferManager(pm)
    catalog = Catalog()
    tm = TableManager(bm, catalog)

    schema = Schema("users", [Column("id", INT)])
    tm.create_table("users", schema)

    tm.insert("users", {"id": 100})
    tm.insert("users", {"id": 200})

    rid = tm.select_by_id("users", 100)
    assert rid is not None
    assert rid[0] == 0

    assert tm.select_by_id("users", 999) is None
