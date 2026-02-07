from core.table_manager import TableManager
from core.page_manager import PageManager
from core.buffer_manager import BufferManager
from core.catalog import Catalog
from catalog.schema import Schema, Column, INT
from core.config import DATA_DIR
import os


def clean():
    for f in os.listdir(DATA_DIR):
        os.remove(os.path.join(DATA_DIR, f))


def test_table_insert_scan():
    clean()

    pm = PageManager()
    bm = BufferManager(pm)
    catalog = Catalog()
    tm = TableManager(bm, catalog)

    schema = Schema("users", [Column("id", INT)])

    tm.create_table("users", schema)

    tm.insert("users", {"id": 1})
    tm.insert("users", {"id": 2})

    rows = list(tm.scan("users"))

    assert rows == [{"id": 1}, {"id": 2}]
