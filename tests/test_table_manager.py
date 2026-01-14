import pytest
import os
from core.page_manager import PageManager
from core.buffer_manager import BufferManager
from core.catalog import Catalog
from core.table_manager import TableManager
from core.config import DATA_DIR


@pytest.fixture
def clean_data_dir():
    # limpa páginas e catalog.json
    for f in os.listdir(DATA_DIR):
        if f.startswith("page_") or f == "catalog.json":
            os.remove(os.path.join(DATA_DIR, f))


def test_table_insert_scan(clean_data_dir):
    pm = PageManager()
    bm = BufferManager(pm)
    cat = Catalog()
    tm = TableManager(bm, cat)

    tm.create_table("users")
    record = b"user1"

    pid, off = tm.insert("users", record)
    records = list(tm.scan("users"))

    assert record in records
    assert pid in cat.get_table("users")["heap_pages"]
