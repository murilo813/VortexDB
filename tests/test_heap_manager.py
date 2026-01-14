import pytest
import os
from core.page_manager import PageManager
from core.buffer_manager import BufferManager
from core.heap_manager import HeapManager
from core.config import DATA_DIR


@pytest.fixture
def clean_data_dir():
    for f in os.listdir(DATA_DIR):
        if f.startswith("page_"):
            os.remove(os.path.join(DATA_DIR, f))


def test_heap_insert_scan(clean_data_dir):
    pm = PageManager()
    bm = BufferManager(pm)
    heap = HeapManager(bm)

    r1 = b"record1"
    r2 = b"record2"

    pid1, off1 = heap.insert(r1)
    pid2, off2 = heap.insert(r2)

    records = list(heap.scan())
    assert r1 in records
    assert r2 in records
