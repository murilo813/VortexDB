from core.heap_manager import HeapManager
from core.page_manager import PageManager
from core.buffer_manager import BufferManager
from core.config import DATA_DIR
import os


def clean():
    for f in os.listdir(DATA_DIR):
        if f.startswith("page_"):
            os.remove(os.path.join(DATA_DIR, f))


def test_heap_insert_scan():
    clean()

    pm = PageManager()
    bm = BufferManager(pm)
    heap = HeapManager(bm)

    heap.insert({"id": 1})
    heap.insert({"id": 2})

    rows = list(heap.scan())

    assert rows == [{"id": 1}, {"id": 2}]
