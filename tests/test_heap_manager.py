from core.heap_manager import HeapManager
from core.page_manager import PageManager
from core.buffer_manager import BufferManager
from catalog.schema import Schema, Column, INT


def test_heap_insert_scan():
    pm = PageManager()
    bm = BufferManager(pm)

    schema = Schema("heap_test", [Column("id", INT)])

    heap = HeapManager(bm, schema)

    heap.insert({"id": 1})
    heap.insert({"id": 2})

    rows = list(heap.scan())

    assert rows == [{"id": 1}, {"id": 2}]
