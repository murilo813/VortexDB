import pytest
from core.page_manager import PageManager
from core.buffer_manager import BufferManager
from index.btree_manager import BTreeManager


def test_btree_insert_and_search():
    pm = PageManager()
    bm = BufferManager(pm)
    btree = BTreeManager(bm)

    btree.insert(10, heap_page_id=1, slot_id=1)
    btree.insert(20, heap_page_id=1, slot_id=2)
    btree.insert(5, heap_page_id=2, slot_id=1)

    assert btree.search(10) == (1, 1)
    assert btree.search(20) == (1, 2)
    assert btree.search(5) == (2, 1)
    assert btree.search(99) is None


def test_btree_split_root():
    pm = PageManager()
    bm = BufferManager(pm)
    btree = BTreeManager(bm)

    # inserimos registros suficientes para forçar um split
    for i in range(1, 1000):
        btree.insert(i, heap_page_id=i, slot_id=0)

    assert btree.search(1) == (1, 0)
    assert btree.search(500) == (500, 0)
    assert btree.search(999) == (999, 0)
