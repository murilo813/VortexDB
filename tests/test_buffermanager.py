import pytest
from core.page_manager import PageManager
from core.buffer_manager import BufferManager


def test_buffer_create_get_flush():
    pm = PageManager()
    bm = BufferManager(pm, max_pages=2)

    page_id, buffer = bm.create_page("heap")

    buffer[64] = 123
    bm.mark_dirty(page_id)
    bm.unpin(page_id)

    bm.flush_all()

    loaded = bm.get_page(page_id)
    assert loaded[64] == 123
    bm.unpin(page_id)


def test_buffer_full_eviction():
    pm = PageManager()
    bm = BufferManager(pm, max_pages=1)

    p1, _ = bm.create_page("heap")
    bm.unpin(p1)

    p2, _ = bm.create_page("heap")
    assert p2 == 1
    assert p1 not in bm.pages
    bm.unpin(p2)
