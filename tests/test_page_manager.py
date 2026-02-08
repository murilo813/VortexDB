import pytest
from core.page_manager import PageManager
from core.buffer_manager import BufferManager


def test_create_load_save_page():
    pm = PageManager()
    bm = BufferManager(pm)

    page_id, buffer = bm.create_page("heap")
    assert page_id == 0

    buffer[header_size_offset := 64] = 99
    bm.mark_dirty(page_id)
    bm.unpin(page_id)
    bm.flush_all()

    loaded = bm.get_page(page_id)
    assert loaded[64] == 99
    bm.unpin(page_id)
