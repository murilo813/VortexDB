import pytest
import os
from core.page_manager import PageManager
from core.buffer_manager import BufferManager
from core.config import DATA_DIR


@pytest.fixture
def clean_data_dir():
    for f in os.listdir(DATA_DIR):
        if f.startswith("page_"):
            os.remove(os.path.join(DATA_DIR, f))


def test_create_load_save_page(clean_data_dir):
    pm = PageManager()
    bm = BufferManager(pm)

    page_id, buffer = bm.create_page("heap")  # agora correto
    assert page_id == 0

    buffer[64] = 99
    bm.mark_dirty(page_id)
    bm.unpin(page_id)
    bm.flush_all()

    loaded = bm.get_page(page_id)
    assert loaded[64] == 99
    bm.unpin(page_id)
