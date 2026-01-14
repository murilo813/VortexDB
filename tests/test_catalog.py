import pytest
import os
from core.catalog import Catalog
from core.config import DATA_DIR

CATALOG_FILE = os.path.join(DATA_DIR, "catalog.json")


@pytest.fixture
def clean_catalog():
    if os.path.exists(CATALOG_FILE):
        os.remove(CATALOG_FILE)


def test_create_table_add_get(clean_catalog):
    cat = Catalog()
    cat.create_table("users")
    cat.add_heap_page("users", 0)

    table = cat.get_table("users")
    assert table is not None
    assert 0 in table["heap_pages"]
