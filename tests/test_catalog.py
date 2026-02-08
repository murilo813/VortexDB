import pytest
from core.catalog import Catalog
from catalog.schema import Schema, Column, INT


def test_create_table_add_get():
    cat = Catalog()
    schema = Schema("users", [Column("id", INT)])
    cat.create_table("users", schema)
    cat.add_heap_page("users", 0)

    table = cat.get_table("users")
    assert table is not None
    assert 0 in table["heap_pages"]
    assert table["schema"]["name"] == "users"


def test_catalog_index_persistence():
    cat = Catalog()
    schema = Schema("users", [Column("id", INT)])
    cat.create_table("users", schema)

    cat.update_table_root("users", 5)

    table = cat.get_table("users")

    assert table is not None
    assert table["index_root_id"] == 5
