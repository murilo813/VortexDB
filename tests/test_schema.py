import pytest
from catalog.schema import Schema, Column, INT, TEXT


def test_schema_validate_ok():
    schema = Schema(
        "users",
        [
            Column("id", INT),
            Column("name", TEXT),
        ],
    )

    schema.validate({"id": 1, "name": "bob"})


def test_schema_missing_column():
    schema = Schema("users", [Column("id", INT)])

    with pytest.raises(ValueError):
        schema.validate({})
