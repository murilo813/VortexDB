import os
import json
from core.config import DATA_DIR

CATALOG_FILE = os.path.join(DATA_DIR, "catalog.json")


class Catalog:
    def __init__(self):
        if os.path.exists(CATALOG_FILE):
            self._load()
        else:
            self.tables = {}
            self._save()

    def _load(self):
        print("CATALOG: _load")
        with open(CATALOG_FILE, "r") as f:
            data = json.load(f)
        self.tables = data["tables"]

    def _save(self):
        print("CATALOG: _save")
        with open(CATALOG_FILE, "w") as f:
            json.dump({"tables": self.tables}, f, indent=2)

    def create_table(self, name, schema):
        print(f"CATALOG: create_table {name}")

        if name in self.tables:
            raise ValueError("Tabela já existe")

        self.tables[name] = {
            "heap_pages": [],
            "schema": schema.to_dict()
        }

        self._save()

    def add_heap_page(self, table_name, page_id):
        print(f"CATALOG: add_heap_page {table_name}, {page_id}")
        self.tables[table_name]["heap_pages"].append(page_id)
        self._save()

    def get_table(self, name):
        print(f"CATALOG: get_table {name}")
        return self.tables.get(name)
