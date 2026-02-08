from core.heap_manager import HeapManager
from catalog.schema import Schema
from index.btree_manager import BTreeManager


class TableManager:
    def __init__(self, buffer_manager, catalog):
        self.buffer_manager = buffer_manager
        self.catalog = catalog
        self.heaps = {}
        self.indexes = {}

    def create_table(self, name, schema):
        self.catalog.create_table(name, schema)
        heap = HeapManager(self.buffer_manager, schema)
        table_meta = self.catalog.get_table(name)
        heap.heap_pages = table_meta["heap_pages"]
        self.heaps[name] = heap

        root_id = table_meta.get("index_root_id")

        def update_catalog_root(new_id):
            self.catalog.update_table_root(name, new_id)

        self.indexes[name] = BTreeManager(
            self.buffer_manager, root_id, on_root_change=update_catalog_root
        )

    def insert(self, table, record_dict):
        heap = self.heaps.get(table)
        if not heap:
            raise ValueError("Tabela não existe")

        table_meta = self.catalog.get_table(table)
        schema = Schema.from_dict(table_meta["schema"])
        schema.validate(record_dict)

        page_id, slot_id = heap.insert(record_dict)

        if "id" in record_dict:
            btree = self.indexes[table]
            btree.insert(record_dict["id"], page_id, slot_id)

        if page_id not in table_meta["heap_pages"]:
            self.catalog.add_heap_page(table, page_id)

        return page_id, slot_id

    def select_by_id(self, table, target_id):
        btree = self.indexes.get(table)
        if not btree or btree.root_page_id is None:
            for row in self.scan(table):
                if row["id"] == target_id:
                    return row
            return None

        rid = btree.search(target_id)
        if rid:
            page_id, slot_id = rid
            print(
                f"✅ ID {target_id} encontrado via Índice na Página {page_id}, Slot {slot_id}"
            )
            return rid
        return None

    def scan(self, table):
        heap = self.heaps.get(table)
        if not heap:
            raise ValueError("Tabela não existe")
        return heap.scan()
