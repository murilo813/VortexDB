from core.heap_manager import HeapManager
from catalog.schema import Schema


class TableManager:
    def __init__(self, buffer_manager, catalog):
        self.buffer_manager = buffer_manager
        self.catalog = catalog
        self.heaps = {}

    def create_table(self, name, schema):
        self.catalog.create_table(name, schema)
        heap = HeapManager(self.buffer_manager, schema)

        table_meta = self.catalog.get_table(name)
        heap.heap_pages = table_meta["heap_pages"]

        self.heaps[name] = heap

    def insert(self, table, record_bytes):
        heap = self.heaps.get(table)
        if not heap:
            raise ValueError("Tabela não existe")
        
        table_meta = self.catalog.get_table(table)
        schema = Schema.from_dict(table_meta["schema"])

        schema.validate(record_bytes)

        page_id, offset = heap.insert(record_bytes)

        if page_id not in self.catalog.get_table(table)["heap_pages"]:
            self.catalog.add_heap_page(table, page_id)

        return page_id, offset

    def scan(self, table):
        heap = self.heaps.get(table)
        if not heap:
            raise ValueError("Tabela não existe")
        return heap.scan()
