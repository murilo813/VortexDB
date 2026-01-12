from core.heap_manager import HeapManager 

class TableManager:
    def __init__(self, page_manager, catalog):
        self.page_manager = page_manager
        self.catalog = catalog
        self.heaps = {}

    def create_table(self, name):
        self.catalog.create_table(name)
        heap = HeapManager(self.page_manager)

        table_meta = self.catalog.get_table(name)
        heap.heap_pages = table_meta["heap_pages"]

        self.heaps[name] = heap
    
    def insert(self, table, record_bytes):
        heap = self.heaps.get(table)
        if not heap:
            raise ValueError("Tabela não existe")

        page_id, offset = heap.insert(record_bytes)

        # se a página for nova, registra no catalogo
        if page_id not in self.catalog.get_table(table)["heap_pages"]:
            self.catalog.add_heap_page(table, page_id)

        return page_id, offset
    
    def scan(self, table):
        heap = self.heaps.get(table)
        if not heap:
            raise ValueError("Tabela não existe")
        return heap.scan()
