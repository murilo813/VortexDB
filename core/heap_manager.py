# Só pede página
from core.page_manager import PageManager
from core.page import insert_record, iter_records

class HeapManager:
    def __init__(self, page_manager):
        self.page_manager = page_manager
        self.heap_pages = [] # lista de page_ids

    def insert(self, record_bytes):
        print("HEAP MANAGER: insert")
        for page_id in self.heap_pages:
            buffer = self.page_manager.load_page(page_id)
            try:
                offset = insert_record(buffer, record_bytes)
                self.page_manager.save_page(buffer, page_id)
                return page_id, offset
            except ValueError:
                continue
        
        page_id, buffer = self.page_manager.create_page("heap")
        offset = insert_record(buffer, record_bytes)

        self.page_manager.save_page(buffer, page_id)
        self.heap_pages.append(page_id)

        return page_id, offset

    def scan(self):
        print("HEAP MANAGER: scan")
        for page_id in self.heap_pages:
            buffer = self.page_manager.load_page(page_id)
            for record in iter_records(buffer):
                yield record

