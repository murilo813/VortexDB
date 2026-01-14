# Só pede página
from core.page import insert_record, iter_records


class HeapManager:
    def __init__(self, buffer_manager):
        self.buffer_manager = buffer_manager
        self.heap_pages = []  # lista de page_ids

    def insert(self, record_bytes):
        print("HEAP MANAGER: insert")
        for page_id in self.heap_pages:
            buffer = self.buffer_manager.get_page(page_id)

            try:
                offset = insert_record(buffer, record_bytes)
            except ValueError:
                pass
            else:
                self.buffer_manager.mark_dirty(page_id)
                return page_id, offset
            finally:
                self.buffer_manager.unpin(page_id)

        page_id, buffer = self.buffer_manager.create_page("heap")

        self.heap_pages.append(page_id)

        offset = insert_record(buffer, record_bytes)
        self.buffer_manager.mark_dirty(page_id)
        self.buffer_manager.unpin(page_id)

        return page_id, offset

    def scan(self):
        print("HEAP MANAGER: scan")
        for page_id in self.heap_pages:
            buffer = self.buffer_manager.get_page(page_id)
            try:
                for record in iter_records(buffer):
                    yield record
            finally:
                self.buffer_manager.unpin(page_id)
