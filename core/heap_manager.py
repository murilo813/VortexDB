# Só pede página
from core.page import insert_record, iter_records, serialize_record, deserialize_record


class HeapManager:
    def __init__(self, buffer_manager, schema):
        self.buffer_manager = buffer_manager
        self.schema = schema
        self.heap_pages = []  # lista de page_ids

    def insert(self, record_bytes):
        print("HEAP MANAGER: insert")
        for page_id in self.heap_pages:
            buffer = self.buffer_manager.get_page(page_id)

            try:
                raw = serialize_record(record_bytes, self.schema)
                slot_id = insert_record(buffer, raw)
                
            except ValueError:
                pass
            else:
                self.buffer_manager.mark_dirty(page_id)
                return page_id, slot_id 
            finally:
                self.buffer_manager.unpin(page_id)

        page_id, buffer = self.buffer_manager.create_page("heap")
        self.heap_pages.append(page_id)

        raw = serialize_record(record_bytes, self.schema)
        slot_id = insert_record(buffer, raw)

        self.buffer_manager.mark_dirty(page_id)
        self.buffer_manager.unpin(page_id)

        return page_id, slot_id

    def scan(self):
        print("HEAP MANAGER: scan")
        for page_id in self.heap_pages:
            buffer = self.buffer_manager.get_page(page_id)
            try:
                for raw in iter_records(buffer):
                    yield deserialize_record(raw, self.schema)
            finally:
                self.buffer_manager.unpin(page_id)
