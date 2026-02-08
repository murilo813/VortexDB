# Só pede página
from core.page import insert_record, iter_records, serialize_record, deserialize_record


class HeapManager:
    def __init__(self, buffer_manager, schema):
        self.buffer_manager = buffer_manager
        self.schema = schema
        self.heap_pages = []  # lista de page_ids

    def insert(self, record_dict):
        raw = serialize_record(record_dict, self.schema)

        if self.heap_pages:
            last_page_id = self.heap_pages[-1]
            buffer = self.buffer_manager.get_page(last_page_id)
            try:
                slot_id = insert_record(buffer, raw)
                self.buffer_manager.mark_dirty(last_page_id)
                return last_page_id, slot_id
            except ValueError:
                pass
            finally:
                self.buffer_manager.unpin(last_page_id)

        page_id, buffer = self.buffer_manager.create_page("heap")
        self.heap_pages.append(page_id)

        slot_id = insert_record(buffer, raw)
        self.buffer_manager.mark_dirty(page_id)
        self.buffer_manager.unpin(page_id)

        return page_id, slot_id

    def scan(self):
        for page_id in self.heap_pages:
            buffer = self.buffer_manager.get_page(page_id)
            try:
                for raw in iter_records(buffer):
                    yield deserialize_record(raw, self.schema)
            finally:
                self.buffer_manager.unpin(page_id)
