# Apenas lê e escreve no disco

import os
from core.page import create_empty_page
from core.config import DATA_DIR


class PageManager:
    def __init__(self, data_dir=DATA_DIR):
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)
        self.next_page_id = self._dicover_next_page_id()

    def _dicover_next_page_id(self):
        files = os.listdir(self.data_dir)
        page_ids = []

        for name in files:
            if name.endswith(".vortex"):
                pid = int(
                    name[5:-3]
                )  # pula os 5 primeiros caracteres e elimina os ultimos 3 (page_) (.db)
                page_ids.append(pid)

        if not page_ids:
            return 0

        return max(page_ids) + 1

    def create_page(self, page_type="heap"):
        page_id = self.next_page_id

        self.next_page_id += 1
        return page_id

    def load_page(self, page_id):
        return self.read_page_from_disk(page_id)

    def save_page(self, buffer, page_id):
        self.write_page_to_disk(buffer, page_id)

    def write_page_to_disk(self, buffer, page_id):
        filename = os.path.join(DATA_DIR, f"page_{page_id}.vortex")
        with open(filename, "wb") as f:  # wb = write binary
            f.write(buffer)

    def read_page_from_disk(self, page_id):
        filename = os.path.join(DATA_DIR, f"page_{page_id}.vortex")
        with open(filename, "rb") as f:  # rb = read binary
            buffer = bytearray(f.read())
        return buffer

    # def allocate_record(record_bytes):
