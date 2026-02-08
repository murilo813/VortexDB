# Decide se vai usar cache ou disco
from core.page_manager import PageManager
from core.page import create_empty_page


class BufferManager:
    def __init__(self, page_manager, max_pages=10):
        self.page_manager = page_manager
        self.max_pages = max_pages
        self.pages = {}  # cache
        self.order = []  # controla FIFO

    def get_page(self, page_id):

        if page_id in self.pages:
            entry = self.pages[page_id]
            entry["pin"] += 1
            return entry["buffer"]

        # buffer cheio -> precisa expulsar alguem
        if len(self.pages) >= self.max_pages:
            self._evict()

        # lê do disco, coloca no buffer e marca como em uso
        buffer = self.page_manager.load_page(page_id)

        self.pages[page_id] = {"buffer": buffer, "dirty": False, "pin": 1}

        self.order.append(page_id)
        return buffer

    def create_page(self, page_type="heap"):
        if len(self.pages) >= self.max_pages:
            self._evict()

        page_id = self.page_manager.create_page(page_type)

        buffer = create_empty_page(page_type, page_id)

        self.pages[page_id] = {
            "buffer": buffer,
            "dirty": True,  # já começa suja
            "pin": 1,  # já em uso
        }
        self.order.append(page_id)

        return page_id, buffer

    def mark_dirty(self, page_id):  # marca página como modified
        self.pages[page_id]["dirty"] = True

    def unpin(self, page_id):

        entry = self.pages[page_id]

        if entry["pin"] <= 0:
            raise RuntimeError(f"Página {page_id} já está despinada")

        entry["pin"] -= 1

    # FIFO
    # remove da ram caso esteja cheia
    # dirty = suja
    def _evict(self):
        for page_id in list(self.order):
            entry = self.pages[page_id]

            if entry["pin"] > 0:
                continue

            if entry["dirty"]:
                self.page_manager.save_page(entry["buffer"], page_id)

            del self.pages[page_id]
            self.order.remove(page_id)
            return

        raise RuntimeError("Todas as páginas estão pinadas")

    def flush_all(self):  # grava tudo
        for page_id, entry in self.pages.items():
            if entry["dirty"]:
                self.page_manager.save_page(entry["buffer"], page_id)
                entry["dirty"] = False
