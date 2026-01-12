# Decide se vai usar cache ou disco
from core.page_manager import PageManager

class BufferManager():
    def __init__(self, page_manager, max_pages=10):
        self.page_manager = page_manager 
        self.max_pages = max_pages 
        self.pages = {} # cache
        self.order = [] # controla FIFO

    def get_page(self, page_id): # devolve buffer
        # se ja está no buffer
        print(f"BUFFER_MANAGER: get_page {page_id}")
        if page_id in self.pages:
            entry = self.pages[page_id]
            entry["pin"] += 1
            return entry["buffer"] 
        
        # buffer cheio -> precisa expulsar alguem
        if len(self.pages) > self.max_pages:
            self._evict()

        # lê do disco, coloca no buffer e marca como em uso
        buffer = self.page_manager.load_page(page_id)

        self.pages[page_id] = {
            "buffer": buffer,
            "dirty": False,
            "pin": 1
        }

        self.order.append(page_id)
        return buffer

    def mark_dirty(self, page_id): # marca página como modified
        print(f"BUFFER_MANAGER: mark_dirty {page_id}")
        self.pages[page_id]["dirty"] = True

    def unpin(self, page_id):
        print(f"BUFFER_MANAGER: unpin {page_id}")
        self.pages[page_id]["pin"] -= 1

    #FIFO
    # remove da ram caso esteja cheia
    # dirty = suja
    def _evict(self):
        print("BUFFER_MANAGER: _evict")
        for page_id in self.order:
            entry = self.pages[page_id]

            if entry["pin"] > 0:
                continue

            if entry["dirty"]:
                self.page_manager.save_page(entry["buffer"], page_id)

            del self.pages[page_id]
            self.order.remove(page_id)
            return

        raise RuntimeError("Todas as páginas estão pinadas")

    def flush_all(self): # grava tudo
        print("BUFFER_MANAGER: flush_Fall")
        for page_id, entry in self.pages.items():
            if entry["dirty"]:
                self.page_manager.save_page(entry["buffer"], page_id)
                entry["dirty"] = False
        