from core.page_manager import PageManager
from core.heap_manager import HeapManager 
from core.catalog import Catalog 

pm = PageManager()
catalog = Catalog()
heap = HeapManager(pm)
table = catalog.get_table("users")
heap.heap_pages = table["heap_pages"]

# try:
#     catalog.create_table("users")
# except ValueError:
#     pass

# pid, offset = heap.insert(b"alice")
# catalog.add_heap_page("users", pid)

# pid, offset = heap.insert(b"bob")
# catalog.add_heap_page("users", pid)

# print("inserido")
for rec in heap.scan():
    print(rec)