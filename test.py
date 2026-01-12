from core.catalog import Catalog

catalog = Catalog()

catalog.add_heap_page("users", 99)

catalog2 = Catalog()
print(catalog2.get_table("users"))