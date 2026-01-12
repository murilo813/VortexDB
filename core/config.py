import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_MAGIC = 0x53514D44
DB_VERSION = 1
PAGE_SIZE = 8192