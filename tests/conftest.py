import pytest
import os
import shutil
from core.config import DATA_DIR


@pytest.fixture(autouse=True)
def clean_environment():
    if os.path.exists(DATA_DIR):
        for filename in os.listdir(DATA_DIR):
            file_path = os.path.join(DATA_DIR, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f"Erro ao deletar {file_path}: {e}")
    else:
        os.makedirs(DATA_DIR)
