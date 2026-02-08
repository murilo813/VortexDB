import time
import os
import platform
import pytest
from core.page_manager import PageManager
from core.buffer_manager import BufferManager
from core.catalog import Catalog
from core.table_manager import TableManager
from catalog.schema import Schema, Column, INT, TEXT

@pytest.mark.benchmark
def test_heavy_load_btree_benchmark():
    # CONFIGURAÇÕES (EXATAMENTE IGUAIS ÀS ANTERIORES)
    config = {
        "TOTAL_RECORDS": 100000,
        "MAX_PAGES_RAM": 50,
        "PAYLOAD_SIZE": 150,
        "FLUSH_INTERVAL": 1000,
        "TARGET_ID": 99999,
    }

    # Informações do Ambiente
    env_info = {
        "os": platform.system(),
        "processor": platform.processor(),
        "python_version": platform.python_version(),
    }

    # Setup do motor
    pm = PageManager()
    bm = BufferManager(pm, max_pages=config["MAX_PAGES_RAM"])
    cat = Catalog()
    tm = TableManager(bm, cat)

    schema = Schema("big_table_btree", [Column("id", INT), Column("dado", TEXT)])
    tm.create_table("big_table_btree", schema)

    # 1. MEDIÇÃO DE INSERÇÃO (Agora indexando na B-Tree automaticamente)
    print(f"\n🚀 [VORTEX] Inserindo {config['TOTAL_RECORDS']} registros com B-Tree...")
    payload_string = "A" * config["PAYLOAD_SIZE"]

    start_insert = time.time()
    for i in range(config["TOTAL_RECORDS"]):
        tm.insert("big_table_btree", {"id": i, "dado": f"{payload_string}_{i}"})

        if config["FLUSH_INTERVAL"] > 0 and i % config["FLUSH_INTERVAL"] == 0:
            bm.flush_all()

    bm.flush_all()
    end_insert = time.time()
    insert_time = end_insert - start_insert

    # 2. MEDIÇÃO DE BUSCA VIA B-TREE (Aqui o Vortex brilha)
    print(f"⚡ [VORTEX] Buscando ID {config['TARGET_ID']} via B-Tree Index...")
    start_index = time.time()
    
    # Agora usamos o select_by_id que utiliza a btree internamente
    found = tm.select_by_id("big_table_btree", config["TARGET_ID"])

    end_index = time.time()
    index_time = end_index - start_index

    # Validação de integridade
    assert found is not None, "ERRO: O registro não foi encontrado via índice!"

    # 3. RELATÓRIO TÉCNICO
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")

    # Cálculo de arquivos gerados
    data_path = pm.data_dir
    num_files = len([f for f in os.listdir(data_path) if f.endswith(".vortex")])

    report = f"""
==================================================
VORTEXDB BENCHMARK - DATA: {timestamp}
==================================================
AMBIENTE:
- SO: {env_info["os"]}
- CPU: {env_info["processor"]}
- Linguagem: Python {env_info["python_version"]}

CONFIGURAÇÕES DO TESTE:
- Registros:      {config["TOTAL_RECORDS"]}
- Payload:         {config["PAYLOAD_SIZE"]} bytes
- Buffer Pool:    {config["MAX_PAGES_RAM"]} pgs
- Flush a cada:   {config["FLUSH_INTERVAL"]} registros
- Arquivos .vortex gerados: {num_files}

RESULTADOS:
[INSERÇÃO + INDEXAÇÃO]
- Tempo Total:    {insert_time:.4f} s
- Vazão (Throughput): {int(config["TOTAL_RECORDS"] / insert_time)} reg/s

[BUSCA (PONTO ÚNICO)]
- Estratégia:      B-TREE INDEX SEEK (O(log n))
- Alvo ID:         {config["TARGET_ID"]}
- Tempo de Busca: {index_time:.4f} s

STATUS DO MOTOR: 
[X] B-Tree Index  | [X] Heap Scan Enabled
[ ] Write-Ahead Log | [ ] Rust Engine
--------------------------------------------------
"""

    # 'a' garante que vai inserir abaixo do anterior sem apagar nada
    with open("benchmark_results.txt", "a", encoding="utf-8") as f:
        f.write(report)

    print(f"✅ Concluído! Index Seek em {index_time:.4f}s. Registrado no histórico.")