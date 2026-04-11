"""
etl_cnpj.py
===========
ETL mensal — baixa dados da Receita Federal e grava na tabela
`empresas_alvo` no Turso (libsql remoto).

Variáveis de ambiente necessárias:
    TURSO_URL    — ex: libsql://atlas-leads-xxx.turso.io
    TURSO_TOKEN  — token de acesso ao banco
"""

import requests
import polars as pl
import zipfile
import io
import csv
import os
import re
import tempfile
import time
import threading
import queue
from concurrent.futures import ThreadPoolExecutor, as_completed
import libsql_client

# =============================================================================
# Configurações de Filtro
# =============================================================================
CNAE_ALVO     = "5320"       # Transporte rodoviário de encomendas
SITUACAO_ATIVA = "02"        # 02 = Ativa
PORTES_ALVO   = {"00", "01"} # 00 = Não informado, 01 = Micro Empresa

NEXTCLOUD_BASE = "https://arquivos.receitafederal.gov.br"
CNPJ_PATH      = "Dados/Cadastros/CNPJ"
NUM_PARTES     = 10
MAX_WORKERS    = 2
MAX_RETRIES    = 3

_print_lock = threading.Lock()

def log(msg):
    with _print_lock:
        print(msg, flush=True)

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

PROPFIND_BODY = '''<?xml version="1.0" encoding="utf-8" ?>
<d:propfind xmlns:d="DAV:"><d:prop><d:displayname/></d:prop></d:propfind>'''

# =============================================================================
# Conexão Turso
# =============================================================================

def _get_turso_client():
    return libsql_client.create_client_sync(
        url=os.environ["TURSO_URL"],
        auth_token=os.environ["TURSO_TOKEN"],
    )

# =============================================================================
# Descoberta automática de token e período
# =============================================================================

def obter_token_raiz():
    r = requests.get(NEXTCLOUD_BASE, headers=HEADERS, timeout=15)
    r.raise_for_status()
    match = re.search(r'og:url.*?/s/([A-Za-z0-9]{10,25})', r.text)
    if match:
        token = match.group(1)
        print(f"Token raiz encontrado: {token}")
        return token
    raise RuntimeError(f"Token não encontrado em {NEXTCLOUD_BASE}.")

def obter_periodo_mais_recente(token):
    dav_url = f"{NEXTCLOUD_BASE}/public.php/dav/files/{token}/{CNPJ_PATH}/"
    r = requests.request(
        "PROPFIND", dav_url,
        data=PROPFIND_BODY,
        headers={**HEADERS, 'Depth': '1', 'Content-Type': 'application/xml'},
        timeout=15,
    )
    r.raise_for_status()
    periodos = sorted(re.findall(r'/(\d{4}-\d{2})/', r.text))
    if not periodos:
        raise RuntimeError("Nenhum período encontrado via WebDAV.")
    periodo = periodos[-1]
    print(f"Período mais recente disponível: {periodo}")
    return periodo

# =============================================================================
# Streaming
# =============================================================================

def stream_csv_do_zip(token, period, tipo_arquivo, num_parte):
    url = f"{NEXTCLOUD_BASE}/public.php/dav/files/{token}/{CNPJ_PATH}/{period}/{tipo_arquivo}{num_parte}.zip"
    for tentativa in range(1, MAX_RETRIES + 1):
        try:
            log(f"  [{tipo_arquivo}{num_parte}] Baixando... (tentativa {tentativa}/{MAX_RETRIES})")
            with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp:
                tmp_path = tmp.name
                r = requests.get(url, headers=HEADERS, timeout=600, stream=True)
                r.raise_for_status()
                total = 0
                for chunk in r.iter_content(chunk_size=4 * 1024 * 1024):
                    tmp.write(chunk)
                    total += len(chunk)
            log(f"  [{tipo_arquivo}{num_parte}] {total / 1024 / 1024:.0f} MB — processando...")
            break
        except Exception as e:
            try: os.unlink(tmp_path)
            except: pass
            if tentativa == MAX_RETRIES:
                raise
            wait = 2 ** tentativa
            log(f"  [{tipo_arquivo}{num_parte}] Erro: {e}. Aguardando {wait}s...")
            time.sleep(wait)
    try:
        with zipfile.ZipFile(tmp_path) as z:
            with z.open(z.namelist()[0]) as f:
                reader = csv.reader(io.TextIOWrapper(f, encoding="latin-1"), delimiter=";")
                yield from reader
    finally:
        os.unlink(tmp_path)

def filtrar_empresas(token, period, num_parte):
    rows = []
    for row in stream_csv_do_zip(token, period, "Empresas", num_parte):
        if len(row) < 6: continue
        porte = row[5].strip()
        if porte in PORTES_ALVO:
            rows.append({"cnpj_basico": row[0].strip(), "razao_social": row[1].strip(), "porte": porte})
    return rows

def filtrar_estabelecimentos(token, period, num_parte):
    rows = []
    for row in stream_csv_do_zip(token, period, "Estabelecimentos", num_parte):
        if len(row) < 28: continue
        situacao = row[5].strip()
        cnae_pri = row[11].strip()
        cnae_sec = row[12].strip()
        if situacao != SITUACAO_ATIVA: continue
        if not (cnae_pri.startswith(CNAE_ALVO) or CNAE_ALVO in cnae_sec): continue
        ddd1, tel1 = row[21].strip(), row[22].strip()
        ddd2, tel2 = row[23].strip(), row[24].strip()
        rows.append({
            "cnpj_basico":    row[0].strip(),
            "cnpj_ordem":     row[1].strip(),
            "cnpj_dv":        row[2].strip(),
            "nome_fantasia":  row[4].strip(),
            "cnae_principal": cnae_pri,
            "cnae_secundaria": cnae_sec,
            "endereco": " ".join(filter(None, [row[13].strip(), row[14].strip(), row[15].strip(), row[16].strip()])),
            "bairro":    row[17].strip(),
            "cep":       row[18].strip(),
            "uf":        row[19].strip(),
            "municipio": row[20].strip(),
            "telefone_1": (ddd1 + tel1) if (ddd1 or tel1) else "",
            "telefone_2": (ddd2 + tel2) if (ddd2 or tel2) else "",
            "email":     row[27].strip(),
        })
    return rows

# =============================================================================
# Workers
# =============================================================================

_SENTINEL = None

def _processar_parte_empresas(token, period, i):
    rows = filtrar_empresas(token, period, i)
    log(f"  [Empresas{i}] {len(rows)} registros filtrados")
    return i, rows

def _processar_parte_estab(token, period, empresas, i, write_queue):
    rows = filtrar_estabelecimentos(token, period, i)
    matched = [
        {**estab, **empresa}
        for estab in rows
        if (empresa := empresas.get(estab["cnpj_basico"]))
    ]
    log(f"  [Estabelecimentos{i}] {len(rows)} filtrados → {len(matched)} após join")
    if matched:
        write_queue.put(matched)

def _thread_escritora(write_queue):
    """Consome a fila e insere em batches no Turso."""
    client = _get_turso_client()
    tabela_criada = False
    total = 0

    # Criar tabela se não existir
    client.execute("""
        CREATE TABLE IF NOT EXISTS empresas_alvo (
            cnpj_basico TEXT, cnpj_ordem TEXT, cnpj_dv TEXT,
            razao_social TEXT, nome_fantasia TEXT, porte TEXT,
            cnae_principal TEXT, cnae_secundaria TEXT,
            endereco TEXT, bairro TEXT, cep TEXT, uf TEXT, municipio TEXT,
            telefone_1 TEXT, telefone_2 TEXT, email TEXT
        )
    """)

    if not tabela_criada:
        # Limpar dados antigos antes de inserir novos
        client.execute("DELETE FROM empresas_alvo")
        tabela_criada = True

    while True:
        batch = write_queue.get()
        if batch is _SENTINEL:
            break

        # Inserir em lotes de 100 (limite do Turso por statement)
        chunk_size = 100
        for i in range(0, len(batch), chunk_size):
            chunk = batch[i:i + chunk_size]
            stmts = []
            for r in chunk:
                stmts.append(libsql_client.Statement(
                    """INSERT INTO empresas_alvo
                       (cnpj_basico, cnpj_ordem, cnpj_dv, razao_social, nome_fantasia, porte,
                        cnae_principal, cnae_secundaria, endereco, bairro, cep, uf, municipio,
                        telefone_1, telefone_2, email)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    [
                        r.get("cnpj_basico",""), r.get("cnpj_ordem",""), r.get("cnpj_dv",""),
                        r.get("razao_social",""), r.get("nome_fantasia",""), r.get("porte",""),
                        r.get("cnae_principal",""), r.get("cnae_secundaria",""),
                        r.get("endereco",""), r.get("bairro",""), r.get("cep",""),
                        r.get("uf",""), r.get("municipio",""),
                        r.get("telefone_1",""), r.get("telefone_2",""), r.get("email",""),
                    ]
                ))
            client.batch(stmts)

        total += len(batch)
        log(f"  [Turso] {len(batch)} registros gravados | total: {total}")
        write_queue.task_done()

    client.close()
    log(f"  [Turso] Escrita concluída — {total} registros no total")

# =============================================================================
# Main
# =============================================================================

def main():
    token  = obter_token_raiz()
    period = obter_periodo_mais_recente(token)

    print(f"\nProcessando Empresas (0-{NUM_PARTES-1}) com {MAX_WORKERS} workers...")
    empresas = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_processar_parte_empresas, token, period, i): i for i in range(NUM_PARTES)}
        for future in as_completed(futures):
            try:
                _, rows = future.result()
                for r in rows:
                    empresas[r["cnpj_basico"]] = {"razao_social": r["razao_social"], "porte": r["porte"]}
            except Exception as e:
                log(f"  Erro Empresas{futures[future]}: {e}")

    if not empresas:
        print("Nenhum dado de Empresas processado. Abortando.")
        return
    print(f"  Total Empresas no lookup: {len(empresas)}")

    print(f"\nProcessando Estabelecimentos (0-{NUM_PARTES-1}) com {MAX_WORKERS} workers...")
    write_queue = queue.Queue(maxsize=4)
    writer = threading.Thread(target=_thread_escritora, args=(write_queue,), daemon=True)
    writer.start()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(_processar_parte_estab, token, period, empresas, i, write_queue): i
            for i in range(NUM_PARTES)
        }
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                log(f"  Erro Estabelecimentos{futures[future]}: {e}")

    write_queue.put(_SENTINEL)
    writer.join()
    print("Sucesso!")

if __name__ == "__main__":
    main()
