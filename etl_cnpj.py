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
import zipfile
import io
import csv
import os
import re
import tempfile
import time
import threading
import queue
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

# =============================================================================
# Configurações de Filtro
# =============================================================================
# Prefixos de CNAE aceitos (4 dígitos — match por startswith)
CNAES_ALVO = {
    "5320",  # Transporte rodoviário de encomendas
    "5611",  # Restaurantes e similares
    "9521",  # Reparação de equipamentos eletroeletrônicos
    "9512",  # Reparação de equipamentos de informática
    "9511",  # Reparação de computadores e periféricos
    "4635",  # Comércio atacadista de bebidas
    "4784",  # Comércio varejista de artigos de caça, pesca e camping
    "4723",  # Comércio varejista de bebidas
    "9602",  # Cabeleireiros, manicure e pedicure
    "4724",  # Comércio varejista de hortifrutigranjeiros
    "9529",  # Reparação e manutenção de outros objetos pessoais
    "4530",  # Comércio de peças e acessórios para veículos
}

SITUACAO_ATIVA = "02"        # 02 = Ativa
PORTES_ALVO   = {"00", "01"} # 00 = Não informado, 01 = Micro Empresa

# Estados de interesse
UFS_ALVO = {"CE", "PE", "PB", "BA", "DF", "GO", "MG", "ES", "RJ", "SP", "PR", "AM", "SC", "RS"}

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
# Conexão Turso via HTTP (mais estável que libsql_client WebSocket)
# =============================================================================

class TursoClient:
    """Cliente HTTP simples para Turso/libsql."""

    def __init__(self, url: str, token: str):
        # Converte libsql:// → https://
        self.base_url = url.replace("libsql://", "https://") + "/v2/pipeline"
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    def _post(self, requests_payload: list) -> list:
        body = {"requests": requests_payload}
        r = requests.post(self.base_url, headers=self.headers, json=body, timeout=30)
        r.raise_for_status()
        return r.json()["results"]

    def execute(self, sql: str, params: list = None):
        req = {"type": "execute", "stmt": {"sql": sql}}
        if params:
            req["stmt"]["args"] = [{"type": "text", "value": str(p)} for p in params]
        results = self._post([req, {"type": "close"}])
        return results[0]

    def batch(self, statements: list[tuple]):
        """statements: lista de (sql, params)"""
        reqs = []
        for sql, params in statements:
            req = {"type": "execute", "stmt": {"sql": sql}}
            if params:
                req["stmt"]["args"] = [
                    {"type": "null"} if p is None else {"type": "text", "value": str(p)}
                    for p in params
                ]
            reqs.append(req)
        reqs.append({"type": "close"})
        self._post(reqs)

    def close(self):
        pass  # HTTP é stateless, nada a fechar


def _get_turso_client() -> TursoClient:
    return TursoClient(
        url=os.environ["TURSO_URL"],
        token=os.environ["TURSO_TOKEN"],
    )

# =============================================================================
# Descoberta automática de token e período
# =============================================================================

def obter_token_raiz():
    r = requests.get(NEXTCLOUD_BASE, headers=HEADERS, timeout=15, allow_redirects=True)
    r.raise_for_status()
    # Tenta extrair token da URL final (após redirect) ou do HTML
    match = re.search(r'/s/([A-Za-z0-9]{10,25})', r.url)
    if not match:
        match = re.search(r'og:url.*?/s/([A-Za-z0-9]{10,25})', r.text)
    if not match:
        match = re.search(r'/s/([A-Za-z0-9]{10,25})', r.text)
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
        if len(row) < 30: continue
        situacao = row[5].strip()
        uf       = row[19].strip()
        cnae_pri = row[11].strip()
        cnae_sec = row[12].strip()
        if situacao != SITUACAO_ATIVA: continue
        if uf not in UFS_ALVO: continue
        # Aceita se CNAE principal ou algum secundário começa com qualquer prefixo alvo
        cnae_match = any(cnae_pri.startswith(c) for c in CNAES_ALVO)
        if not cnae_match:
            cnae_match = any(
                sec.startswith(c)
                for sec in cnae_sec.split(",")
                for c in CNAES_ALVO
            )
        if not cnae_match: continue
        # Telefones: 3 pares DDD+número nos índices 21-26
        ddd1, tel1 = row[21].strip(), row[22].strip()
        ddd2, tel2 = row[23].strip(), row[24].strip()
        ddd3, tel3 = row[25].strip(), row[26].strip()
        fones = [
            (ddd + tel) for ddd, tel in [(ddd1, tel1), (ddd2, tel2), (ddd3, tel3)]
            if ddd or tel
        ]
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
            "telefone_1": fones[0] if len(fones) > 0 else "",
            "telefone_2": fones[1] if len(fones) > 1 else "",
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
    total = 0

    # Criar tabela e limpar dados antigos
    client.execute("""
        CREATE TABLE IF NOT EXISTS empresas_alvo (
            cnpj_basico TEXT, cnpj_ordem TEXT, cnpj_dv TEXT,
            razao_social TEXT, nome_fantasia TEXT, porte TEXT,
            cnae_principal TEXT, cnae_secundaria TEXT,
            endereco TEXT, bairro TEXT, cep TEXT, uf TEXT, municipio TEXT,
            telefone_1 TEXT, telefone_2 TEXT, email TEXT
        )
    """)
    client.execute("DELETE FROM empresas_alvo")

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
                stmts.append((
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
