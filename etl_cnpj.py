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
from concurrent.futures import ThreadPoolExecutor, as_completed
import libsql

# =============================================================================
# Configurações de Filtro
# =============================================================================
CNAE_ALVO = "5320"       # Transporte rodoviário de encomendas
SITUACAO_ATIVA = "02"    # 02 = Ativa
PORTES_ALVO = {"00", "01"}  # 00 = Não informado, 01 = Micro Empresa

NEXTCLOUD_BASE = "https://arquivos.receitafederal.gov.br"
CNPJ_PATH = "Dados/Cadastros/CNPJ"
NUM_PARTES = 10   # Empresas0..9 e Estabelecimentos0..9
MAX_WORKERS = 2   # Servidor da RF não suporta muitas conexões simultâneas
MAX_RETRIES = 3   # Tentativas por arquivo em caso de timeout

_print_lock = threading.Lock()

def log(msg):
    """Print thread-safe."""
    with _print_lock:
        print(msg, flush=True)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}
PROPFIND_BODY = '''<?xml version="1.0" encoding="utf-8" ?>
<d:propfind xmlns:d="DAV:"><d:prop><d:displayname/></d:prop></d:propfind>'''

# =============================================================================
# Layout oficial dos arquivos (verificado nos dados reais de 2026-03)
# =============================================================================
# EMPRESAS — 7 colunas, separador ";", sem header, encoding latin-1
# 0  cnpj_basico
# 1  razao_social
# 2  natureza_juridica
# 3  qualificacao_responsavel
# 4  capital_social
# 5  porte  (00=Não inf, 01=ME, 03=EPP, 05=Demais)
# 6  ente_federativo_responsavel

# ESTABELECIMENTOS — 30 colunas, separador ";", sem header, encoding latin-1
# 0  cnpj_basico           1  cnpj_ordem            2  cnpj_dv
# 3  id_matriz_filial       4  nome_fantasia          5  situacao_cadastral
# 6  data_situacao          7  motivo_situacao        8  nome_cidade_exterior
# 9  pais                  10  data_inicio_atividade 11  cnae_principal
# 12 cnae_secundaria       13  tipo_logradouro       14  logradouro
# 15 numero                16  complemento           17  bairro
# 18 cep                   19  uf                    20  municipio
# 21 ddd_1                 22  telefone_1            23  ddd_2
# 24 telefone_2            25  ddd_fax               26  fax
# 27 email                 28  situacao_especial     29  data_situacao_especial

# =============================================================================
# Descoberta automática de token e período
# =============================================================================

def obter_token_raiz():
    """
    Obtém o token do compartilhamento raiz da Receita Federal via og:url.
    Esse token é estável e exposto publicamente na página do Nextcloud.
    """
    r = requests.get(NEXTCLOUD_BASE, headers=HEADERS, timeout=15)
    r.raise_for_status()
    match = re.search(r'og:url.*?/s/([A-Za-z0-9]{10,25})', r.text)
    if match:
        token = match.group(1)
        print(f"Token raiz encontrado: {token}")
        return token
    raise RuntimeError(
        f"Token não encontrado em {NEXTCLOUD_BASE}. Verifique se o servidor está acessível."
    )

def obter_periodo_mais_recente(token):
    """Lista os períodos disponíveis via WebDAV e retorna o mais recente."""
    dav_url = f"{NEXTCLOUD_BASE}/public.php/dav/files/{token}/{CNPJ_PATH}/"
    r = requests.request(
        "PROPFIND", dav_url,
        data=PROPFIND_BODY,
        headers={**HEADERS, 'Depth': '1', 'Content-Type': 'application/xml'},
        timeout=15
    )
    r.raise_for_status()
    periodos = sorted(re.findall(r'/(\d{4}-\d{2})/', r.text))
    if not periodos:
        raise RuntimeError("Nenhum período encontrado via WebDAV.")
    periodo = periodos[-1]
    print(f"Período mais recente disponível: {periodo}")
    return periodo

# =============================================================================
# Streaming: download + extração + filtragem sem carregar tudo na RAM
# =============================================================================

def stream_csv_do_zip(token, period, tipo_arquivo, num_parte):
    """
    Faz download do zip em streaming, salva em arquivo temporário no disco
    e abre o CSV interno para leitura linha a linha.
    Usa arquivo temporário para evitar acumular GBs na RAM.
    """
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
            break  # sucesso, sai do loop de retry
        except Exception as e:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass
            if tentativa == MAX_RETRIES:
                raise
            wait = 2 ** tentativa  # backoff: 2s, 4s, 8s
            log(f"  [{tipo_arquivo}{num_parte}] Erro: {e}. Aguardando {wait}s...")
            time.sleep(wait)

    try:
        with zipfile.ZipFile(tmp_path) as z:
            with z.open(z.namelist()[0]) as f:
                # Lê linha a linha sem carregar o CSV inteiro na memória
                reader = csv.reader(
                    io.TextIOWrapper(f, encoding="latin-1"),
                    delimiter=";"
                )
                yield from reader
    finally:
        os.unlink(tmp_path)

def filtrar_empresas(token, period, num_parte):
    """Filtra uma parte do arquivo Empresas e retorna lista de dicts."""
    rows = []
    for row in stream_csv_do_zip(token, period, "Empresas", num_parte):
        if len(row) < 6:
            continue
        porte = row[5].strip()
        if porte in PORTES_ALVO:
            rows.append({
                "cnpj_basico": row[0].strip(),
                "razao_social": row[1].strip(),
                "porte": porte,
            })
    return rows

def filtrar_estabelecimentos(token, period, num_parte):
    """Filtra uma parte do arquivo Estabelecimentos e retorna lista de dicts."""
    rows = []
    for row in stream_csv_do_zip(token, period, "Estabelecimentos", num_parte):
        if len(row) < 28:
            continue
        situacao = row[5].strip()
        cnae_pri = row[11].strip()
        cnae_sec = row[12].strip()
        if situacao != SITUACAO_ATIVA:
            continue
        if not (cnae_pri.startswith(CNAE_ALVO) or CNAE_ALVO in cnae_sec):
            continue
        ddd1, tel1 = row[21].strip(), row[22].strip()
        ddd2, tel2 = row[23].strip(), row[24].strip()
        rows.append({
            "cnpj_basico":   row[0].strip(),
            "cnpj_ordem":    row[1].strip(),
            "cnpj_dv":       row[2].strip(),
            "nome_fantasia": row[4].strip(),
            "cnae_principal":  cnae_pri,
            "cnae_secundaria": cnae_sec,
            "endereco": " ".join(filter(None, [
                row[13].strip(), row[14].strip(),
                row[15].strip(), row[16].strip(),
            ])),
            "bairro": row[17].strip(),
            "cep":    row[18].strip(),
            "uf":     row[19].strip(),
            "telefone_1": (ddd1 + tel1) if (ddd1 or tel1) else "",
            "telefone_2": (ddd2 + tel2) if (ddd2 or tel2) else "",
            "email":  row[27].strip(),
        })
    return rows

# =============================================================================
# Main
# =============================================================================

_SENTINEL = None  # sinal de fim para a thread escritora

def _processar_parte_empresas(token, period, i):
    """Worker: baixa + filtra uma parte de Empresas. Retorna (i, rows)."""
    rows = filtrar_empresas(token, period, i)
    log(f"  [Empresas{i}] {len(rows)} registros filtrados")
    return i, rows

def _processar_parte_estab(token, period, empresas, i, write_queue):
    """
    Worker: baixa + filtra uma parte de Estabelecimentos,
    faz o join com o dict de empresas e envia os resultados para a fila.
    """
    rows = filtrar_estabelecimentos(token, period, i)
    matched = [
        {**estab, **empresa}
        for estab in rows
        if (empresa := empresas.get(estab["cnpj_basico"]))
    ]
    log(f"  [Estabelecimentos{i}] {len(rows)} filtrados → {len(matched)} após join")
    if matched:
        write_queue.put(matched)

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS empresas_alvo (
    cnpj_basico      TEXT,
    cnpj_ordem       TEXT,
    cnpj_dv          TEXT,
    razao_social     TEXT,
    nome_fantasia    TEXT,
    porte            TEXT,
    cnae_principal   TEXT,
    cnae_secundaria  TEXT,
    endereco         TEXT,
    bairro           TEXT,
    cep              TEXT,
    uf               TEXT,
    telefone_1       TEXT,
    telefone_2       TEXT,
    email            TEXT,
    contactada       INTEGER DEFAULT 0,
    PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv)
)
"""

_UPSERT = """
INSERT INTO empresas_alvo
    (cnpj_basico, cnpj_ordem, cnpj_dv, razao_social, nome_fantasia, porte,
     cnae_principal, cnae_secundaria, endereco, bairro, cep, uf,
     telefone_1, telefone_2, email)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(cnpj_basico, cnpj_ordem, cnpj_dv) DO UPDATE SET
    razao_social    = excluded.razao_social,
    nome_fantasia   = excluded.nome_fantasia,
    porte           = excluded.porte,
    cnae_principal  = excluded.cnae_principal,
    cnae_secundaria = excluded.cnae_secundaria,
    endereco        = excluded.endereco,
    bairro          = excluded.bairro,
    cep             = excluded.cep,
    uf              = excluded.uf,
    telefone_1      = excluded.telefone_1,
    telefone_2      = excluded.telefone_2,
    email           = excluded.email
"""


def _get_turso_conn():
    return libsql.connect(
        ":memory:",
        sync_url=os.environ["TURSO_URL"],
        auth_token=os.environ["TURSO_TOKEN"],
    )


def _thread_escritora(write_queue):
    """
    Thread dedicada à escrita no Turso.
    Consome a fila e insere em batches — uma thread só, sem conflito de lock.
    """
    conn = _get_turso_conn()
    conn.sync()
    conn.execute(_CREATE_TABLE)
    conn.commit()
    conn.sync()

    total = 0

    while True:
        batch = write_queue.get()
        if batch is _SENTINEL:
            break
        for row in batch:
            conn.execute(_UPSERT, (
                row["cnpj_basico"], row["cnpj_ordem"], row["cnpj_dv"],
                row["razao_social"], row["nome_fantasia"], row["porte"],
                row["cnae_principal"], row["cnae_secundaria"],
                row["endereco"], row["bairro"], row["cep"], row["uf"],
                row["telefone_1"], row["telefone_2"], row["email"],
            ))
        conn.commit()
        conn.sync()
        total += len(batch)
        log(f"  [DB] {len(batch)} registros gravados | total: {total}")
        write_queue.task_done()

    log(f"  [DB] Escrita concluída — {total} registros no total")

def main():
    token = obter_token_raiz()
    period = obter_periodo_mais_recente(token)

    # --- Fase 1: Empresas em paralelo → monta dict de lookup ---
    print(f"\nProcessando Empresas (0-{NUM_PARTES-1}) com {MAX_WORKERS} workers...")
    empresas = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(_processar_parte_empresas, token, period, i): i
            for i in range(NUM_PARTES)
        }
        for future in as_completed(futures):
            try:
                _, rows = future.result()
                for r in rows:
                    empresas[r["cnpj_basico"]] = {
                        "razao_social": r["razao_social"],
                        "porte": r["porte"],
                    }
            except Exception as e:
                log(f"  Erro Empresas{futures[future]}: {e}")

    if not empresas:
        print("Nenhum dado de Empresas processado. Abortando.")
        return
    print(f"  Total Empresas no lookup: {len(empresas)}")

    # --- Fase 2: Estabelecimentos em paralelo + escrita contínua no DB ---
    # Workers processam e jogam na fila; thread escritora consome e grava.
    print(f"\nProcessando Estabelecimentos (0-{NUM_PARTES-1}) com {MAX_WORKERS} workers...")

    db_path = "data/prospeccao.db"
    write_queue = queue.Queue(maxsize=4)  # limita batches pendentes na fila

    writer = threading.Thread(
        target=_thread_escritora, args=(write_queue,), daemon=True
    )
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

    # Sinaliza fim para a thread escritora e aguarda ela terminar
    write_queue.put(_SENTINEL)
    writer.join()
    print("Sucesso!")

if __name__ == "__main__":
    main()
