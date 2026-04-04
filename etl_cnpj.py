import requests
import polars as pl
import zipfile
import io
import sqlite3
import os
import re
from datetime import datetime, timedelta

# Configurações de Filtro
CNAE_ALVO = "5320"
SITUACAO_ATIVA = "02"
PORTES_ALVO = ["00", "01"]

NEXTCLOUD_BASE = "https://arquivos.receitafederal.gov.br"
# Caminho dentro do compartilhamento raiz onde ficam os ZIPs do CNPJ
CNPJ_PATH = "Dados/Cadastros/CNPJ"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36'
}

PROPFIND_BODY = '''<?xml version="1.0" encoding="utf-8" ?>
<d:propfind xmlns:d="DAV:"><d:prop><d:displayname/></d:prop></d:propfind>'''

def obter_token_raiz():
    """
    Obtém o token do compartilhamento raiz da Receita Federal.
    Ele fica exposto no og:url da página pública do Nextcloud,
    que raramente muda (é o compartilhamento principal da RF).
    """
    r = requests.get(NEXTCLOUD_BASE, headers=HEADERS, timeout=15)
    r.raise_for_status()
    match = re.search(r'og:url.*?/s/([A-Za-z0-9]{10,25})', r.text)
    if match:
        token = match.group(1)
        print(f"Token raiz encontrado: {token}")
        return token
    raise RuntimeError(
        f"Não foi possível encontrar o token raiz em {NEXTCLOUD_BASE}. "
        "Verifique se o servidor está acessível."
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

def baixar_e_filtrar(tipo_arquivo, num_parte, token, period):
    """Baixa o zip, filtra os dados em streaming e retorna um DataFrame."""
    url = f"{NEXTCLOUD_BASE}/public.php/dav/files/{token}/{CNPJ_PATH}/{period}/{tipo_arquivo}{num_parte}.zip"
    print(f"--- Iniciando processamento: {url} ---")

    try:
        response = requests.get(url, headers=HEADERS, stream=True, timeout=300)
        response.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            arquivo_csv = z.namelist()[0]
            with z.open(arquivo_csv) as f:
                if tipo_arquivo == "Estabelecimentos":
                    df = pl.read_csv(
                        f.read(),
                        separator=";",
                        has_header=False,
                        encoding="latin-1",
                        infer_schema_length=0
                    )
                    df_filtrado = df.filter(
                        (pl.col("column_6") == SITUACAO_ATIVA) &
                        ((pl.col("column_12").str.starts_with(CNAE_ALVO)) |
                         (pl.col("column_13").str.contains(CNAE_ALVO)))
                    ).select([
                        pl.col("column_1").alias("cnpj_basico"),
                        pl.col("column_19").alias("cep"),
                        pl.col("column_12").alias("cnae_principal"),
                        pl.col("column_22").alias("telefone1"),
                        pl.col("column_24").alias("telefone2"),
                        pl.col("column_28").alias("email"),
                        pl.col("column_14").alias("tipo_logradouro"),
                        pl.col("column_15").alias("logradouro"),
                        pl.col("column_16").alias("numero")
                    ])
                    return df_filtrado

                elif tipo_arquivo == "Empresas":
                    df = pl.read_csv(
                        f.read(),
                        separator=";",
                        has_header=False,
                        encoding="latin-1",
                        infer_schema_length=0
                    )
                    df_filtrado = df.filter(
                        pl.col("column_5").is_in(PORTES_ALVO)
                    ).select([
                        pl.col("column_1").alias("cnpj_basico"),
                        pl.col("column_2").alias("razao_social"),
                        pl.col("column_5").alias("porte")
                    ])
                    return df_filtrado

    except Exception as e:
        print(f"Erro ao processar {url}: {e}")
        return None

def main():
    token = obter_token_raiz()
    period = obter_periodo_mais_recente(token)

    print("Processando Empresas...")
    df_empresas = baixar_e_filtrar("Empresas", 0, token, period)

    print("Processando Estabelecimentos...")
    df_estab = baixar_e_filtrar("Estabelecimentos", 0, token, period)

    if df_empresas is not None and df_estab is not None:
        print("Cruzando dados (Join)...")
        resultado = df_estab.join(df_empresas, on="cnpj_basico", how="inner")

        resultado = resultado.with_columns(
            endereco=pl.col("tipo_logradouro") + " " + pl.col("logradouro") + ", " + pl.col("numero")
        ).drop(["tipo_logradouro", "logradouro", "numero"])

        print(f"Salvando {len(resultado)} registros no SQLite...")
        conn = sqlite3.connect('api/prospeccao.db')
        resultado.to_pandas().to_sql('empresas_alvo', conn, if_exists='replace', index=False)
        conn.close()
        print("Sucesso!")

if __name__ == "__main__":
    main()
