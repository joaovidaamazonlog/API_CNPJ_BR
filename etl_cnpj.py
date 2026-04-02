import requests
import polars as pl
import zipfile
import io
import sqlite3
import os

# Configurações de Filtro e URLs
CNAE_ALVO = "5320"
SITUACAO_ATIVA = "02"
PORTES_ALVO = ["01", "03"] # 01: ME, 03: EPP (Engloba o faturamento desejado)
BASE_URL = "https://dadosabertos.rfb.gov.br/CNPJ/"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36'
}

def baixar_e_filtrar(tipo_arquivo, num_parte):
    """Baixa o zip, filtra os dados em streaming e retorna um DataFrame."""
    url = f"{BASE_URL}{tipo_arquivo}{num_parte}.zip"
    print(f"--- Iniciando processamento: {url} ---")
    
    try:
        response = requests.get(url, headers=HEADERS, stream=True, timeout=300)
        response.raise_for_status()
        
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            arquivo_csv = z.namelist()[0]
            with z.open(arquivo_csv) as f:
                # Lendo apenas as colunas necessárias para economizar memória
                # Estabelecimentos: 0:CNPJ_BASE, 5:SITUACAO, 11:CNAE_PRI, 12:CNAE_SEC, 18:CEP, 21:TEL1, 23:TEL2, 27:EMAIL
                if tipo_arquivo == "Estabelecimentos":
                    df = pl.read_csv(
                        f.read(),
                        separator=";",
                        has_header=False,
                        encoding="latin-1",
                        infer_schema_length=0
                    )
                    
                    # Filtro: Situação Ativa E (CNAE Principal ou Secundário contendo 5320)
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

                # Empresas: 0:CNPJ_BASE, 1:RAZAO_SOCIAL, 4:PORTE
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
    # Para o MVP no GitHub Actions, vamos processar a Parte 0 de cada (mais que suficiente para teste)
    # Em um servidor maior, você faria um loop de 0 a 9.
    
    print("Processando Empresas...")
    df_empresas = baixar_e_filtrar("Empresas", 0)
    
    print("Processando Estabelecimentos...")
    df_estab = baixar_e_filtrar("Estabelecimentos", 0)
    
    if df_empresas is not None and df_estab is not None:
        print("Cruzando dados (Join)...")
        # Unindo as tabelas pelo CNPJ Básico
        resultado = df_estab.join(df_empresas, on="cnpj_basico", how="inner")
        
        # Criando o endereço completo
        resultado = resultado.with_columns(
            endereco = pl.col("tipo_logradouro") + " " + pl.col("logradouro") + ", " + pl.col("numero")
        ).drop(["tipo_logradouro", "logradouro", "numero"])

        print(f"Salvando {len(resultado)} registros no SQLite...")
        conn = sqlite3.connect('data/prospeccao.db')
        resultado.to_pandas().to_sql('empresas_alvo', conn, if_exists='replace', index=False)
        conn.close()
        print("Sucesso!")

if __name__ == "__main__":
    if not os.path.exists('data'): os.makedirs('data')
    main()