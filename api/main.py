from fastapi import FastAPI, HTTPException
import sqlite3
import os

app = FastAPI()

# Na Vercel, funções serverless rodam com cwd na raiz do projeto.
# Usamos caminho relativo ao cwd, com fallback para relativo ao __file__.
def _db_path():
    # Tenta relativo ao cwd (funciona na Vercel e localmente na raiz)
    p = os.path.join(os.getcwd(), "data", "prospeccao.db")
    if os.path.exists(p):
        return p
    # Fallback: relativo ao diretório do arquivo (útil em dev fora da raiz)
    return os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "prospeccao.db")

@app.get("/api")
def status():
    return {"status": "API de Prospecção Ativa", "versao": "1.0"}

@app.get("/api/buscar")
def buscar_por_cep(cep: str):
    db_path = _db_path()
    if not os.path.exists(db_path):
        raise HTTPException(status_code=500, detail=f"Banco de dados não encontrado em {db_path}.")

    cep_limpo = cep.replace("-", "").strip()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM empresas_alvo WHERE cep = ?", (cep_limpo,))
    resultados = cursor.fetchall()
    conn.close()

    return {
        "total": len(resultados),
        "empresas": [dict(row) for row in resultados],
    }

handler = app
