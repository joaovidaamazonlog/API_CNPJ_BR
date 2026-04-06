from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import sqlite3
import os

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://joaovidaamazonlog.github.io"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type"],
)

def _db_path():
    p = os.path.join(os.getcwd(), "data", "prospeccao.db")
    if os.path.exists(p):
        return p
    return os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "prospeccao.db")

class BuscarCepsRequest(BaseModel):
    ceps: list[str]

@app.get("/api")
def status():
    return {"status": "API de Prospecção Ativa", "versao": "1.0"}

@app.post("/api/buscar")
def buscar_por_ceps(body: BuscarCepsRequest):
    db_path = _db_path()
    if not os.path.exists(db_path):
        raise HTTPException(status_code=500, detail=f"Banco de dados não encontrado em {db_path}.")

    ceps_limpos = list({cep.replace("-", "").strip() for cep in body.ceps if cep.strip()})

    if not ceps_limpos:
        raise HTTPException(status_code=422, detail="Nenhum CEP válido informado.")

    placeholders = ",".join("?" * len(ceps_limpos))
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM empresas_alvo WHERE cep IN ({placeholders})", ceps_limpos)
    resultados = cursor.fetchall()
    conn.close()

    return {
        "total": len(resultados),
        "empresas": [dict(row) for row in resultados],
    }

handler = app
