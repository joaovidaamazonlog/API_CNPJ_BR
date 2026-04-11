from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import os
import libsql

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://joaovidaamazonlog.github.io"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type"],
)

# ---------------------------------------------------------------------------
# Conexão Turso
# ---------------------------------------------------------------------------

def get_conn():
    url = os.environ["TURSO_URL"]
    token = os.environ["TURSO_TOKEN"]
    return libsql.connect(":memory:", sync_url=url, auth_token=token)


def query(sql: str, params: tuple = ()):
    conn = get_conn()
    conn.sync()
    cur = conn.execute(sql, params)
    cols = [d[0] for d in cur.description] if cur.description else []
    rows = cur.fetchall()
    return [dict(zip(cols, row)) for row in rows]


def execute(sql: str, params: tuple = ()):
    conn = get_conn()
    conn.sync()
    conn.execute(sql, params)
    conn.commit()
    conn.sync()


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class EmpresasRequest(BaseModel):
    ceps: list[str] = []
    territory_ids: list[str] = []


class ContactadaRequest(BaseModel):
    cnpj: str
    contactada: bool


# ---------------------------------------------------------------------------
# Rotas
# ---------------------------------------------------------------------------

@app.get("/api")
def status():
    return {"status": "API de Prospecção Ativa", "versao": "2.0"}


@app.post("/api/empresas")
def buscar_empresas(body: EmpresasRequest):
    ceps = list({c.replace("-", "").strip() for c in body.ceps if c.strip()})
    territory_ids = list({t.strip() for t in body.territory_ids if t.strip()})

    if not ceps and not territory_ids:
        raise HTTPException(status_code=422, detail="Informe ao menos um CEP ou territory_id.")

    resultados = []

    # Busca em empresas_alvo por CEPs
    if ceps:
        ph = ",".join("?" * len(ceps))
        rows = query(
            f"SELECT *, 0 AS fonte_gmaps FROM empresas_alvo WHERE cep IN ({ph})",
            tuple(ceps),
        )
        resultados.extend(rows)

    # Busca em gmaps_leads por territory_id
    if territory_ids:
        ph = ",".join("?" * len(territory_ids))
        rows = query(
            f"SELECT *, 1 AS fonte_gmaps FROM gmaps_leads WHERE territory_id IN ({ph})",
            tuple(territory_ids),
        )
        resultados.extend(rows)

    # Garante campo contactada em todos os registros
    for r in resultados:
        r.setdefault("contactada", 0)

    return {"total": len(resultados), "empresas": resultados}


@app.post("/api/empresas/contactada")
def toggle_contactada(body: ContactadaRequest):
    cnpj = body.cnpj.strip()
    if not cnpj:
        raise HTTPException(status_code=422, detail="CNPJ inválido.")

    val = 1 if body.contactada else 0

    # Tenta atualizar em ambas as tabelas
    execute(
        "UPDATE empresas_alvo SET contactada = ? WHERE cnpj_basico || cnpj_ordem || cnpj_dv = ?",
        (val, cnpj),
    )
    execute(
        "UPDATE gmaps_leads SET contactada = ? WHERE cnpj = ?",
        (val, cnpj),
    )

    return {"ok": True, "cnpj": cnpj, "contactada": body.contactada}


handler = app
