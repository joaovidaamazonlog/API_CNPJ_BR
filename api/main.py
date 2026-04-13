"""
api/main.py
============
API de Prospecção — usa Turso HTTP API diretamente via httpx.
"""

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import httpx
import os

app = FastAPI()

ALLOWED_ORIGIN = "https://joaovidaamazonlog.github.io"

app.add_middleware(
    CORSMiddleware,
    allow_origins=[ALLOWED_ORIGIN],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type"],
)

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"detail": str(exc)},
        headers={"Access-Control-Allow-Origin": ALLOWED_ORIGIN},
    )

# ---------------------------------------------------------------------------
# Turso HTTP client
# ---------------------------------------------------------------------------

def _turso_url():
    return os.environ["TURSO_URL"].replace("libsql://", "https://")

def _turso_token():
    return os.environ["TURSO_TOKEN"]

def _arg(v):
    if v is None:
        return {"type": "null"}
    if isinstance(v, (int, float)):
        return {"type": "float", "value": v}
    return {"type": "text", "value": str(v)}

def turso_execute(sql: str, args: list = []) -> list[dict]:
    """Executa uma query no Turso e retorna lista de dicts. Retorna [] em caso de erro."""
    payload = {
        "requests": [
            {"type": "execute", "stmt": {"sql": sql, "args": [_arg(a) for a in args]}},
            {"type": "close"},
        ]
    }

    with httpx.Client(timeout=15) as client:
        res = client.post(
            f"{_turso_url()}/v2/pipeline",
            json=payload,
            headers={
                "Authorization": f"Bearer {_turso_token()}",
                "Content-Type": "application/json",
            },
        )

    if res.status_code != 200:
        raise Exception(f"Turso HTTP {res.status_code}: {res.text[:200]}")

    data   = res.json()
    result = data["results"][0]

    # Erro na query (ex: tabela não existe) — retorna lista vazia
    if result.get("type") == "error":
        return []

    inner = result.get("response", {}).get("result", {})
    cols  = [c["name"] for c in inner.get("cols", [])]
    rows  = inner.get("rows", [])

    return [
        {
            cols[i]: (cell.get("value") if cell.get("type") != "null" else None)
            for i, cell in enumerate(row)
        }
        for row in rows
    ]

# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class BuscarEmpresasRequest(BaseModel):
    ceps:         list[str]
    territory_id: str | None = None

class ContactadaRequest(BaseModel):
    lead_key:   str
    lead_nome:  str = ""
    territorio: str = ""
    fonte:      str = ""
    action:     str = "add"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _limpar_ceps(ceps: list[str]) -> list[str]:
    return list({c.replace("-", "").strip() for c in ceps if c.strip()})

# ---------------------------------------------------------------------------
# Rotas
# ---------------------------------------------------------------------------

@app.get("/api")
def status():
    return {"status": "API de Prospecção Ativa", "versao": "2.0"}


@app.post("/api/empresas")
def buscar_empresas(body: BuscarEmpresasRequest):
    ceps_limpos = _limpar_ceps(body.ceps)

    # Leads contactados — retorna [] se tabela não existir ainda
    rows       = turso_execute("SELECT lead_key FROM leads_contactados")
    contactadas = {r["lead_key"] for r in rows}

    # Receita Federal — por CEP
    receita = []
    if ceps_limpos:
        placeholders = ",".join("?" * len(ceps_limpos))
        rows = turso_execute(
            f"SELECT * FROM empresas_alvo WHERE cep IN ({placeholders})",
            ceps_limpos,
        )
        for emp in rows:
            nome     = emp.get("razao_social") or emp.get("nome_fantasia") or ""
            endereco = emp.get("endereco") or ""
            emp["fonte"]      = "Receita Federal"
            emp["contactada"] = f"{nome}|{endereco}" in contactadas
            receita.append(emp)

    # Google Maps — por territory_id
    maps = []
    if body.territory_id:
        rows = turso_execute(
            "SELECT * FROM gmaps_leads WHERE territory_id = ?",
            [body.territory_id],
        )
        for emp in rows:
            key = emp.get("google_maps_link") or f"{emp.get('nome','')}|{emp.get('endereco','')}"
            emp["fonte"]      = "Google Maps"
            emp["contactada"] = key in contactadas
            maps.append(emp)

    empresas = receita + maps
    return {"total": len(empresas), "empresas": empresas}


@app.post("/api/empresas/contactada")
def toggle_contactada(body: ContactadaRequest):
    if not body.lead_key:
        raise HTTPException(status_code=422, detail="lead_key é obrigatório")

    # Criar tabela se não existir
    turso_execute("""
        CREATE TABLE IF NOT EXISTS leads_contactados (
            lead_key   TEXT PRIMARY KEY,
            lead_nome  TEXT,
            territorio TEXT,
            fonte      TEXT,
            saved_at   TEXT DEFAULT (datetime('now'))
        )
    """)

    if body.action == "remove":
        turso_execute(
            "DELETE FROM leads_contactados WHERE lead_key = ?",
            [body.lead_key],
        )
        return {"ok": True, "action": "removed"}

    turso_execute(
        """INSERT INTO leads_contactados (lead_key, lead_nome, territorio, fonte)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(lead_key) DO UPDATE SET
               lead_nome  = excluded.lead_nome,
               territorio = excluded.territorio,
               fonte      = excluded.fonte,
               saved_at   = datetime('now')""",
        [body.lead_key, body.lead_nome, body.territorio, body.fonte],
    )
    return {"ok": True, "action": "saved"}


handler = app
