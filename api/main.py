"""
api/main.py
===========
API de Prospecção de Parceiros Logísticos.

Rotas
-----
GET  /api                        — status
POST /api/empresas               — busca unificada (Receita Federal + Maps) por CEPs e territory_id
POST /api/empresas/contactada    — toggle de empresa contactada (qualquer fonte)
"""

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import libsql_client
import os

app = FastAPI()

ALLOWED_ORIGIN = "https://joaovidaamazonlog.github.io"

app.add_middleware(
    CORSMiddleware,
    allow_origins=[ALLOWED_ORIGIN],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type"],
)

# Garante headers CORS mesmo em erros 500
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"detail": str(exc)},
        headers={"Access-Control-Allow-Origin": ALLOWED_ORIGIN},
    )

# ---------------------------------------------------------------------------
# Conexão Turso
# ---------------------------------------------------------------------------

def _get_client():
    url   = os.environ["TURSO_URL"].replace("libsql://", "https://")
    token = os.environ["TURSO_TOKEN"]
    return libsql_client.create_client_sync(url=url, auth_token=token)

# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class BuscarEmpresasRequest(BaseModel):
    ceps:         list[str]
    territory_id: str | None = None

class ContactadaRequest(BaseModel):
    lead_key:   str
    lead_nome:  str  = ""
    territorio: str  = ""
    fonte:      str  = ""
    action:     str  = "add"  # "add" | "remove"

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

    with _get_client() as client:
        # Leads contactados
        rs = client.execute("SELECT lead_key FROM leads_contactados")
        contactadas = {row[0] for row in rs.rows}

        # Receita Federal — por CEP
        receita = []
        if ceps_limpos:
            placeholders = ",".join("?" * len(ceps_limpos))
            rs = client.execute(
                f"SELECT * FROM empresas_alvo WHERE cep IN ({placeholders})",
                ceps_limpos,
            )
            cols = [d[0] for d in rs.columns] if rs.columns else []
            for row in rs.rows:
                emp = dict(zip(cols, row))
                nome     = emp.get("razao_social", "") or emp.get("nome_fantasia", "")
                endereco = emp.get("endereco", "")
                key      = f"{nome}|{endereco}"
                emp["fonte"]      = "Receita Federal"
                emp["contactada"] = key in contactadas
                receita.append(emp)

        # Google Maps — por territory_id
        maps = []
        if body.territory_id:
            rs = client.execute(
                "SELECT * FROM gmaps_leads WHERE territory_id = ?",
                [body.territory_id],
            )
            cols = [d[0] for d in rs.columns] if rs.columns else []
            for row in rs.rows:
                emp = dict(zip(cols, row))
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

    with _get_client() as client:
        client.execute("""
            CREATE TABLE IF NOT EXISTS leads_contactados (
                lead_key   TEXT PRIMARY KEY,
                lead_nome  TEXT,
                territorio TEXT,
                fonte      TEXT,
                saved_at   TEXT DEFAULT (datetime('now'))
            )
        """)

        if body.action == "remove":
            client.execute(
                "DELETE FROM leads_contactados WHERE lead_key = ?",
                [body.lead_key],
            )
            return {"ok": True, "action": "removed"}
        else:
            client.execute(
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
