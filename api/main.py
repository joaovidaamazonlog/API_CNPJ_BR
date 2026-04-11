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

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import libsql_client
import os
import asyncio

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

def _get_client():
    url   = os.environ["TURSO_URL"]
    token = os.environ["TURSO_TOKEN"]
    return libsql_client.create_client_sync(url=url, auth_token=token)

# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class BuscarEmpresasRequest(BaseModel):
    ceps:         list[str]
    territory_id: str | None = None

class ContactadaRequest(BaseModel):
    lead_key:    str          # link Maps ou "nome|endereço"
    lead_nome:   str  = ""
    territorio:  str  = ""
    fonte:       str  = ""    # "maps" | "receita"
    action:      str  = "add" # "add" | "remove"

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
    """
    Busca unificada: retorna empresas da Receita Federal (por CEP)
    e do Google Maps (por territory_id), com flag 'contactada' em cada uma.
    """
    ceps_limpos = _limpar_ceps(body.ceps)

    with _get_client() as client:
        # ── 1. Leads contactados (para aplicar flag) ──────────────────────
        rs = client.execute("SELECT lead_key FROM leads_contactados")
        contactadas = {row[0] for row in rs.rows}

        # ── 2. Receita Federal — busca por CEP ────────────────────────────
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
                key = emp.get("cnpj_basico", "") + "|" + emp.get("endereco", "")
                emp["fonte"]       = "Receita Federal"
                emp["contactada"]  = key in contactadas or _any_key_match(emp, contactadas)
                receita.append(emp)

        # ── 3. Google Maps — busca por territory_id ───────────────────────
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
    return {
        "total":    len(empresas),
        "empresas": empresas,
    }


@app.post("/api/empresas/contactada")
def toggle_contactada(body: ContactadaRequest):
    """Toggle de empresa contactada — funciona para Maps e Receita Federal."""
    if not body.lead_key:
        raise HTTPException(status_code=422, detail="lead_key é obrigatório")

    with _get_client() as client:
        # Garantir que a tabela existe
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
            return {"ok": True, "action": "removed", "lead_key": body.lead_key}
        else:
            client.execute(
                """
                INSERT INTO leads_contactados (lead_key, lead_nome, territorio, fonte)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(lead_key) DO UPDATE SET
                    lead_nome  = excluded.lead_nome,
                    territorio = excluded.territorio,
                    fonte      = excluded.fonte,
                    saved_at   = datetime('now')
                """,
                [body.lead_key, body.lead_nome, body.territorio, body.fonte],
            )
            return {"ok": True, "action": "saved", "lead_key": body.lead_key}


# ---------------------------------------------------------------------------
# Helper interno
# ---------------------------------------------------------------------------

def _any_key_match(emp: dict, contactadas: set) -> bool:
    """Tenta múltiplas chaves possíveis para empresas da Receita Federal."""
    nome     = emp.get("razao_social", "") or emp.get("nome_fantasia", "")
    endereco = emp.get("endereco", "")
    return f"{nome}|{endereco}" in contactadas


handler = app
