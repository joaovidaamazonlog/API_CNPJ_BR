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
import h3
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

class SlotRef(BaseModel):
    slot_id:  str
    h3_r9_id: str | None = None
    h3_r8_id: str | None = None
    lat:      float | None = None
    lon:      float | None = None

class BuscarEmpresasRequest(BaseModel):
    territory_id: str | None = None
    slots:        list[SlotRef] = []   # todos os slots vagos do território
    # fallback legado
    ceps:         list[str] = []

class ContactadaRequest(BaseModel):
    lead_key:   str
    lead_nome:  str = ""
    territorio: str = ""
    fonte:      str = ""
    action:     str = "add"

class PartnerRecord(BaseModel):
    salesforce_id:           str
    store_id:                str | None = None
    name:                    str = ""
    status:                  str = ""
    lat:                     float | None = None
    lon:                     float | None = None
    zip_code:                str | None = None
    city:                    str | None = None
    state:                   str | None = None
    delivery_station:        str = ""
    supply_run:              str | None = None
    radius:                  float | None = None
    capacity:                int | None = None
    bucket:                  str | None = None
    bucket_ade:              str | None = None
    jurisdiction_type:       str | None = None
    hub_delivey_initiatives: str | None = None
    HCP_rate_card:           str | None = None
    HCP_host_partner:        str | None = None
    launch_date:             str | None = None
    exited_date:             str | None = None
    telefone:                str | None = None
    owner_id:                str | None = None
    decision_status:         str | None = None
    lead_source:             str | None = None
    regiao:                  str | None = None
    decision:                str | None = None
    reason:                  str | None = None
    radius_suggestion:       float | None = None
    cap_suggestion:          int | None = None

class UpsertPartnersRequest(BaseModel):
    partners: list[PartnerRecord]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _limpar_ceps(ceps: list[str]) -> list[str]:
    return list({c.replace("-", "").strip() for c in ceps if c.strip()})

def _build_disk_union(slots: list[SlotRef]) -> tuple[dict[str, str], dict[str, str]]:
    """
    Retorna (hex_r9_to_slot_id, hex_r8_to_slot_id) — mapeamento de cada hex
    para o slot_id mais próximo (primeiro que o cobriu).
    Usa grid_disk(1) de cada slot, priorizando res9 quando disponível.
    """
    r9: dict[str, str] = {}  # h3_r9_id -> slot_id
    r8: dict[str, str] = {}  # h3_r8_id -> slot_id

    for slot in slots:
        # res9
        if slot.h3_r9_id:
            origin = slot.h3_r9_id
        elif slot.lat is not None and slot.lon is not None:
            origin = h3.latlng_to_cell(slot.lat, slot.lon, 9)
        else:
            origin = None

        if origin:
            for nb in h3.grid_disk(origin, 1):
                r9.setdefault(nb, slot.slot_id)

        # res8
        if slot.h3_r8_id:
            origin8 = slot.h3_r8_id
        elif origin:
            origin8 = h3.cell_to_parent(origin, 8)
        elif slot.lat is not None and slot.lon is not None:
            origin8 = h3.latlng_to_cell(slot.lat, slot.lon, 8)
        else:
            origin8 = None

        if origin8:
            for nb in h3.grid_disk(origin8, 1):
                r8.setdefault(nb, slot.slot_id)

    return r9, r8

# ---------------------------------------------------------------------------
# Rotas
# ---------------------------------------------------------------------------

@app.get("/api")
def status():
    return {"status": "API de Prospecção Ativa", "versao": "2.0"}


@app.post("/api/empresas")
def buscar_empresas(body: BuscarEmpresasRequest):
    # Leads contactados
    rows        = turso_execute("SELECT lead_key FROM leads_contactados")
    contactadas = {r["lead_key"] for r in rows}

    # Constrói a união dos grid_disk(1) de todos os slots vagos
    hex_r9_to_slot, hex_r8_to_slot = _build_disk_union(body.slots)
    use_r9 = bool(hex_r9_to_slot)
    use_r8 = bool(hex_r8_to_slot) and not use_r9

    # ── Receita Federal (empresas_geo) ────────────────────────────────────
    receita: list[dict] = []
    seen_cnpjs: set[str] = set()

    if use_r9:
        placeholders = ",".join("?" * len(hex_r9_to_slot))
        rows = turso_execute(
            f"SELECT * FROM empresas_geo WHERE h3_r9_id IN ({placeholders})",
            list(hex_r9_to_slot),
        )
        for emp in rows:
            cnpj = emp.get("cnpj")
            if cnpj in seen_cnpjs:
                continue
            seen_cnpjs.add(cnpj)
            nome     = emp.get("razao_social") or emp.get("nome_fantasia") or ""
            endereco = emp.get("endereco") or ""
            matched_slot = hex_r9_to_slot.get(emp.get("h3_r9_id", ""))
            emp["fonte"]       = "Receita Federal"
            emp["matched_slot"] = matched_slot
            emp["contactada"]  = f"{nome}|{endereco}" in contactadas
            receita.append(emp)

    elif use_r8:
        placeholders = ",".join("?" * len(hex_r8_to_slot))
        rows = turso_execute(
            f"SELECT * FROM empresas_geo WHERE h3_r8_id IN ({placeholders})",
            list(hex_r8_to_slot),
        )
        for emp in rows:
            cnpj = emp.get("cnpj")
            if cnpj in seen_cnpjs:
                continue
            seen_cnpjs.add(cnpj)
            nome     = emp.get("razao_social") or emp.get("nome_fantasia") or ""
            endereco = emp.get("endereco") or ""
            emp["fonte"]       = "Receita Federal"
            emp["matched_slot"] = hex_r8_to_slot.get(emp.get("h3_r8_id", ""))
            emp["contactada"]  = f"{nome}|{endereco}" in contactadas
            receita.append(emp)

    elif body.ceps:
        # fallback legado — sem h3 nos slots
        ceps_limpos = _limpar_ceps(body.ceps)
        placeholders = ",".join("?" * len(ceps_limpos))
        rows = turso_execute(
            f"SELECT * FROM empresas_alvo WHERE cep IN ({placeholders})",
            ceps_limpos,
        )
        for emp in rows:
            nome     = emp.get("razao_social") or emp.get("nome_fantasia") or ""
            endereco = emp.get("endereco") or ""
            emp["fonte"]       = "Receita Federal"
            emp["matched_slot"] = None
            emp["contactada"]  = f"{nome}|{endereco}" in contactadas
            receita.append(emp)

    # ── Google Maps (gmaps_leads) ─────────────────────────────────────────
    maps: list[dict] = []

    if use_r9:
        placeholders = ",".join("?" * len(hex_r9_to_slot))
        rows = turso_execute(
            f"SELECT * FROM gmaps_leads WHERE h3_r9_id IN ({placeholders})",
            list(hex_r9_to_slot),
        )
        for emp in rows:
            key = emp.get("google_maps_link") or f"{emp.get('nome','')}|{emp.get('endereco','')}"
            emp["fonte"]       = "Google Maps"
            emp["matched_slot"] = hex_r9_to_slot.get(emp.get("h3_r9_id", ""))
            emp["contactada"]  = key in contactadas
            maps.append(emp)

    elif use_r8:
        placeholders = ",".join("?" * len(hex_r8_to_slot))
        rows = turso_execute(
            f"SELECT * FROM gmaps_leads WHERE h3_r8_id IN ({placeholders})",
            list(hex_r8_to_slot),
        )
        for emp in rows:
            key = emp.get("google_maps_link") or f"{emp.get('nome','')}|{emp.get('endereco','')}"
            emp["fonte"]       = "Google Maps"
            emp["matched_slot"] = hex_r8_to_slot.get(emp.get("h3_r8_id", ""))
            emp["contactada"]  = key in contactadas
            maps.append(emp)

    elif body.territory_id:
        # fallback legado
        rows = turso_execute(
            "SELECT * FROM gmaps_leads WHERE territory_id = ?",
            [body.territory_id],
        )
        for emp in rows:
            key = emp.get("google_maps_link") or f"{emp.get('nome','')}|{emp.get('endereco','')}"
            emp["fonte"]       = "Google Maps"
            emp["matched_slot"] = None
            emp["contactada"]  = key in contactadas
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


# ---------------------------------------------------------------------------
# OAuth 2.0 service-to-service (para Amazon Q connector)
# ---------------------------------------------------------------------------

@app.post("/api/oauth/token")
async def oauth_token(request: Request):
    """
    Endpoint de token OAuth 2.0 client_credentials.
    Aceita tanto application/x-www-form-urlencoded quanto application/json.
    """
    content_type = request.headers.get("content-type", "")
    if "application/json" in content_type:
        body = await request.json()
        client_id     = body.get("client_id")
        client_secret = body.get("client_secret")
    else:
        form          = await request.form()
        client_id     = form.get("client_id")
        client_secret = form.get("client_secret")

    expected_id     = os.environ.get("OAUTH_CLIENT_ID")
    expected_secret = os.environ.get("OAUTH_CLIENT_SECRET")

    if client_id != expected_id or client_secret != expected_secret:
        raise HTTPException(status_code=401, detail="invalid_client")

    return {
        "access_token": _turso_token(),
        "token_type":   "Bearer",
        "expires_in":   3600,
    }


# ---------------------------------------------------------------------------
# Upsert de parceiros (chamado pelo agent / GitHub Action)
# ---------------------------------------------------------------------------

PARTNERS_DDL = """
CREATE TABLE IF NOT EXISTS partners (
    salesforce_id            TEXT PRIMARY KEY,
    store_id                 TEXT,
    name                     TEXT,
    status                   TEXT,
    lat                      REAL,
    lon                      REAL,
    zip_code                 TEXT,
    city                     TEXT,
    state                    TEXT,
    delivery_station         TEXT,
    supply_run               TEXT,
    radius                   REAL,
    capacity                 INTEGER,
    bucket                   TEXT,
    bucket_ade               TEXT,
    jurisdiction_type        TEXT,
    hub_delivey_initiatives  TEXT,
    HCP_rate_card            TEXT,
    HCP_host_partner         TEXT,
    launch_date              TEXT,
    exited_date              TEXT,
    telefone                 TEXT,
    owner_id                 TEXT,
    decision_status          TEXT,
    lead_source              TEXT,
    regiao                   TEXT,
    decision                 TEXT,
    reason                   TEXT,
    radius_suggestion        REAL,
    cap_suggestion           INTEGER,
    updated_at               TEXT DEFAULT (datetime('now'))
)
"""

UPSERT_SQL = """
INSERT OR REPLACE INTO partners (
    salesforce_id, store_id, name, status, lat, lon, zip_code, city, state,
    delivery_station, supply_run, radius, capacity, bucket, bucket_ade,
    jurisdiction_type, hub_delivey_initiatives, HCP_rate_card, HCP_host_partner,
    launch_date, exited_date, telefone, owner_id, decision_status, lead_source,
    regiao, decision, reason, radius_suggestion, cap_suggestion, updated_at
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'))
"""

def _turso_pipeline(requests_payload: list) -> dict:
    """Envia um pipeline de múltiplos statements pro Turso."""
    with httpx.Client(timeout=30) as client:
        res = client.post(
            f"{_turso_url()}/v2/pipeline",
            json={"requests": requests_payload},
            headers={
                "Authorization": f"Bearer {_turso_token()}",
                "Content-Type": "application/json",
            },
        )
    if res.status_code != 200:
        raise Exception(f"Turso HTTP {res.status_code}: {res.text[:300]}")
    return res.json()


@app.post("/api/partners/upsert")
def upsert_partners(body: UpsertPartnersRequest):
    # Garante que a tabela existe
    turso_execute(PARTNERS_DDL)

    total   = len(body.partners)
    batch   = 200
    upserted = 0

    for i in range(0, total, batch):
        chunk = body.partners[i : i + batch]
        requests_payload = [
            {
                "type": "execute",
                "stmt": {
                    "sql": UPSERT_SQL,
                    "args": [_arg(v) for v in [
                        p.salesforce_id, p.store_id, p.name, p.status,
                        p.lat, p.lon, p.zip_code, p.city, p.state,
                        p.delivery_station, p.supply_run, p.radius, p.capacity,
                        p.bucket, p.bucket_ade, p.jurisdiction_type,
                        p.hub_delivey_initiatives, p.HCP_rate_card, p.HCP_host_partner,
                        p.launch_date, p.exited_date, p.telefone, p.owner_id,
                        p.decision_status, p.lead_source, p.regiao,
                        p.decision, p.reason, p.radius_suggestion, p.cap_suggestion,
                    ]],
                },
            }
            for p in chunk
        ] + [{"type": "close"}]

        _turso_pipeline(requests_payload)
        upserted += len(chunk)

    return {"ok": True, "upserted": upserted, "total": total}


handler = app
