"""
Scraper Google Maps → Turso
Busca leads de transportadoras por territory_id usando os dados do atlas.
Grava na tabela gmaps_leads no Turso.

Env vars necessárias:
  TURSO_URL   — libsql://...
  TURSO_TOKEN — token de autenticação
"""

import os
import time
import requests
import libsql

ATLAS_BASE = "https://joaovidaamazonlog.github.io/atlas/output_data"
GMAPS_API_KEY = os.environ.get("GMAPS_API_KEY", "")
GMAPS_NEARBY_URL = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
GMAPS_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"

SEARCH_KEYWORDS = ["transportadora", "logística", "courier", "entrega expressa"]
SEARCH_RADIUS_M = 5000  # raio em metros ao redor do centróide do território

# ---------------------------------------------------------------------------
# Turso
# ---------------------------------------------------------------------------

def get_conn():
    return libsql.connect(
        ":memory:",
        sync_url=os.environ["TURSO_URL"],
        auth_token=os.environ["TURSO_TOKEN"],
    )


def init_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gmaps_leads (
            place_id     TEXT PRIMARY KEY,
            territory_id TEXT,
            nome         TEXT,
            endereco     TEXT,
            telefone     TEXT,
            website      TEXT,
            cnpj         TEXT,
            lat          REAL,
            lon          REAL,
            rating       REAL,
            contactada   INTEGER DEFAULT 0,
            criado_em    TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    conn.sync()


def upsert_lead(conn, lead: dict):
    conn.execute("""
        INSERT INTO gmaps_leads
            (place_id, territory_id, nome, endereco, telefone, website, lat, lon, rating)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(place_id) DO UPDATE SET
            territory_id = excluded.territory_id,
            nome         = excluded.nome,
            endereco     = excluded.endereco,
            telefone     = excluded.telefone,
            website      = excluded.website,
            lat          = excluded.lat,
            lon          = excluded.lon,
            rating       = excluded.rating
    """, (
        lead["place_id"], lead["territory_id"], lead["nome"],
        lead["endereco"], lead["telefone"], lead["website"],
        lead["lat"], lead["lon"], lead["rating"],
    ))


# ---------------------------------------------------------------------------
# Atlas
# ---------------------------------------------------------------------------

def fetch_territories() -> dict:
    r = requests.get(f"{ATLAS_BASE}/territories_index.json", timeout=30)
    r.raise_for_status()
    return r.json()


# ---------------------------------------------------------------------------
# Google Maps
# ---------------------------------------------------------------------------

def nearby_search(lat: float, lon: float, keyword: str) -> list[dict]:
    """Retorna lista de places da Nearby Search (paginada até 3 páginas)."""
    places = []
    params = {
        "location": f"{lat},{lon}",
        "radius": SEARCH_RADIUS_M,
        "keyword": keyword,
        "key": GMAPS_API_KEY,
    }
    while True:
        r = requests.get(GMAPS_NEARBY_URL, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        places.extend(data.get("results", []))
        token = data.get("next_page_token")
        if not token:
            break
        time.sleep(2)  # Google exige delay antes de usar o next_page_token
        params = {"pagetoken": token, "key": GMAPS_API_KEY}
    return places


def place_details(place_id: str) -> dict:
    r = requests.get(GMAPS_DETAILS_URL, params={
        "place_id": place_id,
        "fields": "name,formatted_address,formatted_phone_number,website,geometry,rating",
        "key": GMAPS_API_KEY,
    }, timeout=15)
    r.raise_for_status()
    return r.json().get("result", {})


def scrape_territory(territory_id: str, lat: float, lon: float) -> list[dict]:
    seen = set()
    leads = []
    for kw in SEARCH_KEYWORDS:
        for place in nearby_search(lat, lon, kw):
            pid = place.get("place_id")
            if not pid or pid in seen:
                continue
            seen.add(pid)
            details = place_details(pid)
            leads.append({
                "place_id":    pid,
                "territory_id": territory_id,
                "nome":        details.get("name", place.get("name", "")),
                "endereco":    details.get("formatted_address", ""),
                "telefone":    details.get("formatted_phone_number", ""),
                "website":     details.get("website", ""),
                "lat":         details.get("geometry", {}).get("location", {}).get("lat", lat),
                "lon":         details.get("geometry", {}).get("location", {}).get("lng", lon),
                "rating":      details.get("rating", 0.0),
            })
            time.sleep(0.1)
    return leads


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if not GMAPS_API_KEY:
        raise RuntimeError("GMAPS_API_KEY não definida.")

    print("Carregando territories_index.json do atlas...")
    territories = fetch_territories()
    # Remove metadados se existirem
    territories = {k: v for k, v in territories.items() if isinstance(v, dict) and "centroid_lat" in v}
    print(f"  {len(territories)} territórios encontrados.")

    conn = get_conn()
    conn.sync()
    init_table(conn)

    total = 0
    for tid, info in territories.items():
        lat = info["centroid_lat"]
        lon = info["centroid_lon"]
        print(f"[{tid}] scraping ({lat:.4f}, {lon:.4f})...")
        try:
            leads = scrape_territory(tid, lat, lon)
            for lead in leads:
                upsert_lead(conn, lead)
            conn.commit()
            conn.sync()
            total += len(leads)
            print(f"  → {len(leads)} leads gravados")
        except Exception as e:
            print(f"  ERRO: {e}")
        time.sleep(0.5)

    print(f"\nConcluído. Total de leads: {total}")


if __name__ == "__main__":
    main()
