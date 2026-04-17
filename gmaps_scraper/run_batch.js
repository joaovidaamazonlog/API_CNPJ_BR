/**
 * run_batch.js
 * ============
 * Scraping do Google Maps — grava resultados no Turso via HTTP API.
 *
 * Uso:
 *   node run_batch.js
 *   node run_batch.js --stations DSP2 DBH5
 */

const { scrapeGmaps, closeSharedBrowser } = require('./scraper');

const { latLngToCell } = require('h3-js');

// ---------------------------------------------------------------------------
// TURSO HTTP

const BUSINESS_TYPES = [
    'lanchonete',
    'açaí e sorveteria',
    'chaveiro',
    'assistência técnica',
    'disk agua e gas',
];

const ATLAS_BASE_URL    = 'https://joaovidaamazonlog.github.io/atlas/output_data';
const BATCH_CONCURRENCY = 2;    // menos workers = menos detecção pelo Google
const DELAY_MS          = 4000; // 4s entre buscas — sem pressa

// ---------------------------------------------------------------------------
// TURSO HTTP
// ---------------------------------------------------------------------------

const TURSO_URL   = process.env.TURSO_URL;
const TURSO_TOKEN = process.env.TURSO_TOKEN;

if (!TURSO_URL || !TURSO_TOKEN) {
    console.error('TURSO_URL e TURSO_TOKEN são obrigatórios');
    process.exit(1);
}

const TURSO_HTTP_URL = TURSO_URL.replace(/^libsql:\/\//, 'https://');

function _arg(v) {
    if (v === null || v === undefined) return { type: 'null' };
    if (typeof v === 'number')         return { type: 'float', value: v };
    return { type: 'text', value: String(v) };
}

async function tursoExecute(sql, args = []) {
    const res = await fetch(`${TURSO_HTTP_URL}/v2/pipeline`, {
        method:  'POST',
        headers: {
            'Authorization': `Bearer ${TURSO_TOKEN}`,
            'Content-Type':  'application/json',
        },
        body: JSON.stringify({
            requests: [
                { type: 'execute', stmt: { sql, args: args.map(_arg) } },
                { type: 'close' },
            ],
        }),
    });

    if (!res.ok) {
        const text = await res.text();
        throw new Error(`Turso HTTP ${res.status}: ${text}`);
    }
    return res.json();
}

async function ensureTable() {
    await tursoExecute(`
        CREATE TABLE IF NOT EXISTS gmaps_leads (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            nome             TEXT,
            endereco         TEXT,
            telefone         TEXT,
            site             TEXT,
            google_maps_link TEXT UNIQUE,
            lat              REAL,
            lon              REAL,
            h3_r8_id         TEXT,
            h3_r9_id         TEXT,
            tipo             TEXT,
            territory_id     TEXT,
            station_code     TEXT,
            cep              TEXT,
            updated_at       TEXT DEFAULT (datetime('now'))
        )
    `);
    // Migração: adiciona colunas se a tabela já existia sem elas
    for (const col of ['h3_r8_id', 'h3_r9_id']) {
        try {
            await tursoExecute(`ALTER TABLE gmaps_leads ADD COLUMN ${col} TEXT`);
        } catch (_) { /* já existe */ }
    }
    await tursoExecute(`CREATE INDEX IF NOT EXISTS idx_gmaps_leads_h3_r8 ON gmaps_leads (h3_r8_id)`);
    await tursoExecute(`CREATE INDEX IF NOT EXISTS idx_gmaps_leads_h3_r9 ON gmaps_leads (h3_r9_id)`);
}

async function upsertLead(item) {
    const h3r8 = latLonToH3(item.lat, item.lon, 8);
    const h3r9 = latLonToH3(item.lat, item.lon, 9);

    if (item.google_maps_link && item.google_maps_link !== 'N/A') {
        await tursoExecute(
            `INSERT INTO gmaps_leads
                (nome, endereco, telefone, site, google_maps_link, lat, lon, h3_r8_id, h3_r9_id, tipo, territory_id, station_code, cep, updated_at)
             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'))
             ON CONFLICT(google_maps_link) DO UPDATE SET
                nome       = excluded.nome,
                endereco   = CASE WHEN excluded.endereco != 'N/A' THEN excluded.endereco ELSE gmaps_leads.endereco END,
                telefone   = excluded.telefone,
                lat        = excluded.lat,
                lon        = excluded.lon,
                h3_r8_id   = excluded.h3_r8_id,
                h3_r9_id   = excluded.h3_r9_id,
                cep        = excluded.cep,
                updated_at = datetime('now')`,
            [item.nome, item.endereco, item.telefone, item.site,
             item.google_maps_link, item.lat, item.lon, h3r8, h3r9,
             item.tipo, item.territory_id, item.station_code, item.cep]
        );
    } else {
        await tursoExecute(
            `INSERT OR IGNORE INTO gmaps_leads
                (nome, endereco, telefone, site, google_maps_link, lat, lon, h3_r8_id, h3_r9_id, tipo, territory_id, station_code, cep)
             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`,
            [item.nome, item.endereco, item.telefone, item.site,
             item.google_maps_link, item.lat, item.lon, h3r8, h3r9,
             item.tipo, item.territory_id, item.station_code, item.cep]
        );
    }
}

// ---------------------------------------------------------------------------
// HELPERS
// ---------------------------------------------------------------------------

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function parseStationsArg() {
    const idx = process.argv.indexOf('--stations');
    if (idx === -1) return null;
    const stations = [];
    for (let i = idx + 1; i < process.argv.length; i++) {
        if (process.argv[i].startsWith('--')) break;
        stations.push(process.argv[i]);
    }
    return stations.length > 0 ? stations : null;
}

function extractCep(address) {
    const match = (address || '').match(/\b(\d{5})-?(\d{3})\b/);
    return match ? match[1] + match[2] : null;
}

function latLonToH3(lat, lon, res) {
    if (lat == null || lon == null) return null;
    try {
        return latLngToCell(lat, lon, res);
    } catch (_) {
        return null;
    }
}

async function fetchJson(url) {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`HTTP ${res.status} ao buscar ${url}`);
    return res.json();
}

async function _runWithConcurrency(tasks, concurrency, fn) {
    const queue = [...tasks];
    const workers = Array.from({ length: Math.min(concurrency, tasks.length) }, async () => {
        while (queue.length > 0) {
            const task = queue.shift();
            if (task) await fn(task);
        }
    });
    await Promise.all(workers);
}

// ---------------------------------------------------------------------------
// MAIN
// ---------------------------------------------------------------------------

async function main() {
    const filterStations = parseStationsArg();

    console.log('\nBuscando dados do Atlas via GitHub Pages...');
    const [idealSupply, territoriesIndex] = await Promise.all([
        fetchJson(`${ATLAS_BASE_URL}/ideal_supply.json`),
        fetchJson(`${ATLAS_BASE_URL}/territories_index.json`),
    ]);

    const openTerritories = new Set();
    for (const [tid, slots] of Object.entries(idealSupply.slots || {})) {
        const hasOpen = slots.some(s => !s.matched_partner_id);
        if (!hasOpen) continue;
        const meta = territoriesIndex[tid];
        if (!meta) continue;
        if (filterStations && !filterStations.includes(meta.station_code)) continue;
        openTerritories.add(tid);
    }

    const total = openTerritories.size * BUSINESS_TYPES.length;
    console.log(`\n${'='.repeat(60)}`);
    console.log(`  GMAPS BATCH SCRAPER → TURSO`);
    console.log(`  Territórios: ${openTerritories.size} | Tipos: ${BUSINESS_TYPES.length} | Total buscas: ${total}`);
    console.log(`  Concorrência: ${BATCH_CONCURRENCY} | Delay: ${DELAY_MS}ms`);
    console.log(`${'='.repeat(60)}\n`);

    await ensureTable();

    const tasks = [];
    for (const tid of openTerritories) {
        const meta = territoriesIndex[tid];
        for (const type of BUSINESS_TYPES) {
            tasks.push({ tid, meta, type });
        }
    }

    let done = 0;
    await _runWithConcurrency(tasks, BATCH_CONCURRENCY, async ({ tid, meta, type }) => {
        const seq = ++done;
        console.log(`  [${seq}/${total}] ${tid} | "${type}" @ ${meta.centroid_lat},${meta.centroid_lon}`);

        try {
            const items = await scrapeGmaps(type, String(meta.centroid_lat), String(meta.centroid_lon));
            for (const item of items) {
                await upsertLead({
                    nome:             item.name    || 'N/A',
                    endereco:         item.address || 'N/A',
                    telefone:         item.phone   || 'N/A',
                    site:             item.website || 'N/A',
                    google_maps_link: item.link    || 'N/A',
                    lat:              item.lat     ?? null,
                    lon:              item.lon     ?? null,
                    tipo:             type,
                    territory_id:     tid,
                    station_code:     meta.station_code,
                    cep:              item.cep || extractCep(item.address),
                });
            }
            console.log(`    → ${items.length} empresas processadas`);
        } catch (err) {
            console.error(`    ERR: ${err.message}`);
        }

        await sleep(DELAY_MS);
    });

    await closeSharedBrowser();

    console.log(`\n${'='.repeat(60)}`);
    console.log(`  CONCLUÍDO — dados gravados no Turso`);
    console.log(`${'='.repeat(60)}\n`);
}

main().catch(err => {
    console.error('Erro fatal:', err);
    process.exit(1);
});
