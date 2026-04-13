/**
 * scraper.js
 * ==========
 * Scraping do Google Maps via Puppeteer.
 *
 * Ajustes para maior cobertura (sem pressa):
 * - Concorrência reduzida para 2 abas por busca
 * - Timeouts maiores para páginas lentas
 * - Scroll mais agressivo para carregar mais resultados
 * - Delay humano maior entre ações
 */

const puppeteer = require('puppeteer');

/** Abas simultâneas por busca — menor = menos detecção */
const CONCURRENCY = 2;

const USER_AGENT =
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' +
    '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';

// ---------------------------------------------------------------------------
// BROWSER COMPARTILHADO
// ---------------------------------------------------------------------------

let _sharedBrowser = null;

async function getSharedBrowser() {
    if (!_sharedBrowser || !_sharedBrowser.connected) {
        _sharedBrowser = await puppeteer.launch({
            headless: true,
            args: [
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--window-size=1280,800',
            ],
        });
    }
    return _sharedBrowser;
}

async function closeSharedBrowser() {
    if (_sharedBrowser) {
        await _sharedBrowser.close().catch(() => {});
        _sharedBrowser = null;
    }
}

// ---------------------------------------------------------------------------
// FUNÇÃO PRINCIPAL
// ---------------------------------------------------------------------------

async function scrapeGmaps(query, lat, long) {
    const browser = await getSharedBrowser();

    try {
        // ── 1. Coletar links da página de resultados ──────────────────────
        const searchPage = await browser.newPage();
        await searchPage.setUserAgent(USER_AGENT);
        await searchPage.setViewport({ width: 1280, height: 800 });
        await searchPage.setRequestInterception(true);
        searchPage.on('request', req => {
            if (['image', 'font', 'media'].includes(req.resourceType())) req.abort();
            else req.continue();
        });

        const url = `https://www.google.com/maps/search/${encodeURIComponent(query)}/@${lat},${long},15z?hl=pt-BR`;

        // Timeout maior para carregar a página de resultados
        await searchPage.goto(url, { waitUntil: 'networkidle2', timeout: 45000 });
        await searchPage.waitForSelector('a[href*="/maps/place/"]', { timeout: 20000 }).catch(() => null);

        // Scroll mais agressivo — espera mais tempo para carregar mais resultados
        await autoScroll(searchPage, 'div[role="feed"]');

        // Delay extra após scroll para garantir que todos os resultados carregaram
        await delay(2000);

        const links = await searchPage.$$eval(
            'a[href*="/maps/place/"]',
            anchors => [...new Set(anchors.map(a => a.href))]
        ).catch(() => []);

        await searchPage.close();

        // Pegar até 30 links (era 20)
        const uniqueLinks = links.slice(0, 30);
        console.log(`    [scraper] ${uniqueLinks.length} links coletados para "${query}"`);

        // ── 2. Processar links em paralelo com concorrência limitada ──────
        const results = await _processWithConcurrency(browser, uniqueLinks, CONCURRENCY);

        return results.filter(r => r.name);

    } catch (error) {
        console.error('[scraper] Erro:', error.message);
        return [];
    }
}

// ---------------------------------------------------------------------------
// PROCESSAMENTO PARALELO
// ---------------------------------------------------------------------------

async function _processWithConcurrency(browser, links, concurrency) {
    const results = [];
    const queue   = [...links];
    const active  = new Set();

    return new Promise((resolve) => {
        function next() {
            while (active.size < concurrency && queue.length > 0) {
                const link = queue.shift();
                const task = _scrapeDetail(browser, link)
                    .then(result => { if (result) results.push(result); })
                    .catch(err => console.log(`    [scraper] Erro em ${link.slice(0, 60)}...: ${err.message}`))
                    .finally(() => {
                        active.delete(task);
                        if (queue.length === 0 && active.size === 0) {
                            resolve(results);
                        } else {
                            next();
                        }
                    });
                active.add(task);
            }
        }

        if (links.length === 0) { resolve(results); return; }
        next();
    });
}

// ---------------------------------------------------------------------------
// SCRAPING DE UMA PÁGINA DE DETALHES
// ---------------------------------------------------------------------------

async function _scrapeDetail(browser, link) {
    const page = await browser.newPage();
    await page.setUserAgent(USER_AGENT);
    await page.setViewport({ width: 1280, height: 800 });
    await page.setRequestInterception(true);
    page.on('request', req => {
        if (['image', 'font', 'media'].includes(req.resourceType())) req.abort();
        else req.continue();
    });

    try {
        // Timeout maior para páginas de detalhe
        await page.goto(link, { waitUntil: 'networkidle2', timeout: 45000 });

        // Aguardar endereço carregar — timeout maior
        await Promise.race([
            page.waitForSelector('[data-item-id="address"]',        { timeout: 12000 }),
            page.waitForSelector('button[data-item-id="address"]',  { timeout: 12000 }),
            page.waitForSelector('h1',                              { timeout: 12000 }),
        ]).catch(() => null);

        // Delay humano maior (800ms – 1800ms)
        await delay(800 + Math.random() * 1000);

        const data = await page.evaluate(() => {
            const getText = (selector) =>
                document.querySelector(selector)?.innerText?.trim() || null;

            const name = getText('h1');

            const addressRaw = (() => {
                const btn =
                    document.querySelector('[data-item-id="address"]') ||
                    document.querySelector('button[data-item-id="address"]');
                if (btn) {
                    const label = btn.getAttribute('aria-label') || '';
                    const fromLabel = label.replace(/^Endere[çc]o:\s*/i, '').trim();
                    if (fromLabel) return fromLabel;
                    return btn.innerText?.trim() || null;
                }
                const byAriaLabel =
                    document.querySelector('[aria-label*="Endereço"]') ||
                    document.querySelector('[aria-label*="Address"]');
                if (byAriaLabel) {
                    return (byAriaLabel.getAttribute('aria-label') || byAriaLabel.innerText || '').trim() || null;
                }
                const all = Array.from(document.querySelectorAll('button, div[role="button"]'));
                const found = all.find(el =>
                    /(?:Rua|Av\.|Avenida|R\.|Alameda|Travessa|Praça)\s+/i.test(el.innerText)
                );
                return found?.innerText?.trim() || null;
            })();

            const phoneRaw =
                getText('[data-item-id="phone"]') ||
                getText('button[data-item-id="phone"]') ||
                getText('[aria-label*="Telefone"]') ||
                getText('[aria-label*="Phone"]');

            const website =
                document.querySelector('a[data-item-id="authority"]')?.href ||
                document.querySelector('a[aria-label*="Site"]')?.href ||
                null;

            return { name, addressRaw, phoneRaw, website };
        });

        const bodyText = await page.evaluate(() => document.body.innerText);

        // CEP
        let cep = null;
        const cepMatch = (data.addressRaw || bodyText).match(/\b(\d{5})-?(\d{3})\b/);
        if (cepMatch) cep = cepMatch[1] + cepMatch[2];

        // Endereço
        const parsedAddr = parseAddressField(data.addressRaw, cepMatch);
        let address = parsedAddr.address;
        if (!address) {
            const m = bodyText.match(/(?:Rua|Av\.|Avenida|R\.|Alameda|Travessa|Praça)\s+[^\n,]+,\s*\d+/i);
            address = m ? parseAddressField(m[0], cepMatch).address : null;
        }
        if (!cep) {
            const fb = bodyText.match(/\b(\d{5})-?(\d{3})\b/);
            if (fb) cep = fb[1] + fb[2];
        }

        // Telefone
        const normalizePhone = (raw) => {
            if (!raw) return null;
            let d = raw.replace(/\D/g, '');
            if (d.startsWith('55')) d = d.slice(2);
            return (d.length === 10 || d.length === 11) ? d : null;
        };
        let phone = normalizePhone(data.phoneRaw);
        if (!phone && parsedAddr.phone) phone = parsedAddr.phone;
        if (!phone) {
            const m = bodyText.match(/(?:\+55\s?)?(?:\(?\d{2}\)?\s?)?(?:9\d{4}|\d{4})-?\d{4}/);
            phone = normalizePhone(m ? m[0] : null);
        }

        const website = data.website || parsedAddr.website || null;

        // CEP via ViaCEP fallback
        if (!cep && address) {
            cep = await lookupCep(address);
        }

        // Coordenadas
        let coordLat = null, coordLon = null;
        const dataCoord = link.match(/!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)/);
        if (dataCoord) {
            coordLat = parseFloat(dataCoord[1]);
            coordLon = parseFloat(dataCoord[2]);
        } else {
            const urlCoord = page.url().match(/@(-?\d+\.\d+),(-?\d+\.\d+)/);
            if (urlCoord) {
                coordLat = parseFloat(urlCoord[1]);
                coordLon = parseFloat(urlCoord[2]);
            }
        }

        return { name: data.name || null, address, cep, phone, website, link, lat: coordLat, lon: coordLon };

    } catch (err) {
        throw err;
    } finally {
        await page.close();
    }
}

// ---------------------------------------------------------------------------
// HELPERS
// ---------------------------------------------------------------------------

async function lookupCep(address) {
    try {
        const streetMatch = address.match(/^(.+?,\s*\d+[^,\-]*)/);
        const street = streetMatch ? streetMatch[1].trim() : address.split(' - ')[0].trim();
        const cityMatch = address.match(/,\s*([^,\-]+)\s*$/);
        const city = cityMatch ? cityMatch[1].trim() : 'Belo Horizonte';
        const url = `https://viacep.com.br/ws/MG/${encodeURIComponent(city)}/${encodeURIComponent(street)}/json/`;
        const res = await fetch(url, { signal: AbortSignal.timeout(5000) });
        if (!res.ok) return null;
        const json = await res.json();
        if (Array.isArray(json) && json.length > 0 && json[0].cep) {
            return json[0].cep.replace('-', '');
        }
    } catch (_) {}
    return null;
}

function parseAddressField(raw, cepMatch) {
    const result = { address: null, phone: null, website: null };
    if (!raw) return result;

    let clean = raw
        .replace(/[\uE000-\uF8FF]/g, '')
        .replace(/[\n\r\t]+/g, ' ')
        .replace(/^[\s·•\-–—]+/, '')
        .replace(/\s{2,}/g, ' ')
        .trim();

    const phonePattern = /^\+?(?:55\s?)?(?:\(?\d{2}\)?\s?)[\d\s\-().]{7,}$/;
    if (phonePattern.test(clean)) {
        const digits = clean.replace(/\D/g, '');
        const normalized = digits.startsWith('55') ? digits.slice(2) : digits;
        if (normalized.length === 10 || normalized.length === 11) result.phone = normalized;
        return result;
    }

    if (/^https?:\/\//i.test(clean) || /^www\./i.test(clean)) {
        result.website = clean;
        return result;
    }

    let addr = clean;
    addr = addr.replace(cepMatch ? cepMatch[0] : /(?!x)x/, '');
    addr = addr.replace(/,?\s*,\s*Brasil\s*$/i, '');
    addr = addr.replace(/\s*-\s*[A-Z]{2}\s*,.*$/i, '');
    addr = addr.replace(/,\s*,/g, ',').replace(/\s{2,}/g, ' ').trim();

    const streetRe = /(?:Rua|Av\.|Avenida|R\.|Alameda|Travessa|Praça|Estrada|Rod\.|Beco|Largo)\s+.+/i;
    const startsWithStreet = /^(?:Rua|Av\.|Avenida|R\.|Alameda|Travessa|Praça|Estrada|Rod\.|Beco|Largo)/i;
    if (!startsWithStreet.test(addr)) {
        const m = addr.match(streetRe);
        if (m) addr = m[0].trim();
    }

    result.address = addr.replace(/[\s,\-–—]+$/, '').trim() || null;
    return result;
}

async function autoScroll(page, selector) {
    await page.evaluate(async (selector) => {
        const container = document.querySelector(selector);
        if (!container) return;
        await new Promise((resolve) => {
            let lastHeight = 0, sameCount = 0;
            const interval = setInterval(() => {
                container.scrollBy(0, 600);
                const newHeight = container.scrollHeight;
                if (newHeight === lastHeight) {
                    sameCount++;
                } else {
                    sameCount = 0;
                    lastHeight = newHeight;
                }
                // Espera mais ciclos sem mudança antes de parar (era 5, agora 8)
                if (sameCount >= 8) {
                    clearInterval(interval);
                    resolve();
                }
            }, 600); // intervalo maior entre scrolls (era 400ms)
        });
    }, selector);
}

function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

module.exports = { scrapeGmaps, closeSharedBrowser };
