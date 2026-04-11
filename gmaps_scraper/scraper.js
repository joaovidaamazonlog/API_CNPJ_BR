/**
 * scraper.js
 * ==========
 * Scraping do Google Maps via Puppeteer com paralelismo controlado.
 *
 * Estratégia de paralelismo
 * -------------------------
 * - 1 browser compartilhado por chamada a scrapeGmaps
 * - N abas abertas simultaneamente (CONCURRENCY = 3 por padrão)
 * - Delay humano aleatório por aba para evitar detecção
 * - Coordenadas extraídas do link original (mais estável que URL da página)
 */

const puppeteer = require('puppeteer');

/** Número de abas abertas simultaneamente por busca */
const CONCURRENCY = 3;

/** User-agent para evitar bloqueio básico */
const USER_AGENT =
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' +
    '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';

// ---------------------------------------------------------------------------
// BROWSER COMPARTILHADO
// ---------------------------------------------------------------------------

let _sharedBrowser = null;

/**
 * Retorna (ou cria) o browser compartilhado para todo o batch.
 * Elimina o overhead de launch/close por chamada (~2-3s cada).
 * @returns {Promise<import('puppeteer').Browser>}
 */
async function getSharedBrowser() {
    if (!_sharedBrowser || !_sharedBrowser.connected) {
        _sharedBrowser = await puppeteer.launch({
            headless: true,
            args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
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

/**
 * Busca estabelecimentos no Google Maps para uma query e localização.
 * Reutiliza o browser compartilhado — não abre/fecha browser por chamada.
 *
 * @param {string} query  - Tipo de negócio (ex: "lanchonete")
 * @param {string} lat    - Latitude do centroide do território
 * @param {string} long   - Longitude do centroide do território
 * @returns {Promise<Object[]>}
 */
async function scrapeGmaps(query, lat, long) {
    const browser = await getSharedBrowser();

    try {
        // ── 1. Coletar links da página de resultados ──────────────────────
        const searchPage = await browser.newPage();
        await searchPage.setUserAgent(USER_AGENT);
        // Bloquear imagens/fontes/mídia na página de busca — não são necessários
        await searchPage.setRequestInterception(true);
        searchPage.on('request', req => {
            if (['image', 'font', 'media'].includes(req.resourceType())) req.abort();
            else req.continue();
        });

        const url = `https://www.google.com/maps/search/${encodeURIComponent(query)}/@${lat},${long},15z?hl=pt-BR`;
        await searchPage.goto(url, { waitUntil: 'networkidle2', timeout: 30000 });
        await searchPage.waitForSelector('a[href*="/maps/place/"]', { timeout: 15000 }).catch(() => null);
        await autoScroll(searchPage, 'div[role="feed"]');

        const links = await searchPage.$$eval(
            'a[href*="/maps/place/"]',
            anchors => [...new Set(anchors.map(a => a.href))]
        );
        await searchPage.close();

        const uniqueLinks = links.slice(0, 20);
        console.log(`    [scraper] ${uniqueLinks.length} links coletados para "${query}"`);

        // ── 2. Processar links em paralelo com concorrência limitada ──────
        const results = await _processWithConcurrency(browser, uniqueLinks, CONCURRENCY);

        return results.filter(r => r.name);

    } catch (error) {
        console.error('[scraper] Erro:', error.message);
        return [];
    }
    // Não fecha o browser — é compartilhado pelo batch inteiro
}

// ---------------------------------------------------------------------------
// PROCESSAMENTO PARALELO
// ---------------------------------------------------------------------------

/**
 * Processa uma lista de links com no máximo `concurrency` abas simultâneas.
 * Usa um pool de Promises para controlar a concorrência sem bibliotecas externas.
 *
 * @param {import('puppeteer').Browser} browser
 * @param {string[]} links
 * @param {number}   concurrency
 * @returns {Promise<Object[]>}
 */
async function _processWithConcurrency(browser, links, concurrency) {
    const results = [];
    const queue   = [...links];
    const active  = new Set();

    return new Promise((resolve) => {
        function next() {
            // Preencher slots disponíveis
            while (active.size < concurrency && queue.length > 0) {
                const link = queue.shift();
                const task = _scrapeDetail(browser, link)
                    .then(result => { if (result) results.push(result); })
                    .catch(err  => console.log(`    [scraper] Erro em ${link.slice(0, 60)}...: ${err.message}`))
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

/**
 * Abre uma nova aba, visita a página de detalhes do estabelecimento
 * e extrai nome, endereço, CEP, telefone, site e coordenadas.
 *
 * @param {import('puppeteer').Browser} browser
 * @param {string} link
 * @returns {Promise<Object|null>}
 */
async function _scrapeDetail(browser, link) {
    const page = await browser.newPage();
    await page.setUserAgent(USER_AGENT);
    // Bloquear imagens/fontes/mídia — reduz tempo de carregamento significativamente
    await page.setRequestInterception(true);
    page.on('request', req => {
        if (['image', 'font', 'media'].includes(req.resourceType())) req.abort();
        else req.continue();
    });

    try {
        await page.goto(link, { waitUntil: 'networkidle2', timeout: 30000 });

        // Aguardar o painel de detalhes carregar — o endereço completo (com CEP)
        // é renderizado num segundo request após o carregamento inicial da página.
        // Esperamos pelo elemento de endereço ou pelo h1, o que vier primeiro.
        await Promise.race([
            page.waitForSelector('[data-item-id="address"]', { timeout: 8000 }),
            page.waitForSelector('button[data-item-id="address"]', { timeout: 8000 }),
        ]).catch(() => null); // se não aparecer, continua mesmo assim

        // Delay humano aleatório (400ms – 900ms) — só para evitar detecção,
        // não mais responsável por esperar o DOM
        await delay(400 + Math.random() * 500);

        // ── Extração de dados via evaluate ────────────────────────────────
        const data = await page.evaluate(() => {
            const getText = (selector) =>
                document.querySelector(selector)?.innerText?.trim() || null;

            const name = getText('h1');

            // Endereço — o aria-label do botão contém o endereço completo com CEP
            // Ex: aria-label="Endereço: Rua X, 123 - Bairro, Cidade - MG, 30000-000, Brasil"
            const addressRaw = (() => {
                const btn =
                    document.querySelector('[data-item-id="address"]') ||
                    document.querySelector('button[data-item-id="address"]');
                if (btn) {
                    // aria-label tem o endereço completo incluindo CEP
                    const label = btn.getAttribute('aria-label') || '';
                    const fromLabel = label.replace(/^Endere[çc]o:\s*/i, '').trim();
                    if (fromLabel) return fromLabel;
                    return btn.innerText?.trim() || null;
                }
                // Fallbacks
                const byAriaLabel =
                    document.querySelector('[aria-label*="Endereço"]') ||
                    document.querySelector('[aria-label*="Address"]');
                if (byAriaLabel) {
                    return (byAriaLabel.getAttribute('aria-label') || byAriaLabel.innerText || '').trim() || null;
                }
                // Último recurso: buscar botão com texto de logradouro
                const all = Array.from(document.querySelectorAll('button, div[role="button"]'));
                const found = all.find(el =>
                    /(?:Rua|Av\.|Avenida|R\.|Alameda|Travessa|Praça)\s+/i.test(el.innerText)
                );
                return found?.innerText?.trim() || null;
            })();

            // Telefone — múltiplos seletores
            const phoneRaw =
                getText('[data-item-id="phone"]') ||
                getText('button[data-item-id="phone"]') ||
                getText('[aria-label*="Telefone"]') ||
                getText('[aria-label*="Phone"]');

            // Site
            const website =
                document.querySelector('a[data-item-id="authority"]')?.href ||
                document.querySelector('a[aria-label*="Site"]')?.href ||
                null;

            return { name, addressRaw, phoneRaw, website };
        });

        const bodyText = await page.evaluate(() => document.body.innerText);

        // ── CEP ───────────────────────────────────────────────────────────
        let cep = null;
        const cepMatch = (data.addressRaw || bodyText).match(/\b(\d{5})-?(\d{3})\b/);
        if (cepMatch) cep = cepMatch[1] + cepMatch[2];

        // ── Endereço limpo ────────────────────────────────────────────────
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

        // ── Telefone normalizado ──────────────────────────────────────────
        const normalizePhone = (raw) => {
            if (!raw) return null;
            let d = raw.replace(/\D/g, '');
            if (d.startsWith('55')) d = d.slice(2);
            return (d.length === 10 || d.length === 11) ? d : null;
        };

        // Telefone: campo dedicado > campo endereço > bodyText
        let phone = normalizePhone(data.phoneRaw);
        if (!phone && parsedAddr.phone) phone = parsedAddr.phone;
        if (!phone) {
            const m = bodyText.match(/(?:\+55\s?)?(?:\(?\d{2}\)?\s?)?(?:9\d{4}|\d{4})-?\d{4}/);
            phone = normalizePhone(m ? m[0] : null);
        }

        // Site: campo dedicado > campo endereço
        const website = data.website || parsedAddr.website || null;

        // ── CEP via ViaCEP (fallback quando não encontrado na página) ─────
        if (!cep && address) {
            cep = await lookupCep(address);
        }

        // ── Coordenadas ───────────────────────────────────────────────────
        // Prioridade 1: link original (!3d<lat>!4d<lon>)
        // Prioridade 2: URL atual da página (@lat,lon)
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

        return {
            name:    data.name || null,
            address,
            cep,
            phone,
            website: website,
            link,
            lat:     coordLat,
            lon:     coordLon,
        };

    } catch (err) {
        throw err;
    } finally {
        await page.close();
    }
}

// ---------------------------------------------------------------------------
// HELPERS
// ---------------------------------------------------------------------------

/**
 * Consulta o CEP via ViaCEP a partir de um endereço normalizado.
 * Extrai logradouro e tenta inferir a cidade do endereço ou usa "Belo Horizonte" como padrão.
 * Retorna o CEP como string de 8 dígitos ou null.
 *
 * @param {string} address
 * @returns {Promise<string|null>}
 */
async function lookupCep(address) {
    try {
        // Extrair logradouro (tudo antes do primeiro " - " ou da vírgula após o número)
        const streetMatch = address.match(/^(.+?,\s*\d+[^,\-]*)/);
        const street = streetMatch ? streetMatch[1].trim() : address.split(' - ')[0].trim();

        // Tentar extrair cidade do endereço (ex: "... - Bairro, Cidade")
        const cityMatch = address.match(/,\s*([^,\-]+)\s*$/);
        const city = cityMatch ? cityMatch[1].trim() : 'Belo Horizonte';

        const encoded = encodeURIComponent(street);
        const encodedCity = encodeURIComponent(city);
        const url = `https://viacep.com.br/ws/MG/${encodedCity}/${encoded}/json/`;

        const res = await fetch(url, { signal: AbortSignal.timeout(5000) });
        if (!res.ok) return null;

        const json = await res.json();
        if (Array.isArray(json) && json.length > 0 && json[0].cep) {
            return json[0].cep.replace('-', '');
        }
    } catch (_) {
        // ViaCEP indisponível ou timeout — não bloquear o scraping
    }
    return null;
}

/**
 * Analisa o campo de endereço bruto do Google Maps.
 * Retorna { address, phone, website } — o campo pode conter qualquer um desses.
 *
 * @param {string|null} raw
 * @param {RegExpMatchArray|null} cepMatch
 * @returns {{ address: string|null, phone: string|null, website: string|null }}
 */
function parseAddressField(raw, cepMatch) {
    const result = { address: null, phone: null, website: null };
    if (!raw) return result;

    // Limpar prefixos visuais antes de classificar
    let clean = raw
        .replace(/[\uE000-\uF8FF]/g, '')
        .replace(/[\n\r\t]+/g, ' ')
        .replace(/^[\s·•\-–—]+/, '')
        .replace(/\s{2,}/g, ' ')
        .trim();

    // Detectar se é telefone (ex: "+55 31 99266-6109" ou "31 99266-6109")
    const phonePattern = /^\+?(?:55\s?)?(?:\(?\d{2}\)?\s?)[\d\s\-().]{7,}$/;
    if (phonePattern.test(clean)) {
        const digits = clean.replace(/\D/g, '');
        const normalized = digits.startsWith('55') ? digits.slice(2) : digits;
        if (normalized.length === 10 || normalized.length === 11) {
            result.phone = normalized;
        }
        return result;
    }

    // Detectar se é URL/site
    if (/^https?:\/\//i.test(clean) || /^www\./i.test(clean)) {
        result.website = clean;
        return result;
    }

    // É endereço — normalizar
    let addr = clean;
    addr = addr.replace(cepMatch ? cepMatch[0] : /(?!x)x/, '');
    addr = addr.replace(/,?\s*,\s*Brasil\s*$/i, '');
    addr = addr.replace(/\s*-\s*[A-Z]{2}\s*,.*$/i, '');
    addr = addr.replace(/,\s*,/g, ',');
    addr = addr.replace(/\s{2,}/g, ' ').trim();

    // Extrair logradouro se houver prefixo descritivo
    const streetRe = /(?:Rua|Av\.|Avenida|R\.|Alameda|Travessa|Praça|Estrada|Rod\.|Beco|Largo)\s+.+/i;
    const startsWithStreet = /^(?:Rua|Av\.|Avenida|R\.|Alameda|Travessa|Praça|Estrada|Rod\.|Beco|Largo)/i;
    if (!startsWithStreet.test(addr)) {
        const m = addr.match(streetRe);
        if (m) addr = m[0].trim();
    }

    addr = addr.replace(/[\s,\-–—]+$/, '').trim();
    result.address = addr || null;
    return result;
}

/**
 * Scroll automático robusto — para quando a altura para de crescer.
 * @param {import('puppeteer').Page} page
 * @param {string} selector
 */
async function autoScroll(page, selector) {
    await page.evaluate(async (selector) => {
        const container = document.querySelector(selector);
        if (!container) return;
        await new Promise((resolve) => {
            let lastHeight = 0, sameCount = 0;
            const interval = setInterval(() => {
                container.scrollBy(0, 800);
                const newHeight = container.scrollHeight;
                if (newHeight === lastHeight) { sameCount++; } else { sameCount = 0; lastHeight = newHeight; }
                if (sameCount >= 5) { clearInterval(interval); resolve(); }
            }, 400);
        });
    }, selector);
}

/** @param {number} ms */
function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

module.exports = { scrapeGmaps, closeSharedBrowser };
