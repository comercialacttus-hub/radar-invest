// Radar Invest — sync-market (v2, sem IA obrigatória)
// Coleta: B3 inteira (brapi), fundamentos (Fundamentus), métricas de 3 meses (brapi),
// EUA (Finnhub), cripto top 100 (CoinGecko), Banco Central (SGS + Focus), notícias.
// Calcula: Radar Score por pilares, sinais por ativo, sinais do mercado e o resumo do dia.
//
// Escopos (campo "scope" do corpo da chamada):
//   crypto  — cripto top 100 + dominância (a cada 15 min)
//   market  — B3 inteira, Ibovespa, EUA, indicadores, notícias, score e resumo (a cada 30 min no pregão)
//   close   — fechamento do dia, fundamentos, Focus, métricas dos EUA (18h20)
//   metrics — métricas de 52 semanas e retornos da B3 + consenso dos EUA, em lotes (18h30–20h50)
//   score   — recalcula o Radar Score completo
//   all     — teste rápido: B3, EUA, cripto, fundamentos, macro, score (sem as métricas em lote)
//
// Secrets: BRAPI_TOKEN, FINNHUB_KEY, COINGECKO_KEY, CRON_SECRET; ANTHROPIC_API_KEY é opcional.

import { createClient } from "jsr:@supabase/supabase-js@2";

const SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? Deno.env.get("SUPABASE_SECRET_KEY") ?? "";
const sb = createClient(Deno.env.get("SUPABASE_URL")!, SERVICE_KEY);
const BRAPI = Deno.env.get("BRAPI_TOKEN") ?? "";
const FINNHUB = Deno.env.get("FINNHUB_KEY") ?? "";
const COINGECKO = Deno.env.get("COINGECKO_KEY") ?? "";
const ANTHROPIC = Deno.env.get("ANTHROPIC_API_KEY") ?? "";
const CRON_SECRET = Deno.env.get("CRON_SECRET") ?? "";
const MODEL = "claude-haiku-5-5";

const B3_METRICS_TOP = 200;   // quantos ativos da B3 recebem métricas de 52 semanas/retornos por dia
const B3_METRICS_BATCH = 40;  // quantos por chamada (respeita o tempo limite da função)
const LIQ_MIN_ACAO = 500_000; // liquidez média diária mínima (R$) para entrar no score
const LIQ_MIN_FII = 150_000;

type Row = Record<string, any>;
type Asset = { symbol: string; name: string; kind: string; source_id: string | null; sector?: string | null; carteiras?: string[] };

// ---------------------------------------------------------------- utilidades
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const todayBR = () => new Date().toLocaleDateString("sv-SE", { timeZone: "America/Sao_Paulo" });
const norm = (t: string) => (t ?? "").toLowerCase().normalize("NFD").replace(/[̀-ͯ]/g, "");
const fmt = (n: number, d = 1) => Number(n).toLocaleString("pt-BR", { minimumFractionDigits: d, maximumFractionDigits: d });
const sinal = (n: number, d = 1) => (n > 0 ? "+" : n < 0 ? "−" : "") + fmt(Math.abs(n), d) + "%";
const avg = (xs: (number | null | undefined)[]) => { const v = xs.filter((x): x is number => typeof x === "number" && isFinite(x)); return v.length ? v.reduce((a, b) => a + b, 0) / v.length : null; };
const median = (xs: number[]) => { const v = xs.filter(isFinite).sort((a, b) => a - b); if (!v.length) return null; const m = Math.floor(v.length / 2); return v.length % 2 ? v[m] : (v[m - 1] + v[m]) / 2; };
const ptNum = (s: string | undefined) => {
  if (s == null) return null;
  const t = String(s).replace(/<[^>]+>/g, "").replace(/%/g, "").replace(/\./g, "").replace(",", ".").trim();
  if (!t || t === "-") return null;
  const n = Number(t); return isFinite(n) ? n : null;
};

async function getJSON(url: string, headers: Record<string, string> = {}) {
  const r = await fetch(url, { headers });
  if (!r.ok) throw new Error(`${r.status} em ${url.split("?")[0]}`);
  return r.json();
}

async function save(table: string, rows: Row | Row[], log: string[], onConflict?: string) {
  const list = Array.isArray(rows) ? rows : [rows];
  for (let i = 0; i < list.length; i += 500) {
    const { error } = await sb.from(table).upsert(list.slice(i, i + 500) as never, onConflict ? { onConflict } : undefined);
    if (error) { log.push(`Banco (${table}): ${error.message}`); return; }
  }
}

// Lê a tabela inteira (o Supabase devolve no máximo 1.000 linhas por vez)
async function selectAll(table: string, cols = "*", filter?: (q: any) => any): Promise<Row[]> {
  const out: Row[] = [];
  for (let from = 0; ; from += 1000) {
    let q = sb.from(table).select(cols).range(from, from + 999);
    if (filter) q = filter(q);
    const { data, error } = await q;
    if (error || !data) break;
    out.push(...data);
    if (data.length < 1000) break;
  }
  return out;
}

// Percentil (0–100) de v dentro de uma lista; higher=false inverte (menor é melhor)
function pr(values: number[], v: number | null | undefined, higher = true): number | null {
  if (v == null || !isFinite(v) || values.length < 3) return null;
  let below = 0, equal = 0;
  for (const x of values) { if (x < v) below++; else if (x === v) equal++; }
  const p = ((below + equal / 2) / values.length) * 100;
  return Math.round(higher ? p : 100 - p);
}

// ---------------------------------------------------------------- B3: universo inteiro
function kindFromB3(s: Row): string {
  const st = norm(s.subType ?? s.sub_type ?? ""), t = norm(s.type ?? "");
  if (st.includes("fii")) return "fii";
  if (st.includes("etf")) return "etf";
  if (t === "bdr" || st.includes("bdr")) return "bdr";
  if (t === "fund") return /11$/.test(s.stock ?? s.symbol ?? "") ? "fii" : "fundo";
  return "acao";
}

async function syncUniverse(assets: Asset[], log: string[]) {
  let stocks: Row[] = [];
  try {
    const j = await getJSON(`https://brapi.dev/api/quote/list`, { Authorization: `Bearer ${BRAPI}` });
    stocks = j?.stocks ?? [];
  } catch (e) { log.push(`B3 lista: ${(e as Error).message}`); return; }
  const now = new Date().toISOString();
  const rows = stocks.filter((s) => s.stock && s.close != null).map((s) => ({
    symbol: s.stock, name: s.name, type: s.type ?? null, sub_type: s.subType ?? null, sector: s.sector ?? null,
    price: s.close, change_pct: s.change, volume: s.volume, market_cap: s.market_cap, logo: s.logo ?? null, updated_at: now,
  }));
  if (!rows.length) { log.push("B3 lista: veio vazia"); return; }
  await save("universe", rows, log);

  // Mantém a tabela de cotações dos ativos acompanhados (usa a mesma consulta, sem gastar cota)
  const bySym = new Map(rows.map((r) => [r.symbol, r]));
  const q = assets.filter((a) => ["acao_br", "fii", "etf_br"].includes(a.kind)).map((a) => bySym.get(a.source_id ?? a.symbol)).filter(Boolean)
    .map((r) => ({ symbol: r!.symbol, price: r!.price, currency: "BRL", change_pct: r!.change_pct, updated_at: now }));
  if (q.length) await save("quotes", q, log);
}

async function syncIbov(log: string[]) {
  try {
    const j = await getJSON(`https://brapi.dev/api/quote/%5EBVSP`, { Authorization: `Bearer ${BRAPI}` });
    const q = j?.results?.[0];
    if (q) await save("quotes", { symbol: "^BVSP", price: q.regularMarketPrice, currency: "BRL", change_pct: q.regularMarketChangePercent, updated_at: new Date().toISOString() }, log);
  } catch (e) { log.push(`Ibovespa: ${(e as Error).message}`); }
}

// Fechamento do dia → histórico próprio
async function snapshotClose(log: string[]) {
  const uni = await selectAll("universe", "symbol,price,volume");
  const day = todayBR();
  await save("daily_close", uni.filter((u) => u.price != null).map((u) => ({ symbol: u.symbol, day, close: u.price, volume: u.volume })), log, "symbol,day");
}

// ---------------------------------------------------------------- B3: fundamentos (Fundamentus)
function parseTable(html: string): { head: string[]; rows: string[][] } {
  const head = [...html.matchAll(/<th[^>]*>([\s\S]*?)<\/th>/gi)].map((m) => norm(m[1].replace(/<[^>]+>/g, "")).replace(/[^a-z0-9]/g, ""));
  const body = html.split(/<tbody[^>]*>/i)[1] ?? "";
  const rows = body.split(/<tr[^>]*>/i).slice(1).map((tr) => [...tr.matchAll(/<td[^>]*>([\s\S]*?)<\/td>/gi)].map((m) => m[1].replace(/<[^>]+>/g, "").trim()));
  return { head, rows: rows.filter((r) => r.length > 3) };
}
async function getLatin1(url: string) {
  const r = await fetch(url, { headers: { "User-Agent": "Mozilla/5.0 (RadarInvest pessoal)", "Accept": "text/html" } });
  if (!r.ok) throw new Error(`${r.status} em ${url}`);
  return new TextDecoder("iso-8859-1").decode(await r.arrayBuffer());
}
const MAP_ACAO: Record<string, string> = {
  cotacao: "preco", pl: "pl", pvp: "pvp", psr: "psr", divyield: "dy", evebit: "ev_ebit", evebitda: "ev_ebitda",
  mrgebit: "mrg_ebit", mrgliq: "mrg_liq", liqcorr: "liq_corr", roic: "roic", roe: "roe", liq2meses: "liq_2m",
  patrimliq: "patrimonio", divbrutpatrim: "div_pl", crescrec5a: "cresc_5a",
};
const MAP_FII: Record<string, string> = {
  segmento: "segmento", cotacao: "preco", ffoyield: "ffo_yield", dividendyield: "dy", pvp: "pvp", valordemercado: "valor_mercado",
  liquidez: "liq", qtddeimoveis: "imoveis", precodom2: "preco_m2", aluguelporm2: "aluguel_m2", caprate: "cap_rate", vacanciamedia: "vacancia",
};
async function syncFundamentus(log: string[]) {
  const now = new Date().toISOString();
  for (const [kind, url, map] of [["acao", "https://www.fundamentus.com.br/resultado.php", MAP_ACAO], ["fii", "https://www.fundamentus.com.br/fii_resultado.php", MAP_FII]] as const) {
    try {
      const { head, rows } = parseTable(await getLatin1(url));
      const out: Row[] = [];
      for (const r of rows) {
        const symbol = r[0]?.trim(); if (!symbol) continue;
        const data: Row = {};
        head.forEach((h, i) => { const key = map[h]; if (!key || i === 0) return; data[key] = key === "segmento" ? r[i] : ptNum(r[i]); });
        out.push({ symbol, kind, data, source: "fundamentus", updated_at: now });
      }
      if (out.length) await save("fundamentals", out, log); else log.push(`Fundamentus ${kind}: tabela vazia`);
    } catch (e) { log.push(`Fundamentus ${kind}: ${(e as Error).message}`); }
  }
}

// ---------------------------------------------------------------- B3: métricas de preço (brapi 3 meses), em lotes
function priceMetrics(closes: number[], price: number | null, high52?: number, low52?: number) {
  const n = closes.length, last = price ?? closes[n - 1];
  const at = (k: number) => (n > k ? closes[n - 1 - k] : null);
  const ret = (k: number) => { const p = at(k); return p ? ((last / p) - 1) * 100 : null; };
  const mm = (k: number) => (n >= k ? avg(closes.slice(n - k)) : null);
  const rets: number[] = []; for (let i = 1; i < n; i++) if (closes[i - 1] > 0) rets.push(Math.log(closes[i] / closes[i - 1]));
  const m = avg(rets) ?? 0; const sd = rets.length > 5 ? Math.sqrt(rets.reduce((a, r) => a + (r - m) ** 2, 0) / (rets.length - 1)) : null;
  const mm50 = mm(50), mm20 = mm(20);
  const prevAbove = n > 51 ? closes[n - 2] > (avg(closes.slice(n - 51, n - 1)) ?? Infinity) : null;
  return {
    ret_1s: ret(5), ret_1m: ret(21), ret_3m: n > 2 ? ((last / closes[0]) - 1) * 100 : null,
    mm20, mm50, acima_mm50: mm50 ? last > mm50 : null, cruzou_mm50: mm50 != null && prevAbove === false && last > mm50,
    vol_anual: sd != null ? sd * Math.sqrt(252) * 100 : null,
    high52: high52 ?? null, low52: low52 ?? null,
    dist_high52: high52 ? ((last / high52) - 1) * 100 : null, dist_low52: low52 ? ((last / low52) - 1) * 100 : null,
  };
}

async function syncB3Metrics(log: string[]) {
  const today = todayBR();
  const uni = await selectAll("universe", "symbol,type,sub_type,volume,price");
  const top = uni.filter((u) => ["acao", "fii", "etf"].includes(kindFromB3(u))).sort((a, b) => (b.volume ?? 0) - (a.volume ?? 0)).slice(0, B3_METRICS_TOP);
  const have = await selectAll("metrics", "symbol,updated_at");
  const fresh = new Set(have.filter((m) => new Date(m.updated_at).toLocaleDateString("sv-SE", { timeZone: "America/Sao_Paulo" }) === today).map((m) => m.symbol));
  const todo = top.filter((u) => !fresh.has(u.symbol)).slice(0, B3_METRICS_BATCH);
  let ok = 0;
  for (const u of todo) {
    try {
      const j = await getJSON(`https://brapi.dev/api/quote/${encodeURIComponent(u.symbol)}?range=3mo&interval=1d`, { Authorization: `Bearer ${BRAPI}` });
      const q = j?.results?.[0]; if (!q) continue;
      const hist: Row[] = (q.historicalDataPrice ?? []).filter((h: Row) => h.close != null).sort((a: Row, b: Row) => a.date - b.date);
      const closes = hist.map((h) => Number(h.close));
      const data = {
        ...priceMetrics(closes, q.regularMarketPrice ?? null, q.fiftyTwoWeekHigh, q.fiftyTwoWeekLow),
        pl_brapi: q.priceEarnings ?? null, lpa: q.earningsPerShare ?? null,
        vol_medio_3m: q.averageDailyVolume3Month ?? null, vol_medio_10d: q.averageDailyVolume10Day ?? null,
        volume_hoje: q.regularMarketVolume ?? null,
      };
      await save("metrics", { symbol: u.symbol, data, source: "brapi", updated_at: new Date().toISOString() }, log);
      if (hist.length) await save("daily_close", hist.map((h) => ({ symbol: u.symbol, day: new Date(h.date * 1000).toISOString().slice(0, 10), close: h.close, volume: h.volume ?? null })), log, "symbol,day");
      ok++;
    } catch (e) { log.push(`Métricas ${u.symbol}: ${(e as Error).message}`); if (String(e).includes("429")) break; }
  }
  if (todo.length) log.push(`Métricas B3: ${ok}/${todo.length} atualizadas neste lote`);
}

// ---------------------------------------------------------------- EUA (Finnhub)
async function syncUSQuotes(assets: Asset[], log: string[]) {
  const us = assets.filter((a) => ["acao_us", "etf_us"].includes(a.kind));
  for (const a of us) {
    try {
      const q = await getJSON(`https://finnhub.io/api/v1/quote?symbol=${encodeURIComponent(a.source_id ?? a.symbol)}`, { "X-Finnhub-Token": FINNHUB });
      if (q?.c) await save("quotes", { symbol: a.symbol, price: q.c, currency: "USD", change_pct: q.dp, updated_at: new Date().toISOString() }, log);
    } catch (e) { log.push(`EUA ${a.symbol}: ${(e as Error).message}`); if (String(e).includes("429")) { await sleep(5000); } }
  }
}
const pick = (o: Row, ...keys: string[]) => { for (const k of keys) if (o?.[k] != null && isFinite(o[k])) return Number(o[k]); return null; };
async function syncUSMetrics(assets: Asset[], log: string[], lote = 15) {
  // Em lotes, começando pelos mais desatualizados (cabe no tempo limite da função)
  const today = todayBR();
  const have = await selectAll("metrics", "symbol,updated_at,source", (q) => q.eq("source", "finnhub"));
  const fresh = new Set(have.filter((m) => new Date(m.updated_at).toLocaleDateString("sv-SE", { timeZone: "America/Sao_Paulo" }) === today).map((m) => m.symbol));
  const us = assets.filter((a) => ["acao_us", "etf_us"].includes(a.kind) && !fresh.has(a.symbol)).slice(0, lote);
  const now = new Date().toISOString();
  if (us.length) log.push(`Métricas EUA: ${us.length} neste lote`);
  for (const a of us) {
    try {
      const j = await getJSON(`https://finnhub.io/api/v1/stock/metric?symbol=${encodeURIComponent(a.source_id ?? a.symbol)}&metric=all`, { "X-Finnhub-Token": FINNHUB });
      const m = j?.metric ?? {};
      const fund = {
        pl: pick(m, "peTTM", "peBasicExclExtraTTM", "peExclExtraTTM"), pvp: pick(m, "pbQuarterly", "pbAnnual"),
        dy: pick(m, "dividendYieldIndicatedAnnual", "currentDividendYieldTTM"), roe: pick(m, "roeTTM", "roeRfy"),
        mrg_liq: pick(m, "netProfitMarginTTM", "netProfitMarginAnnual"), cresc_5a: pick(m, "revenueGrowth5Y"),
        div_pl: pick(m, "totalDebt/totalEquityQuarterly", "totalDebt/totalEquityAnnual"), beta: pick(m, "beta"),
      };
      const old = (await sb.from("fundamentals").select("data").eq("symbol", a.symbol).maybeSingle()).data?.data ?? {};
      await save("fundamentals", { symbol: a.symbol, kind: "us", data: { ...old, ...fund }, source: "finnhub", updated_at: now }, log);
      const high52 = pick(m, "52WeekHigh"), low52 = pick(m, "52WeekLow");
      const price = (await sb.from("quotes").select("price").eq("symbol", a.symbol).maybeSingle()).data?.price ?? null;
      await save("metrics", { symbol: a.symbol, source: "finnhub", updated_at: now, data: {
        high52, low52, ret_3m: pick(m, "13WeekPriceReturnDaily"), ret_6m: pick(m, "26WeekPriceReturnDaily"), ret_12m: pick(m, "52WeekPriceReturnDaily"),
        ret_ytd: pick(m, "yearToDatePriceReturnDaily"), beta: fund.beta,
        dist_high52: price && high52 ? ((price / high52) - 1) * 100 : null, dist_low52: price && low52 ? ((price / low52) - 1) * 100 : null,
      } }, log);
    } catch (e) { log.push(`EUA métricas ${a.symbol}: ${(e as Error).message}`); }
    await sleep(1100); // limite de 60 chamadas/min
  }
}
async function syncUSConsensus(assets: Asset[], log: string[]) {
  const us = assets.filter((a) => a.kind === "acao_us");
  const have = await selectAll("fundamentals", "symbol,data", (q) => q.eq("kind", "us"));
  const today = todayBR();
  const todo = us.filter((a) => (have.find((h) => h.symbol === a.symbol)?.data?.consenso_dia ?? "") !== today).slice(0, 20);
  for (const a of todo) {
    try {
      const arr = await getJSON(`https://finnhub.io/api/v1/stock/recommendation?symbol=${encodeURIComponent(a.source_id ?? a.symbol)}`, { "X-Finnhub-Token": FINNHUB });
      const r = arr?.[0]; if (!r) continue;
      const total = (r.strongBuy ?? 0) + (r.buy ?? 0) + (r.hold ?? 0) + (r.sell ?? 0) + (r.strongSell ?? 0);
      const old = have.find((h) => h.symbol === a.symbol)?.data ?? {};
      await save("fundamentals", { symbol: a.symbol, kind: "us", source: "finnhub", updated_at: new Date().toISOString(), data: {
        ...old, consenso: total ? Math.round(((r.strongBuy + r.buy) / total) * 100) : null, analistas: total,
        rec: { compra: r.strongBuy + r.buy, neutro: r.hold, venda: r.sell + r.strongSell, periodo: r.period }, consenso_dia: today,
      } }, log);
    } catch (e) {
      if (String(e).includes("403")) { log.push("Consenso EUA: não disponível no plano grátis"); return; }
      log.push(`Consenso ${a.symbol}: ${(e as Error).message}`);
    }
    await sleep(1100);
  }
}

// ---------------------------------------------------------------- Cripto (CoinGecko)
async function syncCrypto(assets: Asset[], log: string[]) {
  const h = { "x-cg-demo-api-key": COINGECKO };
  try {
    // Tenta com mais dados; se o plano grátis recusar algum parâmetro (401/400), tenta versões mais simples
    const base = `https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page=1`;
    let arr: Row[] | null = null, erro = "";
    for (const extra of ["&price_change_percentage=24h,7d,30d,1y", "&price_change_percentage=24h,7d,30d", "&price_change_percentage=7d", ""]) {
      try { arr = await getJSON(base + extra, h); break; } catch (e) { erro = (e as Error).message; if (!/40[01]/.test(erro)) break; }
    }
    if (!arr) throw new Error(erro);
    const now = new Date().toISOString();
    const rows = (arr ?? []).map((c: Row) => ({
      id: c.id, symbol: String(c.symbol).toUpperCase(), name: c.name, rank: c.market_cap_rank, price: c.current_price, market_cap: c.market_cap,
      volume: c.total_volume, ch_24h: c.price_change_percentage_24h_in_currency ?? c.price_change_percentage_24h,
      ch_7d: c.price_change_percentage_7d_in_currency, ch_30d: c.price_change_percentage_30d_in_currency, ch_1y: c.price_change_percentage_1y_in_currency,
      ath: c.ath, ath_pct: c.ath_change_percentage, image: c.image, updated_at: now,
    }));
    if (rows.length) await save("crypto_markets", rows, log); else log.push("Cripto: lista vazia");
    const byId = new Map(rows.map((r: Row) => [r.id, r]));
    const q = assets.filter((a) => a.kind === "cripto" && byId.has(a.source_id!)).map((a) => { const r: Row = byId.get(a.source_id!)!; return { symbol: a.symbol, price: r.price, currency: "USD", change_pct: r.ch_24h, updated_at: now }; });
    if (q.length) await save("quotes", q, log);
  } catch (e) { log.push(`Cripto: ${(e as Error).message}`); }
  try {
    const g = (await getJSON(`https://api.coingecko.com/api/v3/global`, h))?.data;
    if (g) await save("macro", [
      { key: "btc_dom", label: "Dominância do bitcoin (%)", value: g.market_cap_percentage?.btc, updated_at: new Date().toISOString() },
      { key: "cripto_mcap_24h", label: "Mercado cripto 24h (%)", value: g.market_cap_change_percentage_24h_usd, updated_at: new Date().toISOString() },
    ], log);
  } catch (e) { log.push(`Cripto global: ${(e as Error).message}`); }
}

// ---------------------------------------------------------------- Banco Central (SGS + Focus)
async function sgs(code: number, n = 1): Promise<{ data: string; valor: number }[]> {
  const j = await getJSON(`https://api.bcb.gov.br/dados/serie/bcdata.sgs.${code}/dados/ultimos/${n}?formato=json`);
  return (j ?? []).map((p: Row) => ({ data: p.data, valor: Number(String(p.valor).replace(",", ".")) }));
}
const isoBR = (d: string) => { const [dd, mm, yy] = d.split("/"); return `${yy}-${mm}-${dd}`; };
async function syncMacro(log: string[], completo = false) {
  const now = new Date().toISOString();
  const simples: [string, string, number][] = [["selic", "Selic meta (% a.a.)", 432], ["ipca12m", "IPCA 12 meses (%)", 13522], ["dolar", "Dólar PTAX venda (R$)", 1], ["cdi", "CDI (% a.a.)", 4389]];
  for (const [key, label, code] of simples) {
    try { const [p] = await sgs(code); if (p) await save("macro", { key, label, value: p.valor, ref_date: isoBR(p.data), updated_at: now }, log); }
    catch (e) { log.push(`BCB ${key}: ${(e as Error).message}`); }
  }
  if (!completo) return;
  try { // IGP-M acumulado em 12 meses e IPCA do último mês
    const ig = await sgs(189, 12); const ac = (ig.reduce((a, p) => a * (1 + p.valor / 100), 1) - 1) * 100;
    if (ig.length === 12) await save("macro", { key: "igpm12m", label: "IGP-M 12 meses (%)", value: ac, ref_date: isoBR(ig[11].data), updated_at: now }, log);
    const [ip] = await sgs(433, 1); if (ip) await save("macro", { key: "ipca_mes", label: "IPCA do mês (%)", value: ip.valor, ref_date: isoBR(ip.data), updated_at: now }, log);
    const pt = await sgs(1, 6); // dólar: dias seguidos de alta/queda
    let seq = 0; for (let i = pt.length - 1; i > 0; i--) { const d = Math.sign(pt[i].valor - pt[i - 1].valor); if (seq === 0) seq = d; else if (Math.sign(seq) === d) seq += d; else break; }
    await save("macro", { key: "dolar_seq", label: "Dias seguidos do dólar (+ alta, − queda)", value: seq, ref_date: isoBR(pt[pt.length - 1].data), updated_at: now }, log);
  } catch (e) { log.push(`BCB extras: ${(e as Error).message}`); }
  try { // Boletim Focus
    const y = Number(todayBR().slice(0, 4));
    const filtro = `(Indicador eq 'IPCA' or Indicador eq 'Selic' or Indicador eq 'PIB Total' or Indicador eq 'Câmbio') and (DataReferencia eq '${y}' or DataReferencia eq '${y + 1}') and baseCalculo eq 0`;
    const url = `https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/ExpectativasMercadoAnuais?$top=200&$filter=${encodeURIComponent(filtro)}&$orderby=${encodeURIComponent("Data desc")}&$select=Indicador,Data,DataReferencia,Mediana&$format=json`;
    const rows: Row[] = (await getJSON(url))?.value ?? [];
    if (!rows.length) { log.push("Focus: sem dados"); return; }
    await save("focus", rows.map((r) => ({ indicador: r.Indicador, ref_year: String(r.DataReferencia), data: r.Data, mediana: r.Mediana })), log, "indicador,ref_year,data");
    const key: Record<string, string> = { "IPCA": "ipca", "Selic": "selic", "PIB Total": "pib", "Câmbio": "cambio" };
    const seen = new Set<string>();
    for (const r of rows) {
      const k = `focus_${key[r.Indicador]}_${r.DataReferencia}`; if (seen.has(k)) continue; seen.add(k);
      await save("macro", { key: k, label: `Focus ${r.Indicador} ${r.DataReferencia}`, value: r.Mediana, ref_date: r.Data, updated_at: now }, log);
    }
  } catch (e) { log.push(`Focus: ${(e as Error).message}`); }
}

// ---------------------------------------------------------------- Notícias
const FEEDS = [["InfoMoney", "https://www.infomoney.com.br/feed/"], ["Money Times", "https://www.moneytimes.com.br/mercados/feed/"]];
type NewsRow = { id: string; published_at: string; title: string; url: string; source: string; category: string; impact: string };
const tag = (item: string, name: string) => { const m = item.match(new RegExp(`<${name}[^>]*>([\\s\\S]*?)</${name}>`, "i")); return m ? m[1].replace(/<!\[CDATA\[|\]\]>/g, "").replace(/<[^>]+>/g, "").trim() : ""; };
const hashId = (s: string) => { let h = 0; for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) | 0; return "n" + (h >>> 0).toString(36); };
const TEMAS: [string, string[]][] = [
  ["Cripto", ["bitcoin", "cripto", "ether", "ethereum", "solana", "btc"]],
  ["Petróleo", ["petroleo", "brent", "petrobras", "opep", "ormuz", "combustive", "gasolina", "diesel"]],
  ["Juros", ["selic", "juros", "copom", "fed ", "fomc", "treasur", "inflacao", "ipca", "bce", "banco central"]],
  ["Global", ["eua", "china", "europa", "trump", "wall street", "s&p", "nasdaq", "dow jones", "fmi"]],
  ["Empresas", ["lucro", "balanco", "dividendo", "jcp", "resultado", "ipo", "aquisicao", "fusao"]],
];
const SOBE = ["sobe", "alta", "avanca", "dispara", "recorde", "valoriza", "ganha", "lucro", "cresce", "salta", "dividendo", "corte de juros", "corta juros", "acordo", "tregua", "queda da inflacao", "inflacao desacelera", "inflacao cai"];
const CAI = ["cai", "queda", "recua", "despenca", "perde", "prejuizo", "tomba", "crise", "guerra", "ataque", "rebaixa", "acima do teto", "inflacao sobe", "inflacao acelera", "eleva juros", "alta dos juros", "tarifa", "calote", "recessao"];
function classifyByRules(r: NewsRow): NewsRow {
  const t = norm(r.title);
  for (const [cat, words] of TEMAS) if (words.some((w) => t.includes(w))) { r.category = cat; break; }
  if (/(queda|recuo|desacelera|cai)\s+(da|de)?\s*inflacao|inflacao\s+(cai|desacelera|recua)/.test(t)) { r.impact = "up"; return r; }
  const neg = CAI.filter((w) => t.includes(w)).length, pos = SOBE.filter((w) => t.includes(w)).length;
  r.impact = neg > pos ? "down" : pos > neg ? "up" : "neu";
  return r;
}
async function syncNews(log: string[]) {
  const fresh: NewsRow[] = [];
  for (const [source, url] of FEEDS) {
    try {
      const xml = await (await fetch(url, { headers: { "User-Agent": "RadarInvest/1.0" } })).text();
      for (const it of xml.split(/<item[\s>]/i).slice(1, 16)) {
        const title = tag(it, "title"), link = tag(it, "link"), pub = tag(it, "pubDate");
        if (title) fresh.push({ id: hashId(link || title), published_at: pub ? new Date(pub).toISOString() : new Date().toISOString(), title, url: link, source, category: "Brasil", impact: "neu" });
      }
    } catch (e) { log.push(`RSS ${source}: ${(e as Error).message}`); }
  }
  if (!fresh.length) return;
  const { data: existing } = await sb.from("news").select("id").in("id", fresh.map((n) => n.id));
  const have = new Set((existing ?? []).map((r: Row) => r.id));
  const novas = fresh.filter((n) => !have.has(n.id)).map(classifyByRules);
  if (novas.length) await save("news", novas, log);
  await sb.from("news").delete().lt("published_at", new Date(Date.now() - 30 * 864e5).toISOString());
}

// ---------------------------------------------------------------- Radar Score
type Pillars = { renda: number | null; valor: number | null; qualidade: number | null; momento: number | null; risco: number | null; consenso: number | null };
const PILAR_NOME: Record<string, string> = { renda: "renda", valor: "valor", qualidade: "qualidade", momento: "momento", risco: "segurança", consenso: "consenso" };

function resumoTexto(nome: string, chg: number | null, p: Pillars, sinais: Row[]): string {
  const partes: string[] = [];
  if (chg != null) partes.push(`${nome} ${chg >= 0 ? "sobe" : "cai"} ${fmt(Math.abs(chg), 2)}% hoje.`);
  const ent = Object.entries(p).filter(([, v]) => v != null) as [string, number][];
  const fortes = ent.filter(([, v]) => v >= 70).sort((a, b) => b[1] - a[1]).slice(0, 2);
  const fracos = ent.filter(([, v]) => v <= 30).sort((a, b) => a[1] - b[1]).slice(0, 1);
  if (fortes.length) partes.push(`Destaques: ${fortes.map(([k, v]) => `${PILAR_NOME[k]} (${v})`).join(" e ")}.`);
  if (fracos.length) partes.push(`Ponto de atenção: ${PILAR_NOME[fracos[0][0]]} (${fracos[0][1]}).`);
  const s = sinais.slice(0, 2).map((x) => x.txt);
  if (s.length) partes.push(s.join("; ") + ".");
  return partes.join(" ");
}

async function computeScores(assets: Asset[], log: string[]) {
  let [uni, funds, mets, quotes, cryptos, macroRows] = await Promise.all([
    selectAll("universe"), selectAll("fundamentals"), selectAll("metrics"), selectAll("quotes"), selectAll("crypto_markets"), selectAll("macro"),
  ]);
  const F = new Map(funds.map((f) => [f.symbol, f.data])), M = new Map(mets.map((m) => [m.symbol, m.data])), Q = new Map(quotes.map((q) => [q.symbol, q]));
  const macro = Object.fromEntries(macroRows.map((m) => [m.key, m.value]));
  const selic = macro.selic ?? 13.75, cdi = macro.cdi ?? selic;
  const A = new Map(assets.map((a) => [a.symbol, a]));
  const now = new Date().toISOString();
  const out: Row[] = [];

  // ---- B3: ações e FIIs (precisam de fundamentos e liquidez mínima)
  const b3 = uni.map((u) => ({ u, kind: kindFromB3(u), f: F.get(u.symbol) ?? {}, m: M.get(u.symbol) ?? {} }));
  // Sem fundamentos (se o Fundamentus falhar), usa os mais negociados para não deixar o score vazio
  const semFund = !funds.some((f) => f.kind === "acao");
  const topVol = (k: string, n: number) => new Set(b3.filter((x) => x.kind === k).sort((a, b) => (b.u.volume ?? 0) - (a.u.volume ?? 0)).slice(0, n).map((x) => x.u.symbol));
  const topA = semFund ? topVol("acao", 300) : new Set<string>(), topF = semFund ? topVol("fii", 200) : new Set<string>();
  if (semFund) log.push("Score sem fundamentos (Fundamentus indisponível): usando os ativos mais negociados");
  const acoes = b3.filter((x) => x.kind === "acao" && (semFund ? topA.has(x.u.symbol) : ((x.f.liq_2m ?? 0) >= LIQ_MIN_ACAO || A.has(x.u.symbol)) && x.f.preco != null));
  const fiis = b3.filter((x) => x.kind === "fii" && (semFund ? topF.has(x.u.symbol) : ((x.f.liq ?? 0) >= LIQ_MIN_FII || A.has(x.u.symbol)) && x.f.preco != null));
  const etfs = b3.filter((x) => x.kind === "etf" && x.m.ret_3m != null);
  const col = (arr: typeof acoes, fn: (x: (typeof acoes)[number]) => number | null | undefined) => arr.map(fn).filter((v): v is number => v != null && isFinite(v));

  // setor: mediana de P/L por setor (para sinal "P/L abaixo do setor")
  const porSetor = new Map<string, typeof acoes>();
  for (const x of acoes) { const s = x.u.sector ?? "Outros"; if (!porSetor.has(s)) porSetor.set(s, []); porSetor.get(s)!.push(x); }
  const plSetor = new Map([...porSetor].map(([s, xs]) => [s, median(col(xs, (x) => x.f.pl > 0 ? x.f.pl : null))]));

  const cA = {
    dy: col(acoes, (x) => x.f.dy), roe: col(acoes, (x) => x.f.roe), mrg: col(acoes, (x) => x.f.mrg_liq), divpl: col(acoes, (x) => x.f.div_pl),
    cresc: col(acoes, (x) => x.f.cresc_5a), liq: col(acoes, (x) => x.f.liq_2m), r3: col(acoes, (x) => x.m.ret_3m), r1: col(acoes, (x) => x.m.ret_1m),
    dh: col(acoes, (x) => x.m.dist_high52), vol: col(acoes, (x) => x.m.vol_anual),
  };
  for (const x of acoes) {
    const { u, f, m } = x; const setor = porSetor.get(u.sector ?? "Outros")!;
    const grupo = setor.length >= 5 ? setor : acoes;
    const p: Pillars = {
      renda: pr(cA.dy, f.dy),
      valor: avg([f.pl > 0 ? pr(col(grupo, (y) => y.f.pl > 0 ? y.f.pl : null), f.pl, false) : 10, f.pvp > 0 ? pr(col(grupo, (y) => y.f.pvp > 0 ? y.f.pvp : null), f.pvp, false) : 10, f.ev_ebit > 0 ? pr(col(grupo, (y) => y.f.ev_ebit > 0 ? y.f.ev_ebit : null), f.ev_ebit, false) : null]),
      qualidade: avg([pr(cA.roe, f.roe), pr(cA.mrg, f.mrg_liq), pr(cA.divpl, f.div_pl, false), pr(cA.cresc, f.cresc_5a)]),
      momento: avg([pr(cA.r3, m.ret_3m), pr(cA.r1, m.ret_1m), pr(cA.dh, m.dist_high52), m.acima_mm50 == null ? null : m.acima_mm50 ? 70 : 30]),
      risco: avg([pr(cA.vol, m.vol_anual, false), pr(cA.liq, f.liq_2m), pr(cA.divpl, f.div_pl, false)]),
      consenso: A.get(u.symbol)?.carteiras?.length ? Math.min(100, A.get(u.symbol)!.carteiras!.length * 25) : null,
    };
    for (const k of Object.keys(p) as (keyof Pillars)[]) if (p[k] != null) p[k] = Math.round(p[k]!);
    const s: Row[] = [];
    const dyExtra = f.dy != null && f.dy > 20;
    if (dyExtra) {
      if (p.renda != null) p.renda = Math.min(p.renda, 60);
      s.push({ k: "dy_extra", txt: `DY de ${fmt(f.dy)}% provavelmente inclui pagamento extraordinário; não deve se repetir`, tom: "neu" });
    } else if (f.dy != null && f.dy >= selic) s.push({ k: "dy_selic", txt: `Dividendos de ${fmt(f.dy)}%, acima da Selic`, tom: "up" });
    else if (f.dy != null && f.dy >= 8) s.push({ k: "dy_alto", txt: `Dividendos altos (${fmt(f.dy)}% em 12 meses)`, tom: "up" });
    const pls = plSetor.get(u.sector ?? "Outros");
    if (f.pl > 0 && pls && f.pl < pls * 0.6) s.push({ k: "pl_setor", txt: `P/L de ${fmt(f.pl)}, abaixo do setor (${fmt(pls)})`, tom: "up" });
    if (f.pl != null && f.pl < 0) s.push({ k: "prejuizo", txt: "Empresa com prejuízo nos últimos 12 meses", tom: "down" });
    if (m.dist_high52 != null && m.dist_high52 >= -1) s.push({ k: "max52", txt: "Na máxima de 52 semanas", tom: "up" });
    if (m.dist_low52 != null && m.dist_low52 <= 3) s.push({ k: "min52", txt: "Perto da mínima de 52 semanas", tom: "down" });
    if (m.cruzou_mm50) s.push({ k: "mm50", txt: "Voltou para cima da média de 50 dias", tom: "up" });
    if (u.change_pct >= 5) s.push({ k: "alta_forte", txt: `Alta forte hoje (${sinal(u.change_pct)})`, tom: "up" });
    if (u.change_pct <= -5) s.push({ k: "queda_forte", txt: `Queda forte hoje (${sinal(u.change_pct)})`, tom: "down" });
    const vm = m.vol_medio_3m; if (vm && u.volume && u.volume >= 2 * vm) s.push({ k: "volume", txt: `Volume ${fmt(u.volume / vm)}x acima da média`, tom: "neu" });
    if (f.div_pl != null && f.div_pl > 3) s.push({ k: "divida", txt: `Dívida alta (${fmt(f.div_pl)}x o patrimônio)`, tom: "down" });
    if (f.roe != null && f.roe >= 20) s.push({ k: "roe", txt: `Rentabilidade alta (ROE ${fmt(f.roe)}%)`, tom: "up" });
    out.push({ symbol: u.symbol, name: u.name, kind: "acao", sector: u.sector, price: u.price, change_pct: u.change_pct, pillars: p, signals: s,
      resumo: resumoTexto(u.symbol, u.change_pct, p, s), updated_at: now,
      data: { dy: f.dy, pl: f.pl, pvp: f.pvp, roe: f.roe, mrg_liq: f.mrg_liq, div_pl: f.div_pl, cresc_5a: f.cresc_5a, liq_2m: f.liq_2m, ev_ebit: f.ev_ebit,
        ret_1m: m.ret_1m, ret_3m: m.ret_3m, high52: m.high52, low52: m.low52, dist_high52: m.dist_high52, vol_anual: m.vol_anual, mm50: m.mm50, pl_setor: pls, market_cap: u.market_cap, logo: u.logo } });
  }

  const cF = { dy: col(fiis, (x) => x.f.dy), pvp: col(fiis, (x) => x.f.pvp > 0 ? x.f.pvp : null), liq: col(fiis, (x) => x.f.liq), vac: col(fiis, (x) => x.f.imoveis > 0 ? x.f.vacancia : null),
    im: col(fiis, (x) => x.f.imoveis > 0 ? x.f.imoveis : null), r3: col(fiis, (x) => x.m.ret_3m), vol: col(fiis, (x) => x.m.vol_anual) };
  for (const x of fiis) {
    const { u, f, m } = x;
    const p: Pillars = {
      renda: pr(cF.dy, f.dy), valor: f.pvp > 0 ? pr(cF.pvp, f.pvp, false) : null,
      qualidade: f.imoveis > 0 ? avg([pr(cF.vac, f.vacancia, false), pr(cF.im, f.imoveis)]) : null,
      momento: avg([pr(cF.r3, m.ret_3m), m.acima_mm50 == null ? null : m.acima_mm50 ? 70 : 30]),
      risco: avg([pr(cF.liq, f.liq), pr(cF.vol, m.vol_anual, false)]), consenso: null,
    };
    for (const k of Object.keys(p) as (keyof Pillars)[]) if (p[k] != null) p[k] = Math.round(p[k]!);
    const s: Row[] = [];
    const cdiLiq = cdi * 0.85;
    const extremo = (f.dy != null && f.dy > 18) || (f.pvp > 0 && f.pvp < 0.6);
    if (extremo) {
      if (p.renda != null) p.renda = Math.min(p.renda, 50);
      if (p.valor != null) p.valor = Math.min(p.valor, 50);
      const motivo = f.pvp > 0 && f.pvp < 0.6 ? `P/VP de ${fmt(f.pvp, 2)}` : `DY de ${fmt(f.dy)}%`;
      s.push({ k: "fii_extremo", txt: `${motivo} fora do normal: o mercado vê risco (inadimplência, dívida ou rendimento não recorrente). Investigar antes`, tom: "down" });
    } else if (f.dy != null && f.dy >= cdiLiq) s.push({ k: "dy_cdi", txt: `Rende ${fmt(f.dy)}% isento, acima do CDI líquido (~${fmt(cdiLiq)}%)`, tom: "up" });
    if (!extremo && f.pvp > 0 && f.pvp < 0.9) s.push({ k: "desconto", txt: `Negocia com ${fmt((1 - f.pvp) * 100, 0)}% de desconto sobre o patrimônio (P/VP ${fmt(f.pvp, 2)})`, tom: "up" });
    if (f.pvp > 1.15) s.push({ k: "agio", txt: `Ágio de ${fmt((f.pvp - 1) * 100, 0)}% sobre o patrimônio`, tom: "down" });
    if (f.imoveis > 0 && f.vacancia >= 15) s.push({ k: "vacancia", txt: `Vacância alta (${fmt(f.vacancia)}%)`, tom: "down" });
    if (u.change_pct <= -3) s.push({ k: "queda_forte", txt: `Queda de ${sinal(u.change_pct)} hoje`, tom: "down" });
    out.push({ symbol: u.symbol, name: u.name, kind: "fii", sector: f.segmento ?? u.sector, price: u.price, change_pct: u.change_pct, pillars: p, signals: s,
      resumo: resumoTexto(u.symbol, u.change_pct, p, s), updated_at: now,
      data: { dy: f.dy, pvp: f.pvp, ffo_yield: f.ffo_yield, vacancia: f.vacancia, imoveis: f.imoveis, liq: f.liq, valor_mercado: f.valor_mercado, cap_rate: f.cap_rate, ret_3m: m.ret_3m, dist_high52: m.dist_high52, logo: u.logo } });
  }

  const cE = { r3: col(etfs, (x) => x.m.ret_3m), vol: col(etfs, (x) => x.m.vol_anual) };
  for (const x of etfs) {
    const { u, m } = x;
    const p: Pillars = { renda: null, valor: null, qualidade: null, momento: avg([pr(cE.r3, m.ret_3m), m.acima_mm50 == null ? null : m.acima_mm50 ? 70 : 30]), risco: pr(cE.vol, m.vol_anual, false), consenso: null };
    for (const k of Object.keys(p) as (keyof Pillars)[]) if (p[k] != null) p[k] = Math.round(p[k]!);
    const s: Row[] = []; if (m.dist_high52 != null && m.dist_high52 >= -1) s.push({ k: "max52", txt: "Na máxima de 52 semanas", tom: "up" });
    out.push({ symbol: u.symbol, name: u.name, kind: "etf", sector: u.sector, price: u.price, change_pct: u.change_pct, pillars: p, signals: s, resumo: resumoTexto(u.symbol, u.change_pct, p, s), updated_at: now, data: { ret_1m: m.ret_1m, ret_3m: m.ret_3m, vol_anual: m.vol_anual, dist_high52: m.dist_high52, logo: u.logo } });
  }

  // ---- EUA
  const us = assets.filter((a) => ["acao_us", "etf_us"].includes(a.kind)).map((a) => ({ a, f: F.get(a.symbol) ?? {}, m: M.get(a.symbol) ?? {}, q: Q.get(a.symbol) ?? {} }));
  const cU = { dy: us.map((x) => x.f.dy).filter((v) => v != null), pl: us.map((x) => x.f.pl).filter((v) => v > 0), pvp: us.map((x) => x.f.pvp).filter((v) => v > 0), roe: us.map((x) => x.f.roe).filter((v) => v != null),
    mrg: us.map((x) => x.f.mrg_liq).filter((v) => v != null), cr: us.map((x) => x.f.cresc_5a).filter((v) => v != null), r3: us.map((x) => x.m.ret_3m).filter((v) => v != null), r12: us.map((x) => x.m.ret_12m).filter((v) => v != null),
    dh: us.map((x) => x.m.dist_high52).filter((v) => v != null), beta: us.map((x) => x.f.beta).filter((v) => v != null) };
  for (const { a, f, m, q } of us) {
    const p: Pillars = {
      renda: pr(cU.dy, f.dy), valor: a.kind === "acao_us" ? avg([f.pl > 0 ? pr(cU.pl, f.pl, false) : 10, f.pvp > 0 ? pr(cU.pvp, f.pvp, false) : null]) : null,
      qualidade: a.kind === "acao_us" ? avg([pr(cU.roe, f.roe), pr(cU.mrg, f.mrg_liq), pr(cU.cr, f.cresc_5a)]) : null,
      momento: avg([pr(cU.r3, m.ret_3m), pr(cU.r12, m.ret_12m), pr(cU.dh, m.dist_high52)]), risco: pr(cU.beta, f.beta, false), consenso: f.consenso ?? null,
    };
    for (const k of Object.keys(p) as (keyof Pillars)[]) if (p[k] != null) p[k] = Math.round(p[k]!);
    const s: Row[] = [];
    if (m.dist_high52 != null && m.dist_high52 >= -1) s.push({ k: "max52", txt: "Na máxima de 52 semanas", tom: "up" });
    if (m.dist_high52 != null && m.dist_high52 <= -30) s.push({ k: "longe_max", txt: `${fmt(Math.abs(m.dist_high52), 0)}% abaixo da máxima de 52 semanas`, tom: "down" });
    if (f.consenso != null && f.analistas >= 10 && f.consenso >= 75) s.push({ k: "consenso", txt: `${f.consenso}% dos ${f.analistas} analistas recomendam compra`, tom: "up" });
    if (f.dy != null && f.dy >= 3) s.push({ k: "dy", txt: `Paga ${fmt(f.dy)}% em dividendos em dólar`, tom: "up" });
    if (q.change_pct <= -4) s.push({ k: "queda_forte", txt: `Queda forte hoje (${sinal(q.change_pct)})`, tom: "down" });
    if (q.change_pct >= 4) s.push({ k: "alta_forte", txt: `Alta forte hoje (${sinal(q.change_pct)})`, tom: "up" });
    out.push({ symbol: a.symbol, name: a.name, kind: "us", sector: a.sector ?? (a.kind === "etf_us" ? "ETF" : null), price: q.price ?? null, change_pct: q.change_pct ?? null, pillars: p, signals: s,
      resumo: resumoTexto(a.symbol, q.change_pct ?? null, p, s), updated_at: now,
      data: { dy: f.dy, pl: f.pl, pvp: f.pvp, roe: f.roe, mrg_liq: f.mrg_liq, beta: f.beta, consenso: f.consenso, analistas: f.analistas, rec: f.rec, ret_3m: m.ret_3m, ret_12m: m.ret_12m, high52: m.high52, low52: m.low52, dist_high52: m.dist_high52, etf: a.kind === "etf_us" } });
  }

  // ---- Cripto (top 100, sem stablecoins)
  const STABLE = new Set(["USDT", "USDC", "DAI", "FDUSD", "TUSD", "USDE", "USDS", "PYUSD", "USD1", "USDD", "BUSD", "USDP", "GUSD", "FRAX", "RLUSD", "USDTB", "BUIDL", "USYC", "EURC"]);
  cryptos = cryptos.filter((c) => !STABLE.has(String(c.symbol).toUpperCase()) && !(Math.abs((c.price ?? 0) - 1) < 0.02 && Math.abs(c.ch_30d ?? 0) < 1));
  const cC = { c7: cryptos.map((c) => c.ch_7d).filter((v) => v != null), c30: cryptos.map((c) => c.ch_30d).filter((v) => v != null), c1y: cryptos.map((c) => c.ch_1y).filter((v) => v != null),
    ath: cryptos.map((c) => c.ath_pct).filter((v) => v != null), mc: cryptos.map((c) => c.market_cap).filter((v) => v != null), vol: cryptos.map((c) => c.volume).filter((v) => v != null) };
  for (const c of cryptos) {
    const p: Pillars = { renda: null, valor: pr(cC.ath, c.ath_pct, false), qualidade: null, momento: avg([pr(cC.c7, c.ch_7d), pr(cC.c30, c.ch_30d), pr(cC.c1y, c.ch_1y)]), risco: avg([pr(cC.mc, c.market_cap), pr(cC.vol, c.volume)]), consenso: null };
    for (const k of Object.keys(p) as (keyof Pillars)[]) if (p[k] != null) p[k] = Math.round(p[k]!);
    const s: Row[] = [];
    if (c.ath_pct != null && c.ath_pct <= -70) s.push({ k: "ath", txt: `${fmt(Math.abs(c.ath_pct), 0)}% abaixo do recorde histórico`, tom: "neu" });
    if (c.ath_pct != null && c.ath_pct >= -3) s.push({ k: "ath_perto", txt: "Perto do recorde histórico", tom: "up" });
    if (c.ch_7d != null && c.ch_7d >= 15) s.push({ k: "7d", txt: `Subiu ${fmt(c.ch_7d, 0)}% em 7 dias`, tom: "up" });
    if (c.ch_7d != null && c.ch_7d <= -15) s.push({ k: "7d", txt: `Caiu ${fmt(Math.abs(c.ch_7d), 0)}% em 7 dias`, tom: "down" });
    out.push({ symbol: c.symbol, name: c.name, kind: "cripto", sector: `#${c.rank}`, price: c.price, change_pct: c.ch_24h, pillars: p, signals: s, resumo: resumoTexto(c.symbol, c.ch_24h, p, s), updated_at: now,
      data: { rank: c.rank, market_cap: c.market_cap, ch_7d: c.ch_7d, ch_30d: c.ch_30d, ch_1y: c.ch_1y, ath: c.ath, ath_pct: c.ath_pct, image: c.image, id: c.id } });
  }

  // grava (troca a tabela inteira para não sobrar ativo que saiu do filtro)
  if (out.length) {
    const seen = new Set<string>(); const uniq = out.filter((r) => (seen.has(r.symbol) ? false : (seen.add(r.symbol), true)));
    await save("scores", uniq, log);
    await sb.from("scores").delete().lt("updated_at", new Date(Date.now() - 3 * 864e5).toISOString());
    log.push(`Score: ${uniq.length} ativos (ações ${acoes.length}, FIIs ${fiis.length}, ETFs ${etfs.length}, EUA ${us.length}, cripto ${cryptos.length})`);
  }
  await computeMarketSignals(uni, acoes.map((x) => x.u), macro, cryptos, log);
}

// ---------------------------------------------------------------- Sinais do mercado
async function computeMarketSignals(uni: Row[], liquidas: Row[], macro: Row, cryptos: Row[], log: string[]) {
  const s: Row[] = []; const add = (id: string, scope: string, tom: string, titulo: string, detalhe: string, afeta: string) => s.push({ id, scope, tom, titulo, detalhe, afeta });
  const y = todayBR().slice(0, 4);
  if (macro.selic != null && macro[`focus_ipca_${Number(y) + 1}`] != null) {
    const real = ((1 + macro.selic / 100) / (1 + macro[`focus_ipca_${Number(y) + 1}`] / 100) - 1) * 100;
    await save("macro", { key: "juro_real", label: "Juro real (Selic − IPCA esperado)", value: real, updated_at: new Date().toISOString() }, log);
    if (real >= 7) add("juro_real", "macro", "neu", `Juro real de ${fmt(real)}% ao ano`, "Renda fixa paga muito acima da inflação esperada, o que compete com ações e FIIs.", "renda fixa ▲ · FIIs ▼ · ações de crescimento ▼");
    else if (real <= 4) add("juro_real", "macro", "up", `Juro real caiu para ${fmt(real)}% ao ano`, "Juro real mais baixo costuma favorecer bolsa e FIIs.", "ações ▲ · FIIs ▲ · renda fixa ▼");
  }
  // tendência do Focus (IPCA e Selic do ano que vem): semanas seguidas na mesma direção
  for (const [ind, nome] of [["IPCA", "IPCA"], ["Selic", "Selic"]] as const) {
    const { data } = await sb.from("focus").select("data,mediana").eq("indicador", ind).eq("ref_year", String(Number(y) + 1)).order("data", { ascending: false }).limit(40);
    const semanal: number[] = []; let ultimaSemana = ""; // uma leitura por semana
    for (const r of data ?? []) { const w = r.data.slice(0, 7) + "-" + Math.floor(Number(r.data.slice(8, 10)) / 7); if (w !== ultimaSemana) { semanal.push(Number(r.mediana)); ultimaSemana = w; } if (semanal.length >= 6) break; }
    let seq = 0; for (let i = 0; i < semanal.length - 1; i++) { const d = Math.sign(semanal[i] - semanal[i + 1]); if (d === 0) break; if (seq === 0 || Math.sign(seq) === d) seq += d; else break; }
    if (Math.abs(seq) >= 3) {
      const subindo = seq > 0;
      add(`focus_${ind}`, "macro", ind === "IPCA" ? (subindo ? "down" : "up") : (subindo ? "down" : "up"),
        `Mercado ${subindo ? "elevou" : "reduziu"} a projeção de ${nome} ${Number(y) + 1} pela ${Math.abs(seq)}ª semana`,
        `Mediana do Focus agora em ${fmt(semanal[0], 2)}%.`,
        subindo ? "Selic ▲ · renda fixa ▲ · ações ▼" : "Selic ▼ · ações ▲ · FIIs ▲");
    }
  }
  if (macro.dolar_seq != null && Math.abs(macro.dolar_seq) >= 4) {
    const up = macro.dolar_seq > 0;
    add("dolar_seq", "macro", up ? "down" : "up", `Dólar ${up ? "subiu" : "caiu"} ${Math.abs(macro.dolar_seq)} dias seguidos`, `PTAX em R$ ${fmt(macro.dolar ?? 0, 4)}.`, up ? "exportadoras ▲ · IVVB11 ▲ · inflação ▲" : "importados ▼ · inflação ▼ · IVVB11 ▼");
  }
  // amplitude da B3 e setores
  const liq = liquidas.filter((u) => u.change_pct != null);
  if (liq.length >= 30) {
    const sobe = liq.filter((u) => u.change_pct > 0).length / liq.length * 100;
    add("amplitude", "b3", sobe >= 60 ? "up" : sobe <= 40 ? "down" : "neu", `${fmt(sobe, 0)}% das ações líquidas sobem hoje`, `${liq.length} ações acompanhadas no cálculo.`, sobe >= 60 ? "Ibovespa ▲" : sobe <= 40 ? "Ibovespa ▼" : "Ibovespa ◆");
    const setores = new Map<string, number[]>(); for (const u of liq) { const k = u.sector ?? "Outros"; if (!setores.has(k)) setores.set(k, []); setores.get(k)!.push(u.change_pct); }
    const med = [...setores].filter(([, v]) => v.length >= 4).map(([k, v]) => [k, avg(v)!] as [string, number]).sort((a, b) => b[1] - a[1]);
    if (med.length >= 2) {
      add("setor_top", "b3", "up", `${med[0][0]} lidera hoje (${sinal(med[0][1])} em média)`, `Pior setor: ${med[med.length - 1][0]} (${sinal(med[med.length - 1][1])}).`, `${med[0][0]} ▲ · ${med[med.length - 1][0]} ▼`);
    }
  }
  const btc = cryptos.find((c) => c.id === "bitcoin");
  if (btc && macro.btc_dom != null) add("btc_dom", "cripto", "neu", `Bitcoin domina ${fmt(macro.btc_dom)}% do mercado cripto`, `BTC ${sinal(btc.ch_7d ?? 0)} em 7 dias e ${fmt(Math.abs(btc.ath_pct ?? 0), 0)}% abaixo do recorde.`, "cripto ◆");
  await sb.from("market_signals").delete().neq("id", "");
  if (s.length) await save("market_signals", s, log);
}

// ---------------------------------------------------------------- Resumo do dia (sem IA)
const AFETA: Record<string, [string, string]> = {
  "Brasil": ["Ibovespa ▲ · dólar ▼", "Ibovespa ▼ · dólar ▲"], "Global": ["ações dos EUA ▲ · IVVB11 ▲", "ações dos EUA ▼ · IVVB11 ▼"],
  "Juros": ["ações ▲ · FIIs ▲ · renda fixa ▼", "renda fixa ▲ · ações ▼ · FIIs ▼"], "Petróleo": ["PETR4 ▲ · inflação ▲", "PETR4 ▼ · inflação ▼"],
  "Cripto": ["bitcoin ▲ · HASH11 ▲", "bitcoin ▼ · HASH11 ▼"], "Empresas": ["ações citadas ▲", "ações citadas ▼"],
};
const PARA_VOCE: Record<string, string> = {
  "Brasil": "Pode mexer com o Ibovespa, o dólar e as ações brasileiras da sua lista.", "Global": "Pode mexer com as ações dos EUA, o IVVB11 e o dólar.",
  "Juros": "Juros mexem com renda fixa, FIIs e ações de crescimento.", "Petróleo": "Petróleo mexe com Petrobras, combustíveis, inflação e empresas aéreas.",
  "Cripto": "Pode mexer com bitcoin, outras criptos e o HASH11.", "Empresas": "Vale olhar a ficha da empresa citada antes de decidir.",
};
async function syncBriefingByRules(log: string[]) {
  const [news, macroRows, sigs, scores] = await Promise.all([
    sb.from("news").select("title,category,impact,published_at").order("published_at", { ascending: false }).limit(30).then((r) => r.data ?? []),
    selectAll("macro"), selectAll("market_signals"), selectAll("scores", "symbol,kind,change_pct,data"),
  ]);
  const m = Object.fromEntries(macroRows.map((x) => [x.key, x.value]));
  const ib = (await sb.from("quotes").select("price,change_pct").eq("symbol", "^BVSP").maybeSingle()).data;
  const coisas: string[] = [];
  if (ib) coisas.push(`Ibovespa ${ib.change_pct >= 0 ? "sobe" : "cai"} ${fmt(Math.abs(ib.change_pct), 2)}% e marca ${fmt(ib.price, 0)} pontos` + (m.dolar ? `; dólar PTAX a R$ ${fmt(m.dolar, 2)}` : ""));
  const liq = scores.filter((s: Row) => s.kind === "acao" && s.change_pct != null && (s.data?.liq_2m ?? 0) >= 5_000_000).sort((a: Row, b: Row) => b.change_pct - a.change_pct);
  if (liq.length >= 2) coisas.push(`Na B3, ${liq[0].symbol} lidera (${sinal(liq[0].change_pct, 2)}) e ${liq[liq.length - 1].symbol} é a maior queda (${sinal(liq[liq.length - 1].change_pct, 2)})`);
  const macroSig = sigs.find((x) => x.scope === "macro") ?? sigs[0];
  if (macroSig) coisas.push(macroSig.titulo);
  else if (m.selic) coisas.push(`Selic em ${fmt(m.selic, 2)}% e IPCA de 12 meses em ${fmt(m.ipca12m ?? 0, 2)}%`);
  const destaque = news.find((n: Row) => n.impact !== "neu") ?? news[0];
  const cen = (tom: string) => [
    ...sigs.filter((x) => x.tom === tom).map((x) => ({ titulo: x.titulo, afeta: x.afeta })),
    ...news.filter((n: Row) => n.impact === tom).map((n: Row) => ({ titulo: n.title, afeta: (AFETA[n.category] ?? AFETA["Brasil"])[tom === "up" ? 0 : 1] })),
  ].slice(0, 3);
  const ups = sigs.filter((x) => x.tom === "up").length + news.filter((n: Row) => n.impact === "up").length;
  const downs = sigs.filter((x) => x.tom === "down").length + news.filter((n: Row) => n.impact === "down").length;
  const chg = ib?.change_pct ?? 0;
  const termometro = chg > 0.5 || ups > downs * 1.5 ? "Otimista" : chg < -0.5 || downs > ups * 1.5 ? "Pessimista" : "Cauteloso";
  const content = { auto: true, tres_coisas: coisas, manchete: destaque ? { titulo: destaque.title, para_voce: PARA_VOCE[destaque.category] ?? PARA_VOCE["Brasil"] } : undefined, melhora: cen("up"), piora: cen("down"), termometro };
  await save("briefing", { day: todayBR(), content, updated_at: new Date().toISOString() }, log);
}

// ---------------------------------------------------------------- entrada
Deno.serve(async (req) => {
  if (!CRON_SECRET || req.headers.get("x-cron-secret") !== CRON_SECRET) {
    return new Response(JSON.stringify({ error: "não autorizado" }), { status: 401, headers: { "Content-Type": "application/json" } });
  }
  const { scope = "market" } = await req.json().catch(() => ({}));
  const log: string[] = [];
  if (!SERVICE_KEY) log.push("Sem chave de serviço do Supabase no ambiente da função");
  const assets = (await selectAll("assets", "symbol,name,kind,source_id,sector,carteiras")) as Asset[];
  if (!assets.length) log.push("Nenhum ativo encontrado na tabela assets");
  const t0 = Date.now();

  try {
    if (scope === "crypto") await syncCrypto(assets, log);
    if (scope === "market" || scope === "all") {
      await syncUniverse(assets, log); await syncIbov(log); await syncUSQuotes(assets, log); await syncMacro(log); await syncNews(log);
    }
    if (scope === "close") {
      await syncUniverse(assets, log); await snapshotClose(log); await syncFundamentus(log); await syncMacro(log, true);
      await syncUSMetrics(assets, log, 15);
    }
    if (scope === "all") { await syncFundamentus(log); await syncMacro(log, true); } // teste rápido (métricas ficam com os escopos close/metrics)
    if (scope === "metrics") { await syncB3Metrics(log); await syncUSMetrics(assets, log, 15); await syncUSConsensus(assets, log); }
    if (scope === "all") await syncCrypto(assets, log);
    if (["market", "close", "metrics", "score", "briefing", "all"].includes(scope)) { await computeScores(assets, log); await syncBriefingByRules(log); }
  } catch (e) { log.push(`Erro geral: ${(e as Error).message}`); }

  log.push(`Tempo: ${((Date.now() - t0) / 1000).toFixed(0)}s`);
  return new Response(JSON.stringify({ ok: true, scope, avisos: log }), { headers: { "Content-Type": "application/json" } });
});
