export interface Env {
  LOGS_BUCKET: R2Bucket;
  DB: D1Database;
  LOG_PREFIX: string;
  MAX_FILES_PER_RUN: string;
  RISK_THRESHOLD: string; // soglia "a rischio" per media e minimo (default 29)
}

interface Agg {
  count: number;
  sum: number;
  scored: number;
  min: number;
}

interface StatRow {
  user_id: string;
  request_count: number;
  botscore_avg: number | null;
  botscore_min: number | null;
}

interface Filters {
  minCalls: number;
  riskOnly: boolean;
  order: string; // count|avg|min
  limit: number;
}

function riskThreshold(env: Env): number {
  const t = parseInt(env.RISK_THRESHOLD || "29", 10);
  return Number.isFinite(t) ? t : 29;
}

export default {
  async scheduled(_event: ScheduledController, env: Env, ctx: ExecutionContext): Promise<void> {
    ctx.waitUntil(ingest(env));
  },

  async fetch(request: Request, env: Env): Promise<Response> {
    await ensureSchema(env);
    const url = new URL(request.url);

    if (url.pathname === "/ingest") {
      const result = await ingest(env);
      return Response.json(result);
    }

    const filters = parseFilters(url);
    const thr = riskThreshold(env);
    const rows = await queryStats(env, filters, thr);

    if (url.pathname === "/api/stats") {
      return Response.json({ filters, threshold: thr, rows });
    }

    if (url.pathname === "/export.csv") {
      const header = "user_id,request_count,botscore_avg,botscore_min,at_risk\n";
      const body = rows
        .map(r => `${csv(r.user_id)},${r.request_count},${r.botscore_avg ?? ""},${r.botscore_min ?? ""},${isRisk(r, thr) ? "1" : "0"}`)
        .join("\n");
      return new Response(header + body, {
        headers: {
          "content-type": "text/csv; charset=utf-8",
          "content-disposition": 'attachment; filename="botscore-stats.csv"',
        },
      });
    }

    return new Response(renderHtml(rows, filters, thr), {
      headers: { "content-type": "text/html; charset=utf-8" },
    });
  },
};

// ----------------------------------------------------------------------------
// FILTRI
// ----------------------------------------------------------------------------
function parseFilters(url: URL): Filters {
  const order = url.searchParams.get("order") || "count";
  return {
    minCalls: Math.max(0, parseInt(url.searchParams.get("min_calls") || "0", 10) || 0),
    riskOnly: url.searchParams.get("risk") === "1",
    order: ["count", "avg", "min"].includes(order) ? order : "count",
    limit: Math.min(5000, Math.max(1, parseInt(url.searchParams.get("limit") || "500", 10) || 500)),
  };
}

function isRisk(r: StatRow, thr: number): boolean {
  return (r.botscore_avg !== null && r.botscore_avg <= thr) ||
         (r.botscore_min !== null && r.botscore_min <= thr);
}

// ----------------------------------------------------------------------------
// SCHEMA AUTO-INIT
// ----------------------------------------------------------------------------
async function ensureSchema(env: Env): Promise<void> {
  await env.DB.batch([
    env.DB.prepare(
      `CREATE TABLE IF NOT EXISTS user_bot_stats (
         user_id       TEXT PRIMARY KEY,
         request_count INTEGER NOT NULL DEFAULT 0,
         botscore_sum  INTEGER NOT NULL DEFAULT 0,
         scored_count  INTEGER NOT NULL DEFAULT 0,
         botscore_min  INTEGER,
         last_updated  TEXT
       )`
    ),
    env.DB.prepare(
      `CREATE TABLE IF NOT EXISTS processed_objects (
         key          TEXT PRIMARY KEY,
         processed_at TEXT NOT NULL,
         rows         INTEGER
       )`
    ),
    env.DB.prepare(
      `CREATE INDEX IF NOT EXISTS idx_user_bot_stats_count ON user_bot_stats(request_count DESC)`
    ),
  ]);
}

// ----------------------------------------------------------------------------
// INGEST
// ----------------------------------------------------------------------------
async function ingest(env: Env): Promise<{ processedFiles: number; processedRows: number }> {
  await ensureSchema(env);
  const maxFiles = parseInt(env.MAX_FILES_PER_RUN || "15", 10) || 15;
  const prefix = (env.LOG_PREFIX && env.LOG_PREFIX !== "/" && env.LOG_PREFIX !== "*") ? env.LOG_PREFIX : undefined;

  const candidates: string[] = [];
  let cursor: string | undefined;
  while (candidates.length < maxFiles) {
    const listed = await env.LOGS_BUCKET.list({ prefix, cursor, limit: 1000 });
    for (const obj of listed.objects) {
      if (!obj.key.endsWith(".gz")) continue;
      const already = await env.DB
        .prepare("SELECT 1 FROM processed_objects WHERE key = ?")
        .bind(obj.key)
        .first();
      if (!already) candidates.push(obj.key);
      if (candidates.length >= maxFiles) break;
    }
    if (!listed.truncated) break;
    cursor = listed.cursor;
  }

  let processedFiles = 0;
  let processedRows = 0;
  for (const key of candidates) {
    processedRows += await processFile(env, key);
    processedFiles++;
  }
  return { processedFiles, processedRows };
}

async function processFile(env: Env, key: string): Promise<number> {
  const object = await env.LOGS_BUCKET.get(key);
  if (!object || !object.body) return 0;

  const stream = object.body
    .pipeThrough(new DecompressionStream("gzip"))
    .pipeThrough(new TextDecoderStream());

  const reader = stream.getReader();
  const agg = new Map<string, Agg>();
  let buffer = "";
  let rowCount = 0;

  const handleLine = (line: string) => {
    const trimmed = line.trim();
    if (!trimmed) return;
    let rec: any;
    try { rec = JSON.parse(trimmed); } catch { return; }

    const userId = (rec.Cookies && rec.Cookies.cf_user_id) || rec.cf_user_id;
    if (userId === undefined || userId === null || userId === "") return;
    const uid = String(userId);

    const score = typeof rec.BotScore === "number" ? rec.BotScore : Number(rec.BotScore);
    const hasScore = Number.isFinite(score) && score > 0; // 0/assente = non valutato

    const cur = agg.get(uid) ?? { count: 0, sum: 0, scored: 0, min: Number.POSITIVE_INFINITY };
    cur.count += 1;
    if (hasScore) {
      cur.sum += score;
      cur.scored += 1;
      if (score < cur.min) cur.min = score;
    }
    agg.set(uid, cur);
    rowCount++;
  };

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += value;
    let nl: number;
    while ((nl = buffer.indexOf("\n")) >= 0) {
      handleLine(buffer.slice(0, nl));
      buffer = buffer.slice(nl + 1);
    }
  }
  if (buffer.length) handleLine(buffer);

  const now = new Date().toISOString();
  const entries = [...agg.entries()];
  const CHUNK = 50;
  for (let i = 0; i < entries.length; i += CHUNK) {
    const batch = entries.slice(i, i + CHUNK).map(([uid, a]) =>
      env.DB
        .prepare(
          `INSERT INTO user_bot_stats (user_id, request_count, botscore_sum, scored_count, botscore_min, last_updated)
           VALUES (?1, ?2, ?3, ?4, ?5, ?6)
           ON CONFLICT(user_id) DO UPDATE SET
             request_count = request_count + excluded.request_count,
             botscore_sum  = botscore_sum  + excluded.botscore_sum,
             scored_count  = scored_count  + excluded.scored_count,
             botscore_min  = CASE
                               WHEN botscore_min IS NULL THEN excluded.botscore_min
                               WHEN excluded.botscore_min IS NULL THEN botscore_min
                               ELSE MIN(botscore_min, excluded.botscore_min)
                             END,
             last_updated  = excluded.last_updated`
        )
        .bind(uid, a.count, a.sum, a.scored, a.scored > 0 ? a.min : null, now)
    );
    if (batch.length) await env.DB.batch(batch);
  }

  await env.DB
    .prepare("INSERT OR IGNORE INTO processed_objects (key, processed_at, rows) VALUES (?, ?, ?)")
    .bind(key, now, rowCount)
    .run();

  return rowCount;
}

// ----------------------------------------------------------------------------
// QUERY
// ----------------------------------------------------------------------------
async function queryStats(env: Env, f: Filters, thr: number): Promise<StatRow[]> {
  const orderBy =
    f.order === "avg" ? "botscore_avg ASC" :
    f.order === "min" ? "botscore_min ASC" :
    "request_count DESC";

  const riskCond = f.riskOnly
    ? `AND (
         (scored_count > 0 AND CAST(botscore_sum AS REAL) / scored_count <= ${thr})
         OR (botscore_min IS NOT NULL AND botscore_min <= ${thr})
       )`
    : "";

  const res = await env.DB
    .prepare(
      `SELECT
         user_id,
         request_count,
         CASE WHEN scored_count > 0
              THEN ROUND(CAST(botscore_sum AS REAL) / scored_count, 1)
              ELSE NULL END AS botscore_avg,
         botscore_min
       FROM user_bot_stats
       WHERE request_count >= ?1
       ${riskCond}
       ORDER BY ${orderBy}
       LIMIT ?2`
    )
    .bind(f.minCalls, f.limit)
    .all<StatRow>();

  return res.results ?? [];
}

// ----------------------------------------------------------------------------
// RENDER
// ----------------------------------------------------------------------------
function scoreClass(v: number | null, thr: number): string {
  if (v === null) return "na";
  if (v <= thr) return "bot";
  if (v < 70) return "warn";
  return "human";
}

function badge(v: number | null, thr: number): string {
  const cls = scoreClass(v, thr);
  const label = v === null ? "—" : String(v);
  return `<span class="badge ${cls}">${label}</span>`;
}

function renderHtml(rows: StatRow[], f: Filters, thr: number): string {
  const totalUsers = rows.length;
  const totalCalls = rows.reduce((s, r) => s + r.request_count, 0);
  const riskCount = rows.filter(r => isRisk(r, thr)).length;
  const maxCalls = rows.reduce((m, r) => Math.max(m, r.request_count), 0) || 1;

  const body = rows
    .map(r => {
      const pct = Math.max(2, Math.round((r.request_count / maxCalls) * 100));
      const risk = isRisk(r, thr);
      return `<tr class="${risk ? "risk" : ""}"
                  data-user="${esc(r.user_id).toLowerCase()}"
                  data-calls="${r.request_count}"
                  data-avg="${r.botscore_avg ?? -1}"
                  data-min="${r.botscore_min ?? -1}">
        <td class="user">${risk ? '<span class="flag" title="A rischio">&#9888;</span> ' : ""}${esc(r.user_id)}</td>
        <td class="num">
          <div class="callcell">
            <span>${r.request_count.toLocaleString("it-IT")}</span>
            <span class="track"><span class="fill" style="width:${pct}%"></span></span>
          </div>
        </td>
        <td class="num">${badge(r.botscore_avg, thr)}</td>
        <td class="num">${badge(r.botscore_min, thr)}</td>
      </tr>`;
    })
    .join("");

  const sel = (v: string) => (f.order === v ? " selected" : "");

  return `<!doctype html>
<html lang="it"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>BotScore per utente</title>
<style>
  :root{
    --navy:#1a3b68; --blue:#406f9c; --blue-2:#2f5680;
    --ink:#16202e; --muted:#65748a; --line:#e4e9f0; --bg:#f4f6f9;
    --green:#1f8a4c; --green-bg:#e7f4ec;
    --amber:#b8860b; --amber-bg:#fbf3df;
    --red:#c0392b; --red-bg:#fbe9e7;
    --serif:Georgia,"Frank Ruhl Libre","Times New Roman",serif;
    --sans:Lato,system-ui,-apple-system,Segoe UI,Roboto,sans-serif;
  }
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--ink);font-family:var(--sans);font-size:14px;}
  header.top{background:var(--navy);color:#fff;padding:18px 0;border-bottom:4px solid var(--blue);}
  .wrap{max-width:1080px;margin:0 auto;padding:0 22px;}
  header.top h1{font-family:var(--serif);font-weight:900;font-size:26px;margin:0;}
  header.top p{margin:.35rem 0 0;color:#cdd9e8;font-size:13px;}
  main{padding:22px 0 48px;}
  .cards{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:18px;}
  .card{background:#fff;border:1px solid var(--line);border-radius:10px;padding:14px 16px;box-shadow:0 1px 2px rgba(20,40,80,.04);}
  .card .k{color:var(--muted);font-size:12px;text-transform:uppercase;letter-spacing:.5px;}
  .card .v{font-family:var(--serif);font-size:26px;font-weight:900;color:var(--navy);margin-top:4px;}
  .card.alert .v{color:var(--red);}
  form.filters{display:flex;flex-wrap:wrap;gap:12px;align-items:flex-end;background:#fff;border:1px solid var(--line);border-radius:12px;padding:14px 16px;margin-bottom:14px;}
  .field{display:flex;flex-direction:column;gap:4px;}
  .field label{font-size:11px;text-transform:uppercase;letter-spacing:.4px;color:var(--muted);}
  .field input,.field select{padding:8px 10px;border:1px solid var(--line);border-radius:8px;font-size:14px;background:#fff;}
  .chk{display:flex;align-items:center;gap:7px;font-weight:700;color:var(--navy);}
  .chk input{width:16px;height:16px;}
  .toolbar{display:flex;flex-wrap:wrap;gap:10px;align-items:center;margin-bottom:14px;}
  .search{flex:1;min-width:200px;}
  .search input{width:100%;padding:9px 12px;border:1px solid var(--line);border-radius:8px;font-size:14px;}
  .btn{display:inline-flex;align-items:center;gap:6px;text-decoration:none;padding:9px 13px;border-radius:8px;font-size:13px;font-weight:700;border:1px solid var(--line);color:var(--navy);background:#fff;cursor:pointer;}
  .btn:hover{border-color:var(--blue);}
  .btn.primary{background:var(--navy);color:#fff;border-color:var(--navy);}
  .btn.primary:hover{background:var(--blue-2);}
  .panel{background:#fff;border:1px solid var(--line);border-radius:12px;overflow:hidden;box-shadow:0 1px 3px rgba(20,40,80,.05);}
  table{border-collapse:collapse;width:100%;}
  thead th{position:sticky;top:0;background:var(--navy);color:#fff;text-align:left;font-size:12px;text-transform:uppercase;letter-spacing:.4px;padding:11px 14px;cursor:pointer;white-space:nowrap;}
  thead th.num{text-align:right;}
  thead th .arr{opacity:.5;font-size:10px;margin-left:4px;}
  tbody td{padding:11px 14px;border-bottom:1px solid var(--line);}
  tbody tr:nth-child(even){background:#fafcff;}
  tbody tr:hover{background:#eef4fb;}
  tbody tr.risk{background:#fdecea;}
  tbody tr.risk:hover{background:#fbe1dd;}
  tbody tr.risk td:first-child{box-shadow:inset 4px 0 0 var(--red);}
  .flag{color:var(--red);font-weight:900;}
  td.user{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:13px;word-break:break-all;}
  td.num{text-align:right;font-variant-numeric:tabular-nums;}
  .callcell{display:flex;align-items:center;justify-content:flex-end;gap:10px;}
  .track{display:inline-block;width:90px;height:7px;background:#eef1f6;border-radius:6px;overflow:hidden;}
  .fill{display:block;height:100%;background:linear-gradient(90deg,var(--blue),var(--navy));}
  .badge{display:inline-block;min-width:42px;text-align:center;padding:4px 9px;border-radius:999px;font-weight:800;font-size:13px;}
  .badge.human{background:var(--green-bg);color:var(--green);}
  .badge.warn{background:var(--amber-bg);color:var(--amber);}
  .badge.bot{background:var(--red-bg);color:var(--red);}
  .badge.na{background:#eef1f6;color:var(--muted);}
  .legend{display:flex;gap:16px;flex-wrap:wrap;margin:14px 2px 0;color:var(--muted);font-size:12px;}
  .legend span b{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:6px;vertical-align:middle;}
  .foot{color:var(--muted);font-size:12px;margin-top:10px;}
  @media(max-width:760px){.cards{grid-template-columns:repeat(2,1fr);}.track{display:none;}}
</style></head>
<body>
  <header class="top"><div class="wrap">
    <h1>BotScore &amp; chiamate per utente</h1>
    <p>Risalto agli utenti con <strong>media ≤ ${thr}</strong> oppure <strong>minimo ≤ ${thr}</strong> · fonte: log Logpush</p>
  </div></header>

  <main class="wrap">
    <section class="cards">
      <div class="card"><div class="k">Utenti (mostrati)</div><div class="v">${totalUsers.toLocaleString("it-IT")}</div></div>
      <div class="card"><div class="k">Chiamate totali</div><div class="v">${totalCalls.toLocaleString("it-IT")}</div></div>
      <div class="card alert"><div class="k">A rischio (≤ ${thr})</div><div class="v">${riskCount.toLocaleString("it-IT")}</div></div>
      <div class="card"><div class="k">Soglia rischio</div><div class="v">${thr}</div></div>
    </section>

    <form class="filters" method="get" action="/">
      <div class="field"><label>Chiamate minime</label><input type="number" name="min_calls" min="0" value="${f.minCalls}" style="width:130px"></div>
      <div class="field"><label>Ordina per</label>
        <select name="order">
          <option value="count"${sel("count")}>Chiamate (desc)</option>
          <option value="avg"${sel("avg")}>BotScore medio (asc)</option>
          <option value="min"${sel("min")}>BotScore minimo (asc)</option>
        </select>
      </div>
      <div class="field"><label>Limite righe</label><input type="number" name="limit" min="1" max="5000" value="${f.limit}" style="width:120px"></div>
      <label class="chk"><input type="checkbox" name="risk" value="1"${f.riskOnly ? " checked" : ""}> Solo a rischio (≤ ${thr})</label>
      <button class="btn primary" type="submit">Applica filtri</button>
    </form>

    <div class="toolbar">
      <div class="search"><input id="q" type="search" placeholder="Cerca utente (cf_user_id)…" oninput="filterRows()"></div>
      <a class="btn" href="/export.csv${qs(f)}">⬇ Esporta CSV</a>
      <a class="btn" href="/api/stats${qs(f)}">JSON</a>
      <a class="btn" href="/ingest">↻ Esegui ingest</a>
    </div>

    <div class="panel">
      <table id="tbl">
        <thead><tr>
          <th data-col="user" data-type="str" onclick="sortBy(this)">Utente (cf_user_id)<span class="arr"></span></th>
          <th class="num" data-col="calls" data-type="num" onclick="sortBy(this)">Chiamate<span class="arr">▼</span></th>
          <th class="num" data-col="avg" data-type="num" onclick="sortBy(this)">BotScore medio<span class="arr"></span></th>
          <th class="num" data-col="min" data-type="num" onclick="sortBy(this)">BotScore min<span class="arr"></span></th>
        </tr></thead>
        <tbody>${body || `<tr><td colspan="4" style="padding:24px;color:#65748a">Nessun dato. Esegui /ingest per caricare i log.</td></tr>`}</tbody>
      </table>
    </div>

    <div class="legend">
      <span><b style="background:#c0392b"></b>≤ ${thr} (a rischio)</span>
      <span><b style="background:#b8860b"></b>${thr + 1}–69</span>
      <span><b style="background:#1f8a4c"></b>70–99</span>
      <span><b style="background:#aab4c4"></b>non valutato</span>
    </div>
    <p class="foot">BotScore basso = più "bot". Le righe evidenziate hanno media o minimo ≤ ${thr}. Ricerca e ordinamento agiscono sulle righe già caricate.</p>
  </main>

<script>
function filterRows(){
  var q=document.getElementById('q').value.trim().toLowerCase();
  document.querySelectorAll('#tbl tbody tr').forEach(function(tr){
    var u=tr.getAttribute('data-user')||'';
    tr.style.display = !q || u.indexOf(q)>=0 ? '' : 'none';
  });
}
var sortState={col:'calls',dir:-1};
function sortBy(th){
  var col=th.getAttribute('data-col'), type=th.getAttribute('data-type');
  if(!col) return;
  sortState.dir = (sortState.col===col)? -sortState.dir : (type==='num'? -1 : 1);
  sortState.col=col;
  var tb=document.querySelector('#tbl tbody');
  var rows=[].slice.call(tb.querySelectorAll('tr'));
  rows.sort(function(a,b){
    var av=a.getAttribute('data-'+col), bv=b.getAttribute('data-'+col);
    if(type==='num'){av=parseFloat(av);bv=parseFloat(bv);return (av-bv)*sortState.dir;}
    return (av||'').localeCompare(bv||'')*sortState.dir;
  });
  rows.forEach(function(r){tb.appendChild(r);});
  document.querySelectorAll('#tbl thead .arr').forEach(function(s){s.textContent='';});
  var arr=th.querySelector('.arr'); if(arr) arr.textContent = sortState.dir<0?'▼':'▲';
}
</script>
</body></html>`;
}

function qs(f: Filters): string {
  const p = new URLSearchParams({
    min_calls: String(f.minCalls),
    order: f.order,
    limit: String(f.limit),
  });
  if (f.riskOnly) p.set("risk", "1");
  return `?${p.toString()}`;
}

function esc(s: string): string {
  return String(s).replace(/[&<>"]/g, c =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c] as string));
}
function csv(s: string): string {
  const v = String(s);
  return /[",\n]/.test(v) ? `"${v.replace(/"/g, '""')}"` : v;
}
