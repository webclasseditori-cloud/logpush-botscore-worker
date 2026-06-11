# Logpush BotScore Worker

Legge i log Logpush (`.log.gz`, NDJSON gzip) da un bucket R2, estrae `cf_user_id` e
`BotScore`, e produce una tabella per utente: **numero di chiamate**, **BotScore medio** e
**BotScore minimo**. Mette in **risalto gli utenti a rischio**: media ≤ 29 **oppure** minimo ≤ 29
(soglia configurabile). I dati aggregati sono salvati su D1 e visualizzabili via HTML/JSON/CSV.

## Deploy (1 click)

[![Deploy to Cloudflare](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/andreadibiase/logpush-botscore-worker)

Il pulsante crea il repo nel tuo account, provisiona **D1** e un **bucket R2**, e fa il deploy.

### Dopo il deploy (passaggi obbligatori)

1. **Ri-punta il bucket R2 esistente**: Dashboard → Workers & Pages → `logpush-botscore-worker`
   → Settings → Bindings → `LOGS_BUCKET` → Edit → seleziona il bucket Logpush esistente → Save.
2. **Variables** (Settings → Variables):
   - `LOG_PREFIX`: sottocartella dei log nel bucket (vuoto = radice).
   - `MAX_FILES_PER_RUN`: default `15`.
   - `RISK_THRESHOLD`: soglia "a rischio" per media e minimo (default `29`).
3. **Redeploy** per applicare il binding.
4. **Backfill**: apri `https://<worker>.<sottodominio>.workers.dev/ingest` più volte
   finché risponde `processedFiles: 0`. Poi il cron (ogni 5 min) tiene aggiornato.

## Visualizzazione

- `/` tabella HTML con filtri (chiamate minime, ordina per, solo a rischio) + ricerca/ordinamento
- `/api/stats` JSON (stessi filtri)
- `/export.csv` CSV (include colonna `at_risk`)
- `/ingest` esegue subito un ciclo di ingest

## Filtri

- **Chiamate minime** e **limite righe** (server-side)
- **Ordina per**: chiamate / BotScore medio / BotScore minimo
- **Solo a rischio**: mostra solo utenti con media ≤ soglia o minimo ≤ soglia
- **Ricerca `cf_user_id`** live + **ordinamento colonna** (client-side)

## Risalto "a rischio"

Una riga è evidenziata (sfondo rosso + ⚠ + bordo) quando:
`BotScore medio ≤ RISK_THRESHOLD` **oppure** `BotScore minimo ≤ RISK_THRESHOLD` (default 29).
Le celle BotScore usano un semaforo: rosso ≤ soglia, giallo soglia+1–69, verde 70–99.

## Note

- `BotScore` mancante o `0` = "non valutato": conta nelle chiamate totali ma è escluso da media/minimo.
- Le tabelle D1 si creano da sole al primo avvio (`ensureSchema`), niente SQL da eseguire.
- Se nei tuoi log i campi non si chiamano `cf_user_id` / `BotScore`, modifica le due righe
  in `src/index.ts` (funzione `processFile`) e fai push.

## Deploy via terminale (alternativa al pulsante)

```bash
npm install
wrangler d1 create botscore-db        # incolla database_id in wrangler.jsonc
# imposta bucket_name (R2) e LOG_PREFIX in wrangler.jsonc
wrangler deploy
```

## Caricamento manuale su GitHub

```bash
git init
git add .
git commit -m "Logpush BotScore Worker"
git branch -M main
git remote add origin https://github.com/andreadibiase/logpush-botscore-worker.git
git push -u origin main
```
