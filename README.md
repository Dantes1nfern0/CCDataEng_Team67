
# Broken Alibi pipeline

Rebuilds what happened at Blackwood HQ between 10 PM and midnight on Aug 14, 2026, from three systems that don't talk to each other. It checks the three alibis against the data and proves no record changed between loading and querying.

## Run it

You need Python 3.10 or newer.

```bash
cd pipeline
pip install -r requirements.txt     # just DuckDB
python run_pipeline.py --reset      # full run from scratch
python run_pipeline.py --tamper-demo  # same, plus a live demo of the tamper check
```

It reads the sample files in `../data/raw/` and never changes them. Everything it makes goes into `output/`. A run takes a few seconds.

To point it at other data: `python run_pipeline.py --source-dir /path/to/files --out-dir /path/to/output`. The folder needs `badge_access.csv`, `device_logs.log`, `transactions.csv` and (optionally) `SHA256SUMS`.

## What you get in `output/`

| File | What it is |
|---|---|
| `investigation_report.md` | The answer in plain English: custody checks, the timeline, and a verdict per person |
| `timeline.csv` | Every event 21:45 to 00:15 UTC for the four people, with its fingerprint and custody check |
| `alibi_findings.csv` | Each contradiction, which rule found it, and the record that proves it |
| `custody_report.json` | Results of every hash check |
| `audit_log.jsonl` | Append-only log of every load, transform and query, each entry chained to the one before |
| `warehouse.duckdb` | The database. Open it with `duckdb output/warehouse.duckdb` to run your own SQL |
| `raw_zone/` | Untouched copies of the evidence, plus a manifest of hashes for each file |

## How it works (the 4 layers)

```
SOURCES               1. INGESTION                 3. STORAGE                     4. SERVING
                                                    ┌─────────────────────┐
Badge CSV  ──file──▶  nightly batch loader  ──┐     │ RAW ZONE (write-once)│
(nightly)             checks vendor hash      │     │ exact bytes          │
                                              ├────▶│ + SHA-256 per file   │
Device     ──lines─▶  stream consumer       ──┤     │ + SHA-256 per record │
syslog                commits an offset       │     └─────────┬───────────┘
(streaming)                                   │               │ 2. CLEAN + NORMALIZE
Postgres   ──change─▶ change data capture   ──┘               │ parse, UTC, map IDs,
txns         events   commits a position                      │ zones, dedupe, quarantine
(live)                                                ┌───────▼───────────┐     ┌──────────────────┐
                                                      │ CURATED            │────▶│ 01_timeline.sql  │
                                                      │ employee_activity_ │     │ 02_alibi_checks  │
                                                      │ timeline           │     │ + re-hash check  │
                                                      └────────────────────┘     └──────────────────┘
                      Audit log: every step above is written to a hash-chained, append-only log
```

### 1. Ingestion: one path per source (`blackwood/ingest.py`)

Each source delivers data a different way, so each gets its own loader. Locally they're simulated; the comments say what each stands in for.

- **Badge (nightly CSV batch).** The loader picks up the night's file, checks it against the vendor's published SHA-256 in `SHA256SUMS`, and copies it byte for byte into the raw zone. Running it twice does nothing the second time. In production: a scheduled job (cron or Airflow) reading the vendor's drop folder.
- **Device logs (streaming syslog).** `syslog_stream()` replays the log file line by line, the way a Kafka topic or Fluent Bit forwarder would deliver it. The consumer writes new lines as a closed segment file, then commits its offset in `output/state/checkpoints.json`. On the next run it starts where it stopped, so no line is read twice or skipped.
- **Transactions (live Postgres, change data capture).** A small SQLite database (`output/sim_sources/pos_postgres_stand_in.sqlite`) plays the live payment system. Triggers write every insert, update and delete into a numbered change log, which is how Debezium reads the Postgres write-ahead log. The reader takes changes after its last committed position and never queries the live table. If someone edits a payment after the fact, the edit arrives as a new change event, and the timeline row says "(edited later in the payment system)" with the before and after values.

Each record is hashed (SHA-256) before anything else touches it.

### 2. Cleaning and normalization (`blackwood/clean.py`)

| Problem in the data | What the pipeline does |
|---|---|
| Two timestamp formats (`22:14:58Z` with a space, `T22:33:00Z` with a T) | Both become one UTC timestamp. The original string is kept in `raw_timestamp`. A timestamp with no time zone is rejected, not guessed. |
| A person named two ways (`user=EMP-0047`, `LAPTOP-EMP0047`) | Joins on `user`. The hostname becomes the location. A lookup table checks the badge or device is registered to that person. |
| The brief says `event_code`, the logs say `event=` | Both map to `event_type`. |
| Optional fields (`path=`, `to=external`) | Kept in a JSON `details` column. |
| IP `10.0.7.03` | Stored as `10.0.7.3`; the raw value stays in `details`. |
| Door and terminal codes with no shared names | `reference/locations.csv` maps each to a floor and zone. |
| Lines that don't parse | Go to `curated.quarantine` with the error. Nothing is dropped silently. |
| Duplicates | One row per source record (by `txn_id`, or by hash when there's no ID). For edited payments, the latest version wins. |

### 3. Storage: raw and curated kept apart (`blackwood/rawzone.py`, `blackwood/warehouse.py`)

- **Raw zone** (`output/raw_zone/`). Files are created in exclusive mode, so an existing file can never be overwritten. Each one has a manifest with its hash and one hash per record. Locally this is a folder. In production it's S3 with Object Lock in compliance mode, where nobody, admins included, can edit or delete.
- **Curated** (`curated.employee_activity_timeline` in DuckDB). One table, one shape, for all three sources. Every row carries `raw_event_hash` and a pointer to the raw file and line it came from. Curated is rebuilt from raw on every run. If any raw record no longer matches its hash, the rebuild stops.

### 4. Serving (`sql/`)

- `sql/01_timeline.sql` answers the question: every event for the four people between 22:00 and 00:00 UTC, plus 15 minutes either side so a 00:03 exit isn't cut off.
- `sql/02_alibi_checks.sql` stores each alibi as data (`reference/alibi_claims.csv`) and runs one rule per claim, plus checks for everyone (device use before badging in, the car leaving before the badge, laptop use after badging out, sensitive actions).
- `sql/00_clock_confidence.sql` marks events whose order is uncertain. See "Clocks" below.

Every query is written to the audit log with a hash of the SQL and a hash of the result.

## Chain of custody

Each run ends with four checks (`blackwood/custody.py`):

1. Source files still match their published hashes.
2. Every raw zone file and every record still matches the hash taken when it was loaded.
3. Every row in the answer is re-read from the raw zone, re-hashed, and compared with its `raw_event_hash`.
4. The audit log's chain is intact. Each entry holds the hash of the one before it, so editing or deleting an entry breaks the chain.

`--tamper-demo` copies the raw zone, moves EMP-0031's server room exit from 23:07:44 to 22:57:44 (to fit their alibi), and shows that the file, the line and the timeline row all fail, and that the pipeline refuses to rebuild from it. The real raw zone isn't touched.

You can also check by hand:

```bash
cd ../data/raw && sha256sum -c SHA256SUMS
echo -n 'BDG-2205,EMP-0047,D-EXEC-3F,2026-08-14 22:31:12Z,IN' | sha256sum   # matches that row's raw_event_hash
```

## Clocks

The parking gate says EMP-0031's car left at 22:59:31, but the badge says they left the server room at 23:07:44. You have to badge out before you drive out, so the clocks disagree by up to 8m 13s. EMP-0011 shows the same pattern with a 27 second gap. The gap changes, so no single correction fixes it.

The pipeline doesn't guess. It uses the biggest gap (493 seconds) as the tolerance between badge and payment clocks, and 120 seconds for pairs with no evidence (set in `blackwood/config.py`). Any two events for the same person closer than that are marked `clock_confidence = low`, with a note saying which event they could swap with.

## Assumptions to say out loud

- **Time zone.** The data is stamped UTC (`Z`), and we read "10 PM to midnight" as UTC. If the brief means Oakville time (UTC-4 in August), change `WINDOW_START` and `WINDOW_END` in `blackwood/config.py` to 02:00 and 04:00 on Aug 15. On the sample data that window is empty.
- **Locations.** The brief gives door and terminal codes only. `reference/locations.csv` lists what we assumed for each, for example that Stairwell B is a side entrance and the 3F door is the restricted executive suite.
- **Badge arrival.** We assume the nightly badge export lands at 02:00 UTC the next day. `available_at` on each row shows when an investigator could first have seen it.
- **Sample size.** This runs on the 20 sample rows from the brief. The code doesn't assume that size, but a real dataset would need the production pieces listed below.

## What runs locally vs in production

| Here | In production |
|---|---|
| Folder with exclusive-create files | S3 with Object Lock (compliance mode) |
| Replay of `device_logs.log` | Kafka topic or Fluent Bit, same offset logic |
| SQLite with change-log triggers | Debezium on the Postgres write-ahead log |
| DuckDB file | BigQuery, Snowflake or Postgres |
| `audit_log.jsonl` | Append-only table or a ledger database, with the latest chain hash also stored off-site |

## Files

```
run_pipeline.py              one command, runs everything
blackwood/config.py          window, tolerances, paths
blackwood/ingest.py          layer 1: batch, stream and CDC loaders
blackwood/clean.py           layer 2: parsing and normalization
blackwood/rawzone.py         layer 3: write-once raw zone and manifests
blackwood/warehouse.py       layer 3/4: DuckDB tables and query serving
blackwood/custody.py         hash checks at query time
blackwood/audit.py           hash-chained audit log
sql/00_clock_confidence.sql  clock tolerance and low-confidence flags
sql/01_timeline.sql          the investigation query
sql/02_alibi_checks.sql      contradiction rules
reference/*.csv              employees, locations, alibi claims
```
