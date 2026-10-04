# How the pipeline works, step by step

This follows one person, EMP-0031, through the whole pipeline. Every row, hash and message below is copied from a real run (`python run_pipeline.py --reset`). Their alibi: *"I was in the server room fixing a batch job. Left before 11 PM."*

Three systems hold evidence about them. Each one writes it differently.

---

## Step 0: What the raw data looks like

**Badge system** (a CSV file, exported once a night). It has fixed columns:

```
badge_id,employee_id,door_id,timestamp,direction
BDG-0113,EMP-0031,D-SERVER,2026-08-14 22:44:07Z,IN
BDG-0113,EMP-0031,D-SERVER,2026-08-14 23:07:44Z,OUT
```

**Device logs** (one line of text per event, arriving all the time). No columns, just `key=value` pairs:

```
2026-08-14T22:41:22Z WORKSTATION-EMP0031 user=EMP-0031 event=FILE_ACCESS action=read src=10.0.7.03 path=/finance/audit_2025.xlsx
```

**Payment database** (live; parking gates and vending machines write to it):

```
txn_id,employee_id,amount,timestamp,terminal_id
TXN-88172,EMP-0031,0.00,2026-08-14 22:59:31Z,PARKING-EXIT-B
```

Notice the mess. The badge time has a space (`2026-08-14 22:44:07Z`) while the device log uses a `T` (`2026-08-14T22:41:22Z`). The device names the person twice (`EMP-0031` and `WORKSTATION-EMP0031`). The IP address has an odd leading zero (`10.0.7.03`). And none of the three systems uses the same place names.

---

## Step 1: Ingest, each source its own way

Each source gets a loader that matches how it delivers data.

| Source | How it arrives | What the loader does |
|---|---|---|
| Badge | One file a night | Picks up the file, checks it against the vendor's published fingerprint, copies it unchanged |
| Device logs | A constant stream | Reads new lines as they come and remembers where it stopped (line 4 of the stream so far) |
| Payments | A live database | Watches the database's change log, so edits made later are caught too (change number 5 so far) |

The payment loader never reads the live table directly. It reads a list of changes. Here's the change it captured for EMP-0031's parking exit, exactly as stored:

```json
{"after":{"amount":"0.00","employee_id":"EMP-0031","terminal_id":"PARKING-EXIT-B","timestamp":"2026-08-14 22:59:31Z","txn_id":"TXN-88172"},"before":null,"lsn":3,"op":"c", ...}
```

`"op":"c"` means "created". If someone edited this payment tomorrow, a second change with `"op":"u"` (updated) would arrive, holding both the old and the new values.

---

## Step 2: Fingerprint every record

Before anything else happens, each record gets a SHA-256 fingerprint. Change even one character and the fingerprint changes completely.

| Record | Fingerprint (SHA-256) |
|---|---|
| `BDG-0113,EMP-0031,D-SERVER,2026-08-14 23:07:44Z,OUT` | `6818d633…ce3e864` |
| The same line with `22:57:44` instead of `23:07:44` | `b2b857e1…d5907b0` |

That second row shows why this matters. Someone who moved EMP-0031's exit 10 minutes earlier to fit their alibi would produce a totally different fingerprint.

Whole files get fingerprinted too. The badge file's fingerprint (`acf4727c…`) matches the one published in `SHA256SUMS`, so you know it arrived as it was sent.

---

## Step 3: Store the originals where nobody can change them

The untouched records go into the **raw zone** (`output/raw_zone/`), one folder per source:

```
raw_zone/badge/dt=2026-08-14/badge_access_2026-08-14.csv
raw_zone/device/dt=2026-08-14/segment-00001.log
raw_zone/txn/dt=2026-08-14/changes-00000001-00000005.jsonl
```

Next to each file sits a **manifest**, a receipt listing the fingerprint of every line. This is the entry for EMP-0031's badge-out:

```
line 6  ->  6818d6335189416b4560bb98c422190b5c47b57ed09c6c72a744e30c8ce3e864
```

A raw file can never be overwritten. If the pipeline tries to save different bytes under the same name, it refuses and logs the attempt. In a real deployment this folder would be S3 storage with Object Lock, where not even an admin can edit or delete.

---

## Step 4: Clean, so all three sources look the same

The cleaning step reads from raw (it never changes it) and turns each record into a row with the same columns. Here's what it does to EMP-0031's device line:

| Field | Raw | Cleaned |
|---|---|---|
| Time | `2026-08-14T22:41:22Z` | `2026-08-14 22:41:22` UTC (the original is kept too) |
| Person | `user=EMP-0031` and `WORKSTATION-EMP0031` | `EMP-0031`, and the lookup table confirms the workstation is theirs |
| What happened | `event=FILE_ACCESS action=read` | `FILE_ACCESS`: "Workstation read file /finance/audit_2025.xlsx" |
| IP | `10.0.7.03` | `10.0.7.3` (the raw value is kept in `details`) |
| Fingerprint | | `5309670c…` (a link back to the raw line) |

Door and terminal codes become plain names through a lookup table: `D-SERVER` becomes "Server room" and `PARKING-EXIT-B` becomes "Parking exit B".

A line the cleaner can't read goes to a quarantine table with the reason. It's never silently dropped. On this data, nothing was quarantined.

---

## Step 5: Merge into one timeline

All three sources now land in one table, `curated.employee_activity_timeline`. These are EMP-0031's four rows:

| Time (UTC) | Source | What happened | Clock | Fingerprint | Comes from |
|---|---|---|---|---|---|
| 22:41:22 | device | Workstation read file /finance/audit_2025.xlsx | high | `5309670c…` | device segment, line 2 |
| 22:44:07 | badge | Badge IN at Server room | high | `497e4700…` | badge file, line 5 |
| 22:59:31 | payment | Car left through Parking exit B | **low** | `bd45fb32…` | payment changes, line 3 |
| 23:07:44 | badge | Badge OUT at Server room | **low** | `6818d633…` | badge file, line 6 |

**Why two rows say "low".** You have to badge out of a room before you can drive out of the garage. But here the car leaves at 22:59:31 and the badge leaves at 23:07:44, 493 seconds (8m 13s) later. So either the badge clock and the parking clock disagree, or someone else used the badge or the parking pass. The pipeline doesn't pick a side. It marks both rows "low" and adds a note: *"Order uncertain vs badge BADGE_OUT at 23:07:44 (493s apart)"*.

---

## Step 6: Ask the question

The investigator runs one query (`sql/01_timeline.sql`):

```sql
SELECT time_utc, employee_id, event_source, description, clock_confidence, raw_event_hash
FROM curated.employee_activity_timeline
WHERE event_timestamp BETWEEN '2026-08-14 22:00:00Z' AND '2026-08-15 00:00:00Z'
  AND employee_id IN ('EMP-0047', 'EMP-0031', 'EMP-0092', 'EMP-0011')
ORDER BY event_timestamp;
```

(Simplified for reading. The real file also pulls 15 minutes either side of the window, so an exit at 00:03 isn't cut off.)

It returns 16 events inside 10 PM to midnight, plus 4 just outside. The full list is in [investigation_report.md](output/investigation_report.md).

---

## Step 7: Check the alibi against the data

Each alibi is stored as data. EMP-0031's becomes two claims the computer can test:

| Claim | Stored as |
|---|---|
| "Left before 11 PM" | `LEFT_BEFORE`, `2026-08-14 23:00` |
| "In the server room" | `IN_ZONE`, `Server room` |

The rules in `sql/02_alibi_checks.sql` then return:

| Severity | What the data says |
|---|---|
| **CONTRADICTS** | Said they left before 23:00, but at 23:07:44: Badge OUT at Server room. |
| SUSPICIOUS | Their car left the garage at 22:59:31, 8m 13s before their badge left Server room at 23:07:44. Either the clocks disagree or someone else used the badge or the parking pass. |
| SUSPICIOUS | Workstation read file /finance/audit_2025.xlsx at 22:41:22. |
| SUSPICIOUS | Workstation read file /finance/audit_2025.xlsx at 22:41:22, 2m 45s before their first badge-in (Server room at 22:44:07). That first badge is an inside door, so their building entry is missing from the data. |
| INFO | Badge puts them in the Server room from 22:44:07 to 23:07:44. |

These lines are copied from `output/alibi_findings.csv`. Reading a finance file also doesn't fit "fixing a batch job", though no rule can judge that on its own; that part is for the investigator.

**Verdict: contradicted by the data.** Every line points to the fingerprint of the record that proves it, so you can trace any finding back to the original evidence.

---

## Step 8: Prove nothing was changed

At the end of every run, the pipeline re-checks everything:

```
- Source files vs published hashes: badge_access.csv OK, transactions.csv OK, device_logs.log OK
- Raw zone files vs hashes taken at ingestion: all 3 OK
- Rows in this answer re-hashed against raw: 20 of 20 OK
- Audit log hash chain: OK
```

The audit log records every step (load, clean, query) along with the fingerprint of the entry before it. Delete or edit one entry and the chain breaks.

**What tampering looks like.** `python run_pipeline.py --tamper-demo` makes a copy of the raw data and moves EMP-0031's exit from 23:07:44 to 22:57:44, just as a forger would to fit the alibi. The output:

```
raw file badge/dt=2026-08-14/badge_access_2026-08-14.csv: FAILED (altered lines [6])
raw file device/dt=2026-08-14/segment-00001.log: OK
raw file txn/dt=2026-08-14/changes-00000001-00000005.jsonl: OK
answer row badge-6818d6335189416b: FAILED: raw record no longer matches the hash on this row
19 other rows still OK
rebuilding the timeline from this copy is refused
```

It catches the exact file, the exact line and the exact timeline row, and it won't build a timeline from altered evidence. The real data is never touched.

---

## The whole flow in one picture

```
Badge CSV ─────▶ nightly loader ──┐
Device logs ───▶ stream reader  ──┼──▶ fingerprint ──▶ raw zone ──▶ clean ──▶ one timeline ──▶ query + alibi checks
Payments DB ───▶ change capture ──┘     (SHA-256)     (locked)    (same columns)  (fingerprint     ──▶ re-check fingerprints
                                                                                    on every row)
```
