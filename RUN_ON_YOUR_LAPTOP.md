# Run the pipeline on your own laptop

This takes about 10 minutes the first time. You type 5 commands. Steps are for a Mac, and Windows differences are noted where they come up.

## 1. Download and unzip

Download `broken-alibi-pipeline.zip` from the project files (it's in the `downloads` folder) and double-click it. You'll get one folder:

```
broken-alibi/
  data/raw/      the 20 sample rows from the brief, plus their fingerprints (SHA256SUMS)
  pipeline/      the code
```

Move the folder somewhere easy to find, like your Desktop.

## 2. Check you have Python

Open **Terminal**: press Cmd + Space, type `Terminal`, press Enter. Then type:

```bash
python3 --version
```

If it prints `Python 3.10` or higher, skip to step 3. If you get an error or an older version, install Python from https://www.python.org/downloads/ (click the big yellow button, run the installer), then close and reopen Terminal.

**Windows:** install from the same page. On the installer's first screen, tick **"Add python.exe to PATH"**. Use **PowerShell** instead of Terminal, and type `py` wherever this guide says `python3`.

## 3. Go into the pipeline folder

Type `cd ` (with a space after it), drag the `pipeline` folder from Finder into the Terminal window, then press Enter. That fills in the path for you. It ends up looking like this:

```bash
cd /Users/abdalla/Desktop/broken-alibi/pipeline
```

## 4. Install the one thing it needs

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

The first line creates a private Python setup just for this project, so nothing else on your laptop changes. The second turns it on (you'll see `(.venv)` at the start of the line). The third installs DuckDB, the small database the pipeline uses.

**Windows:** the second line is `.venv\Scripts\activate`. If PowerShell refuses to run it, first run `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` and answer `Y`.

Next time you open Terminal, you only need `cd` into the folder and `source .venv/bin/activate`. You don't need to install again.

## 5. Run it

```bash
python run_pipeline.py --reset
```

You should see this:

```
1. Ingest (one path per source)
   badge   10 new records
   device  5 new records
   txn     5 new records
2. Clean and normalize, 3. Store (raw zone -> curated table)
   20 timeline rows, 0 quarantined, 0 duplicates dropped
4. Serve: timeline query and alibi checks
   20 events (16 inside 22:00-00:00), 15 alibi findings
5. Verify chain of custody
   all checks passed: 20/20 rows match raw, audit chain OK

Verdicts:
   EMP-0011: No alibi, and the activity needs explaining
   EMP-0031: Contradicted by the data
   EMP-0047: Doesn't fully hold up
   EMP-0092: Doesn't fully hold up
```

The results are now in `pipeline/output/`:

- `investigation_report.md`: the full answer in plain English. Open it in any text editor, or in VS Code for a nicer view.
- `timeline.csv` and `alibi_findings.csv`: open them in Excel or Numbers.

## 6. Run the tamper demo

```bash
python run_pipeline.py --tamper-demo
```

It copies the stored evidence, moves EMP-0031's exit from 23:07:44 to 22:57:44, and shows that the pipeline catches it:

```
  raw file badge/dt=2026-08-14/badge_access_2026-08-14.csv: FAILED (altered lines [6])
  answer row badge-6818d6335189416b: FAILED: raw record no longer matches the hash on this row
  19 other rows still OK
  rebuilding the timeline from this copy is refused
```

Your real files stay untouched.

## 7. Test it yourself

Each of these takes a minute and gives you something to show the judges.

**a. Tamper with the source file for real.** Open `data/raw/badge_access.csv` in a text editor, change `23:07:44` to `22:57:44`, save, and run `python run_pipeline.py --reset`. The pipeline stops before loading anything:

```
STOPPED: badge_access.csv: hash 70ff40546e05 does not match the published acf4727ced34
The evidence does not match its fingerprint, so the pipeline refuses to load it.
```

Change it back to `23:07:44`, save, and it runs again. To check the fingerprints by hand: `cd ../data/raw` then `shasum -a 256 -c SHA256SUMS` (on Windows: `Get-FileHash badge_access.csv`).

**b. Run it twice.** Run `python run_pipeline.py` again, without `--reset`. Each source reports `0 new records`, because every loader remembers where it stopped. Real nightly, streaming and database loaders work the same way.

**c. Ask your own question.** For example, everything EMP-0047 did:

```bash
python -c "import duckdb; duckdb.connect('output/warehouse.duckdb').sql(\"SELECT strftime(event_timestamp, '%H:%M:%S') AS time, employee_id, description FROM curated.employee_activity_timeline WHERE employee_id = 'EMP-0047' ORDER BY event_timestamp\").show()"
```

You get a 6-row table, from `22:31:12 Badge IN at Executive suite (3F)` to `23:58:02 Laptop logout`. Swap in any employee ID.

**d. Change an alibi.** Open `reference/alibi_claims.csv` and change EMP-0031's `2026-08-14 23:00:00+00` to `2026-08-14 23:30:00+00`, as if they'd said "left before 11:30". Run it again and their verdict drops from "Contradicted" to "Doesn't fully hold up", because the 23:07 badge-out now fits the claim. Change it back afterwards.

## If something goes wrong

| You see | Fix |
|---|---|
| `command not found: python3` | Install Python (step 2), then reopen Terminal |
| `No module named duckdb` | You forgot `source .venv/bin/activate`. Run it, then try again |
| `No such file or directory: run_pipeline.py` | You're in the wrong folder. Redo step 3 |
| `STOPPED: ... does not match the published ...` | A file in `data/raw` was changed. Put it back, or download the zip again |
| Odd results after experimenting | Run with `--reset` to start clean |
