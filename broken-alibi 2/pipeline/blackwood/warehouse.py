"""Layer 3: storage (raw and curated kept apart) and Layer 4: serving.

The DuckDB file holds three schemas:
  ref      lookup tables: employees, door and terminal locations, alibi claims
  raw      an index of every raw record (exact bytes plus hash) read from the write-once raw zone
  curated  employee_activity_timeline (the one table investigators query), quarantine,
           clock evidence and alibi findings

Curated is rebuilt from raw on every run. Raw is never rebuilt, only read."""
import hashlib
import json
from datetime import datetime
from pathlib import Path

import duckdb

from . import clean
from .audit import AuditLog
from .config import (DEFAULT_CLOCK_TOLERANCE_SECONDS, REFERENCE_DIR, SQL_DIR, WINDOW_END,
                     WINDOW_MARGIN_MINUTES, WINDOW_START)
from .rawzone import RawZone, sha256_bytes, split_records


class RawIntegrityError(Exception):
    """Raised when a raw file no longer matches the hash taken at ingestion. The timeline is not built."""
    pass


TIMELINE_DDL = """
CREATE TABLE curated.employee_activity_timeline (
    event_id          VARCHAR PRIMARY KEY,  -- '<source>-<first 16 chars of raw hash>'
    event_source      VARCHAR NOT NULL,     -- badge / device / txn
    source_record_id  VARCHAR NOT NULL,     -- txn_id, or the raw hash when the source has no ID
    employee_id       VARCHAR NOT NULL,
    event_timestamp   TIMESTAMPTZ NOT NULL, -- normalized UTC
    raw_timestamp     VARCHAR NOT NULL,     -- exactly as the source wrote it
    event_type        VARCHAR NOT NULL,     -- BADGE_IN, LOGIN, USB_INSERTED, PURCHASE, PARKING_EXIT ...
    location          VARCHAR,              -- door, device or terminal code
    zone              VARCHAR,              -- from ref.locations
    floor             VARCHAR,
    description       VARCHAR,              -- plain-English line for the investigator
    details           JSON,                 -- extra fields that vary by event
    raw_event_hash    VARCHAR NOT NULL,     -- SHA-256 of the raw record; proves the row matches the evidence
    raw_file          VARCHAR NOT NULL,     -- where that raw record lives in the raw zone
    raw_line_no       INTEGER NOT NULL,
    ingestion_path    VARCHAR NOT NULL,     -- nightly_batch / stream_consumer / change_data_capture
    ingested_at       TIMESTAMPTZ NOT NULL, -- when the pipeline stored the raw record
    available_at      TIMESTAMPTZ NOT NULL, -- when an investigator could first have seen it
    clock_confidence  VARCHAR DEFAULT 'high', -- low = could be in a different order vs a nearby event
    clock_note        VARCHAR
);
"""


def connect(db_path: Path) -> duckdb.DuckDBPyConnection:
    """Open the DuckDB database at `db_path` and set it up for this pipeline.

    Sets the time zone to UTC and stores the investigation window, margin and
    default clock tolerance as SQL variables that the files in sql/ read.
    """
    con = duckdb.connect(str(db_path))
    con.execute("SET TimeZone = 'UTC'")
    con.execute(f"SET VARIABLE window_start = TIMESTAMPTZ '{WINDOW_START}'")
    con.execute(f"SET VARIABLE window_end = TIMESTAMPTZ '{WINDOW_END}'")
    con.execute(f"SET VARIABLE margin_minutes = {int(WINDOW_MARGIN_MINUTES)}")
    con.execute(f"SET VARIABLE default_tolerance = {int(DEFAULT_CLOCK_TOLERANCE_SECONDS)}")
    return con


def load_reference(con):
    """Load the three reference CSVs (employees, locations, alibi claims) into the ref schema."""
    con.execute("CREATE SCHEMA IF NOT EXISTS ref")
    con.execute(f"""CREATE OR REPLACE TABLE ref.employees AS
        SELECT * FROM read_csv('{REFERENCE_DIR / 'employees.csv'}', header=true, all_varchar=true)""")
    con.execute(f"""CREATE OR REPLACE TABLE ref.locations AS
        SELECT code, kind, floor, zone, CAST(is_building_entrance AS BOOLEAN) AS is_building_entrance,
               CAST(restricted AS BOOLEAN) AS restricted, assumption
        FROM read_csv('{REFERENCE_DIR / 'locations.csv'}', header=true, all_varchar=true)""")
    con.execute(f"""CREATE OR REPLACE TABLE ref.alibi_claims AS
        SELECT employee_id, alibi_text, claim_type, CAST(claim_time AS TIMESTAMPTZ) AS claim_time, claim_value
        FROM read_csv('{REFERENCE_DIR / 'alibi_claims.csv'}', header=true, all_varchar=true)""")


def index_raw_zone(con, raw: RawZone, audit: AuditLog) -> list[dict]:
    """Read every raw file, check it against the hashes taken at ingestion, and index
    each record. Any mismatch stops the build: we never curate from altered evidence."""
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute("""CREATE OR REPLACE TABLE raw.records (
        source VARCHAR, raw_file VARCHAR, line_no INTEGER, raw_record VARCHAR, raw_event_hash VARCHAR,
        ingestion_path VARCHAR, ingested_at TIMESTAMPTZ, source_position VARCHAR)""")
    out = []
    for m in raw.manifests():
        data = (raw.root / m["file"]).read_bytes()
        if sha256_bytes(data) != m["sha256"]:
            audit.append("raw_integrity_failed", file=m["file"], level="file")
            raise RawIntegrityError(f"{m['file']} no longer matches the hash taken at ingestion")
        lines = split_records(data)
        for rec in m["records"]:
            line = lines[rec["line_no"] - 1]
            if sha256_bytes(line) != rec["raw_event_hash"]:
                audit.append("raw_integrity_failed", file=m["file"], line_no=rec["line_no"])
                raise RawIntegrityError(f"{m['file']} line {rec['line_no']} was altered")
            position = rec.get("stream_offset", rec.get("lsn", rec["line_no"]))
            row = {"source": m["source"], "raw_file": m["file"], "line_no": rec["line_no"],
                   "raw_record": line.decode("utf-8"), "raw_event_hash": rec["raw_event_hash"],
                   "ingestion_path": m["ingestion_path"], "ingested_at": m["ingested_at"],
                   "source_position": str(position), "_manifest": m}
            out.append(row)
    con.executemany("INSERT INTO raw.records VALUES (?,?,?,?,?,?,?,?)",
                    [[r[k] for k in ("source", "raw_file", "line_no", "raw_record", "raw_event_hash",
                                     "ingestion_path", "ingested_at", "source_position")] for r in out])
    audit.append("raw_zone_indexed", files=len(raw.manifests()), records=len(out))
    return out


def build_curated(con, raw_rows: list[dict], audit: AuditLog) -> dict:
    """Clean every raw record and build curated.employee_activity_timeline from scratch.

    Records that fail to parse go to curated.quarantine with the error.
    Duplicates are dropped, then sql/00_clock_confidence.sql marks events
    whose order is uncertain.

    Returns:
        Counts of curated rows, quarantined records and duplicates dropped.
    """
    employees = [dict(zip(["employee_id", "badge_id", "hostname"], r))
                 for r in con.execute("SELECT employee_id, badge_id, hostname FROM ref.employees").fetchall()]
    locations = [dict(zip(["code", "floor", "zone"], r))
                 for r in con.execute("SELECT code, floor, zone FROM ref.locations").fetchall()]
    ref = clean.Reference(employees, locations)

    rows, quarantine = [], []
    for r in raw_rows:
        m = r["_manifest"]
        try:
            if r["source"] == "badge":
                row = clean.clean_badge(r, ref, m["header"])
            elif r["source"] == "device":
                row = clean.clean_device(r, ref)
            else:
                row = clean.clean_txn(r, ref)
        except (clean.ParseError, KeyError, ValueError, json.JSONDecodeError) as e:
            quarantine.append([r["source"], r["raw_file"], r["line_no"], r["raw_record"], r["raw_event_hash"], str(e)])
            continue
        if not row.get("_deleted"):
            row["available_at"] = clean.available_at(row["event_source"], row["event_timestamp"], m.get("export_day"))
        rows.append(row)

    rows, dropped = clean.dedupe(rows)

    con.execute("CREATE SCHEMA IF NOT EXISTS curated")
    con.execute("DROP TABLE IF EXISTS curated.employee_activity_timeline")
    con.execute(TIMELINE_DDL)
    cols = ["event_id", "event_source", "source_record_id", "employee_id", "event_timestamp", "raw_timestamp",
            "event_type", "location", "zone", "floor", "description", "details", "raw_event_hash", "raw_file",
            "raw_line_no", "ingestion_path", "ingested_at", "available_at"]
    con.executemany(f"INSERT INTO curated.employee_activity_timeline ({', '.join(cols)}) "
                    f"VALUES ({', '.join('?' * len(cols))})", [[row[c] for c in cols] for row in rows])

    con.execute("""CREATE OR REPLACE TABLE curated.quarantine (
        source VARCHAR, raw_file VARCHAR, line_no INTEGER, raw_record VARCHAR, raw_event_hash VARCHAR, error VARCHAR)""")
    if quarantine:
        con.executemany("INSERT INTO curated.quarantine VALUES (?,?,?,?,?,?)", quarantine)

    run_sql_file(con, "00_clock_confidence.sql")
    stats = {"curated_rows": len(rows), "quarantined": len(quarantine), "duplicates_dropped": dropped}
    audit.append("curated_built", **stats)
    return stats


def run_sql_file(con, name: str):
    """Run every statement in one file from the sql/ folder."""
    con.execute((SQL_DIR / name).read_text())


def serve_query(con, name: str, audit: AuditLog) -> tuple[list[str], list[tuple]]:
    """Layer 4: run a saved query and log exactly what was run and what came back."""
    sql = (SQL_DIR / name).read_text()
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    result = cur.fetchall()
    result_hash = hashlib.sha256(json.dumps([list(map(str, r)) for r in result]).encode()).hexdigest()
    audit.append("query_served", query=name, sql_sha256=sha256_bytes(sql.encode()), rows=len(result),
                 result_sha256=result_hash)
    return cols, result


def build_findings(con, audit: AuditLog) -> tuple[list[str], list[tuple]]:
    """Run the alibi rules in sql/02_alibi_checks.sql and return the findings.

    Returns:
        (column names, rows) from curated.alibi_findings, with times as text.
    """
    run_sql_file(con, "02_alibi_checks.sql")
    cur = con.execute("SELECT * REPLACE (strftime(evidence_at, '%Y-%m-%d %H:%M:%S') AS evidence_at) "
                      "FROM curated.alibi_findings")
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    audit.append("alibi_checks_run", findings=len(rows))
    return cols, rows


def fmt(v):
    """Format a value for CSV output: datetimes as 'YYYY-MM-DD HH:MM:SS', None as an empty string."""
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    return "" if v is None else v
