"""Layer 1: ingestion. One path per source, each matching how that source delivers data.

  badge        nightly CSV export     -> batch file loader
  device       streaming syslog       -> stream consumer with a committed offset
  transactions live PostgreSQL        -> change data capture (CDC) with a committed position

Every path copies the source bytes unchanged into the raw zone and hashes each
record before anything else touches it."""
import json
import sqlite3
from pathlib import Path

from .audit import AuditLog, now_utc
from .rawzone import RawZone, sha256_bytes, split_records


class SourceChecksumError(Exception):
    """Raised when a source file doesn't match the fingerprint its vendor published. Nothing gets loaded."""
    pass


class Checkpoints:
    """Where each consumer got to, like a Kafka consumer offset or a Debezium LSN."""

    def __init__(self, path: Path):
        """Load saved positions from `path`, or start empty on the first run."""
        self.path = path
        self.state = json.loads(path.read_text()) if path.exists() else {}

    def get(self, key, default=None):
        """Return the saved position for `key`, or `default` if this consumer has never run."""
        return self.state.get(key, default)

    def commit(self, key, value):
        """Save a new position for `key` to disk, so the next run starts after it."""
        self.state[key] = value
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.state, indent=2))


def _check_vendor_checksum(path: Path, data: bytes, sums: dict, audit: AuditLog):
    """Check a source file against its published hash and log the result.

    Returns:
        (actual_hash, True) if it matches, or (actual_hash, None) if no hash was published.

    Raises:
        SourceChecksumError: if a published hash exists and doesn't match.
    """
    expected = sums.get(path.name)
    actual = sha256_bytes(data)
    if expected is None:
        audit.append("source_checksum_missing", file=path.name, sha256=actual)
        return actual, None
    if expected != actual:
        audit.append("source_checksum_failed", file=path.name, expected=expected, actual=actual)
        raise SourceChecksumError(f"{path.name}: hash {actual[:12]} does not match the published {expected[:12]}")
    audit.append("source_checksum_ok", file=path.name, sha256=actual)
    return actual, True


# ---------------------------------------------------------------- badge: nightly batch

def ingest_badge_batch(source_dir: Path, raw: RawZone, audit: AuditLog, sums: dict) -> dict:
    """The badge vendor drops one CSV a night. We pick it up, check it against the
    vendor's published checksum, and copy it byte for byte into the raw zone."""
    src = source_dir / "badge_access.csv"
    data = src.read_bytes()
    file_hash, matched = _check_vendor_checksum(src, data, sums, audit)

    lines = split_records(data)
    header, rows = lines[0], lines[1:]
    export_day = min(r.split(b",")[3][:10].decode() for r in rows if r)
    records = [{"line_no": i, "raw_event_hash": sha256_bytes(r)} for i, r in enumerate(lines, start=1)
               if i > 1 and r]
    written = raw.store("badge", f"dt={export_day}", f"badge_access_{export_day}.csv", data, records,
              ingestion_path="nightly_batch", export_day=export_day, header=header.decode(),
              source_file=src.name, vendor_checksum_match=matched)
    if not written:
        return {"source": "badge", "records": 0, "note": "tonight's export is already in the raw zone"}
    return {"source": "badge", "records": len(records), "file_sha256": file_hash}


# ---------------------------------------------------------------- device logs: streaming

def syslog_stream(path: Path):
    """Stands in for a live syslog feed (a Kafka topic or a Fluent Bit forwarder).
    Yields (offset, line) in arrival order. The offset is the line's position in the stream."""
    with open(path, "rb") as f:
        for offset, line in enumerate(f):
            yield offset, line.rstrip(b"\n")


def ingest_device_stream(source_dir: Path, raw: RawZone, audit: AuditLog, sums: dict,
                         checkpoints: Checkpoints) -> dict:
    """Consume new lines since the last committed offset, write them as one closed
    segment to the raw zone, then commit the offset. A crash before the commit just
    re-writes the same segment, which the raw zone accepts because the bytes match."""
    src = source_dir / "device_logs.log"
    # A live stream has no file checksum. The sample file does, so we check it once per replay.
    _check_vendor_checksum(src, src.read_bytes(), sums, audit)

    last = checkpoints.get("device_stream_offset", -1)
    new = [(o, line) for o, line in syslog_stream(src) if o > last and line]
    if not new:
        audit.append("stream_no_new_messages", source="device", committed_offset=last)
        return {"source": "device", "records": 0, "note": "no new messages since last run"}

    segment = checkpoints.get("device_segment", 0) + 1
    received_at = now_utc()
    data = b"\n".join(line for _, line in new) + b"\n"
    records = [{"line_no": i, "stream_offset": o, "received_at": received_at,
                "raw_event_hash": sha256_bytes(line)} for i, (o, line) in enumerate(new, start=1)]
    day = new[0][1][:10].decode()
    raw.store("device", f"dt={day}", f"segment-{segment:05d}.log", data, records,
              ingestion_path="stream_consumer", first_offset=new[0][0], last_offset=new[-1][0])
    checkpoints.commit("device_stream_offset", new[-1][0])
    checkpoints.commit("device_segment", segment)
    audit.append("stream_offset_committed", source="device", offset=new[-1][0])
    return {"source": "device", "records": len(records)}


# ---------------------------------------------------------------- transactions: CDC

POS_SCHEMA = """
CREATE TABLE transactions (
    txn_id TEXT PRIMARY KEY, employee_id TEXT, amount TEXT, timestamp TEXT, terminal_id TEXT);
-- Change log filled by triggers. Debezium reads the Postgres write-ahead log the same way:
-- every insert, update and delete becomes a numbered change event.
CREATE TABLE _changes (
    lsn INTEGER PRIMARY KEY AUTOINCREMENT, op TEXT, txn_id TEXT, before TEXT, after TEXT,
    committed_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')));
CREATE TRIGGER cdc_insert AFTER INSERT ON transactions BEGIN
  INSERT INTO _changes(op, txn_id, after) VALUES ('c', NEW.txn_id, json_object(
    'txn_id', NEW.txn_id, 'employee_id', NEW.employee_id, 'amount', NEW.amount,
    'timestamp', NEW.timestamp, 'terminal_id', NEW.terminal_id));
END;
CREATE TRIGGER cdc_update AFTER UPDATE ON transactions BEGIN
  INSERT INTO _changes(op, txn_id, before, after) VALUES ('u', NEW.txn_id,
    json_object('txn_id', OLD.txn_id, 'employee_id', OLD.employee_id, 'amount', OLD.amount,
                'timestamp', OLD.timestamp, 'terminal_id', OLD.terminal_id),
    json_object('txn_id', NEW.txn_id, 'employee_id', NEW.employee_id, 'amount', NEW.amount,
                'timestamp', NEW.timestamp, 'terminal_id', NEW.terminal_id));
END;
CREATE TRIGGER cdc_delete AFTER DELETE ON transactions BEGIN
  INSERT INTO _changes(op, txn_id, before) VALUES ('d', OLD.txn_id, json_object(
    'txn_id', OLD.txn_id, 'employee_id', OLD.employee_id, 'amount', OLD.amount,
    'timestamp', OLD.timestamp, 'terminal_id', OLD.terminal_id));
END;
"""


def open_pos_database(sim_dir: Path, source_dir: Path, audit: AuditLog, sums: dict) -> sqlite3.Connection:
    """A local stand-in for Blackwood's live payment database. Seeded once from
    transactions.csv; after that it behaves like the live system the CDC reader watches."""
    sim_dir.mkdir(parents=True, exist_ok=True)
    db_path = sim_dir / "pos_postgres_stand_in.sqlite"
    fresh = not db_path.exists()
    con = sqlite3.connect(db_path)
    if fresh:
        src = source_dir / "transactions.csv"
        data = src.read_bytes()
        _check_vendor_checksum(src, data, sums, audit)
        con.executescript(POS_SCHEMA)
        lines = split_records(data)
        for line in lines[1:]:
            con.execute("INSERT INTO transactions VALUES (?,?,?,?,?)", line.decode().split(","))
        con.commit()
        audit.append("pos_stand_in_seeded", rows=len(lines) - 1)
    return con


def ingest_txn_cdc(con: sqlite3.Connection, raw: RawZone, audit: AuditLog, checkpoints: Checkpoints) -> dict:
    """Read change events after the last committed position. We never query the live
    transactions table itself: that would load the payment system and miss later edits."""
    last = checkpoints.get("txn_cdc_lsn", 0)
    rows = con.execute(
        "SELECT lsn, op, txn_id, before, after, committed_at FROM _changes WHERE lsn > ? ORDER BY lsn", (last,)
    ).fetchall()
    if not rows:
        audit.append("cdc_no_new_changes", source="txn", committed_lsn=last)
        return {"source": "txn", "records": 0, "note": "no new changes since last run"}

    events = []
    for lsn, op, txn_id, before, after, committed_at in rows:
        event = {"lsn": lsn, "op": op, "source": {"db": "blackwood_pos", "table": "transactions"},
                 "committed_at": committed_at,
                 "before": json.loads(before) if before else None,
                 "after": json.loads(after) if after else None}
        events.append(json.dumps(event, sort_keys=True, separators=(",", ":")).encode())
    data = b"\n".join(events) + b"\n"
    records = [{"line_no": i, "lsn": r[0], "raw_event_hash": sha256_bytes(e)}
               for i, (r, e) in enumerate(zip(rows, events), start=1)]
    day = json.loads(events[0])["after"]["timestamp"][:10] if json.loads(events[0])["after"] else "unknown"
    raw.store("txn", f"dt={day}", f"changes-{rows[0][0]:08d}-{rows[-1][0]:08d}.jsonl", data, records,
              ingestion_path="change_data_capture", first_lsn=rows[0][0], last_lsn=rows[-1][0])
    checkpoints.commit("txn_cdc_lsn", rows[-1][0])
    audit.append("cdc_position_committed", source="txn", lsn=rows[-1][0])
    return {"source": "txn", "records": len(records)}
