"""Layer 2: cleaning and normalization.

Reads raw records (never edits them) and produces rows in one shape for the
curated timeline. Each row keeps a pointer back to its raw record and that
record's hash. Anything that cannot be parsed goes to quarantine, never dropped."""
import json
import re
from datetime import datetime, timedelta, timezone

from .config import BADGE_EXPORT_HOUR_UTC


class ParseError(Exception):
    """Raised when a raw record can't be turned into a timeline row. The record goes to quarantine."""
    pass


def parse_timestamp(value: str) -> datetime:
    """Accepts '2026-08-14 22:14:58Z' (badge, txn) and '2026-08-14T22:33:00Z' (device).
    A timestamp without a zone is rejected: guessing the zone would be guessing the evidence."""
    v = value.strip()
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    try:
        ts = datetime.fromisoformat(v)
    except ValueError as e:
        raise ParseError(f"unreadable timestamp {value!r}") from e
    if ts.tzinfo is None:
        raise ParseError(f"timestamp {value!r} has no time zone")
    return ts.astimezone(timezone.utc)


def normalize_ip(value: str) -> str:
    """'10.0.7.03' -> '10.0.7.3'. The raw value is kept separately when it changes."""
    parts = value.split(".")
    if len(parts) != 4 or not all(p.isdigit() and 0 <= int(p) <= 255 for p in parts):
        raise ParseError(f"bad IPv4 address {value!r}")
    return ".".join(str(int(p)) for p in parts)


class Reference:
    """Lookup tables used during cleaning: who owns which badge or device, and where each door, terminal or device is."""

    def __init__(self, employees: list[dict], locations: list[dict]):
        """Build fast lookups from the reference CSV rows.

        Args:
            employees: Rows from reference/employees.csv (employee_id, badge_id, hostname).
            locations: Rows from reference/locations.csv (code, floor, zone).
        """
        self.by_badge = {e["badge_id"]: e["employee_id"] for e in employees if e["badge_id"]}
        self.by_host = {e["hostname"]: e["employee_id"] for e in employees if e["hostname"]}
        self.locations = {loc["code"]: loc for loc in locations}

    def zone(self, code: str) -> tuple[str, str | None]:
        """Turn a door, terminal or device code into a readable zone and floor.

        Returns:
            (zone, floor), e.g. ('Executive suite', '3'). Unknown codes give
            ('Unknown location', None). Floor is None when the brief doesn't say.
        """
        loc = self.locations.get(code)
        if not loc:
            return "Unknown location", None
        return loc["zone"], (loc["floor"] or None)

    def identity_check(self, employee_id: str, badge_id: str = None, hostname: str = None) -> str:
        """Check that a badge or device is registered to the person the record names.

        Returns:
            'match', 'mismatch: registered to EMP-XXXX', or 'unknown' if the badge
            or device isn't in the lookup table. A mismatch can point to badge sharing.
        """
        expected = self.by_badge.get(badge_id) if badge_id else self.by_host.get(hostname)
        if expected is None:
            return "unknown"
        return "match" if expected == employee_id else f"mismatch: registered to {expected}"


def _row(source, record_id, employee_id, ts, raw_ts, event_type, location, ref, description, details, raw):
    """Assemble one row for curated.employee_activity_timeline.

    Shared by all three cleaners so every source ends up with the same columns.
    Looks up zone and floor, builds the event_id from the raw hash, and copies
    the pointer back to the raw record (file, line, hash).
    """
    zone, floor = ref.zone(location)
    return {
        "event_id": f"{source}-{raw['raw_event_hash'][:16]}",
        "event_source": source,
        "source_record_id": record_id,
        "employee_id": employee_id,
        "event_timestamp": ts,
        "raw_timestamp": raw_ts,
        "event_type": event_type,
        "location": location,
        "zone": zone,
        "floor": floor,
        "description": description,
        "details": json.dumps(details, sort_keys=True),
        "raw_event_hash": raw["raw_event_hash"],
        "raw_file": raw["raw_file"],
        "raw_line_no": raw["line_no"],
        "ingestion_path": raw["ingestion_path"],
        "ingested_at": raw["ingested_at"],
        "available_at": None,  # filled in by the warehouse step, see available_at()
    }


# ---------------------------------------------------------------- badge

BADGE_COLUMNS = ["badge_id", "employee_id", "door_id", "timestamp", "direction"]


def clean_badge(raw: dict, ref: Reference, header: str) -> dict:
    """Turn one badge CSV line into a timeline row.

    Checks the columns match what the badge system should send, parses the
    timestamp to UTC, and maps IN/OUT to BADGE_IN/BADGE_OUT.

    Args:
        raw: The raw record dict (raw_record text, hash, file, line number).
        ref: Reference lookups.
        header: The CSV header line stored at ingestion.

    Raises:
        ParseError: if the line has the wrong columns, direction or timestamp.
    """
    cols = header.split(",")
    values = raw["raw_record"].split(",")
    if cols != BADGE_COLUMNS or len(values) != len(cols):
        raise ParseError(f"expected {len(BADGE_COLUMNS)} badge columns, got {len(values)}")
    r = dict(zip(cols, values))
    direction = r["direction"].strip().upper()
    if direction not in ("IN", "OUT"):
        raise ParseError(f"unknown direction {r['direction']!r}")
    ts = parse_timestamp(r["timestamp"])
    zone, floor = ref.zone(r["door_id"])
    where = f"{zone} ({floor}F)" if floor and str(floor) not in zone else zone
    details = {"badge_id": r["badge_id"], "direction": direction,
               "identity_check": ref.identity_check(r["employee_id"], badge_id=r["badge_id"])}
    return _row("badge", raw["raw_event_hash"], r["employee_id"], ts, r["timestamp"], f"BADGE_{direction}",
                r["door_id"], ref, f"Badge {direction} at {where}", details, raw)


# ---------------------------------------------------------------- device

SYSLOG = re.compile(r"^(?P<ts>\S+)\s+(?P<host>\S+)\s+(?P<kv>.*)$")
KV = re.compile(r"(\w+)=(\S+)")
# The brief calls the field event_code; the log lines say event=. Both map to event_type.
EVENT_KEYS = ("event", "event_code")


def _device_description(event: str, d: dict, host: str) -> str:
    """Write a plain-English line for a device event, e.g. 'Laptop sent an email to an external address'."""
    device = "Laptop" if host.startswith("LAPTOP") else "Workstation"
    if event == "FILE_ACCESS":
        return f"{device} {d.get('action', 'accessed')} file {d.get('path', '(no path)')}"
    if event == "EMAIL_SENT":
        target = "an external address" if d.get("to") == "external" else d.get("to", "unknown recipient")
        return f"{device} sent an email to {target}"
    if event == "USB_INSERTED":
        return f"USB device plugged into {device.lower()}"
    if event in ("LOGIN", "LOGOUT"):
        return f"{device} {event.lower()} ({d.get('action', '')})".replace(" ()", "")
    return f"{device} event {event}"


def clean_device(raw: dict, ref: Reference) -> dict:
    """Turn one syslog line into a timeline row.

    Splits the line into timestamp, hostname and key=value pairs. Maps event=
    (or event_code=) to event_type, joins on user=, keeps the hostname as the
    location, normalizes the IP, and puts any leftover keys (path, to) in details.

    Raises:
        ParseError: if the line doesn't have the expected shape or is missing user= or event=.
    """
    m = SYSLOG.match(raw["raw_record"])
    if not m:
        raise ParseError("line is not '<timestamp> <host> key=value ...'")
    kv = dict(KV.findall(m["kv"]))
    event = next((kv.pop(k) for k in EVENT_KEYS if k in kv), None)
    user = kv.pop("user", None)
    if not event or not user:
        raise ParseError("missing event= or user=")
    host = m["host"]
    ts = parse_timestamp(m["ts"])
    details = {"hostname": host, **kv,
               "identity_check": ref.identity_check(user, hostname=host)}
    if "src" in kv:
        clean_ip = normalize_ip(kv["src"])
        details["source_ip"] = clean_ip
        if clean_ip != kv["src"]:
            details["source_ip_raw"] = kv["src"]
        del details["src"]
    # Join on user=, not on the hostname; the hostname becomes the location.
    return _row("device", raw["raw_event_hash"], user, ts, m["ts"], event.upper(), host, ref,
                _device_description(event.upper(), kv, host), details, raw)


# ---------------------------------------------------------------- transactions

def clean_txn(raw: dict, ref: Reference) -> dict:
    """Turn one change-data-capture event from the payment database into a timeline row.

    Parking terminals become PARKING_EXIT, everything else PURCHASE. If the
    change was an update, the row records which fields changed and says so in
    its description. A delete returns a small marker so dedupe() removes the row.
    """
    change = json.loads(raw["raw_record"])
    if change["op"] == "d":
        # A deleted payment leaves the curated table; the raw change event stays as evidence.
        return {"event_source": "txn", "source_record_id": change["before"]["txn_id"],
                "_lsn": change["lsn"], "_deleted": True}
    t = change["after"]
    ts = parse_timestamp(t["timestamp"])
    amount = float(t["amount"])
    zone, _ = ref.zone(t["terminal_id"])
    if t["terminal_id"].startswith("PARKING"):
        event_type, desc = "PARKING_EXIT", f"Car left through {zone}"
    else:
        event_type, desc = "PURCHASE", f"Paid ${amount:.2f} at {zone}"
    details = {"amount": t["amount"], "terminal_id": t["terminal_id"], "cdc_op": change["op"],
               "lsn": change["lsn"]}
    if change["op"] == "u" and change["before"]:
        # CDC catches edits made after the fact. Say so on the row, and keep what it was before.
        details["changed_fields"] = {k: {"before": v, "after": t.get(k)}
                                     for k, v in change["before"].items() if t.get(k) != v}
        desc += " (edited later in the payment system)"
    row = _row("txn", t["txn_id"], t["employee_id"], ts, t["timestamp"], event_type, t["terminal_id"], ref,
               desc, details, raw)
    row["_lsn"] = change["lsn"]
    return row


def available_at(source: str, event_ts: datetime, export_day: str | None) -> datetime:
    """When an investigator could first have seen this record. Streaming and CDC
    sources are near real time; badge rows wait for the nightly export."""
    if source == "badge" and export_day:
        day = datetime.fromisoformat(export_day).replace(tzinfo=timezone.utc)
        return day + timedelta(days=1, hours=BADGE_EXPORT_HOUR_UTC)
    return event_ts


def dedupe(rows: list[dict]) -> tuple[list[dict], int]:
    """One row per (source, source_record_id). For transactions keep the latest change."""
    best = {}
    for r in rows:
        key = (r["event_source"], r["source_record_id"])
        if key not in best or r.get("_lsn", 0) > best[key].get("_lsn", 0):
            best[key] = r
    kept = [r for r in best.values() if not r.get("_deleted")]
    return kept, len(rows) - len(kept)
