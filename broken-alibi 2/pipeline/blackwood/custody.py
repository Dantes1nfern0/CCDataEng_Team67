"""Chain of custody checks, run at query time.

  1. Source files still match the vendor's published SHA-256 (SHA256SUMS).
  2. Every raw zone file still matches the hash taken when it was ingested.
  3. Every row in the answer: re-read its raw record from the raw zone, re-hash it,
     and compare with the raw_event_hash the curated row carries.
  4. The audit log's hash chain is unbroken."""
from pathlib import Path

from .audit import verify_chain
from .rawzone import RawZone, load_checksums, sha256_bytes, split_records


def check_sources(source_dir: Path) -> list[dict]:
    """Compare each source file in `source_dir` with its published hash in SHA256SUMS.

    Returns:
        One dict per file with status 'OK' or 'FAILED' plus the expected and actual hash.
    """
    out = []
    for name, expected in load_checksums(source_dir / "SHA256SUMS").items():
        p = source_dir / name
        actual = sha256_bytes(p.read_bytes()) if p.exists() else None
        out.append({"file": name, "status": "OK" if actual == expected else "FAILED",
                    "expected": expected, "actual": actual})
    return out


def check_raw_zone(raw: RawZone) -> list[dict]:
    """Re-hash every file and every record in the raw zone against its manifest.

    Returns:
        One dict per raw file with status 'OK' or 'FAILED' and the line numbers that changed.
    """
    out = []
    for m in raw.manifests():
        p = raw.root / m["file"]
        data = p.read_bytes() if p.exists() else b""
        file_ok = sha256_bytes(data) == m["sha256"]
        lines = split_records(data)
        bad = [r["line_no"] for r in m["records"]
               if r["line_no"] > len(lines) or sha256_bytes(lines[r["line_no"] - 1]) != r["raw_event_hash"]]
        out.append({"file": m["file"], "status": "OK" if file_ok and not bad else "FAILED",
                    "records": len(m["records"]), "altered_lines": bad})
    return out


def check_rows(con, raw: RawZone, event_ids: list[str]) -> dict[str, str]:
    """Returns event_id -> 'OK' or 'FAILED: <reason>' for each row in a query result."""
    manifests = {m["file"]: m for m in raw.manifests()}
    cache = {}
    status = {}
    for event_id, raw_file, line_no, curated_hash in con.execute(
        "SELECT event_id, raw_file, raw_line_no, raw_event_hash FROM curated.employee_activity_timeline "
        "WHERE list_contains(?, event_id)", [event_ids]
    ).fetchall():
        if raw_file not in cache:
            p = raw.root / raw_file
            cache[raw_file] = split_records(p.read_bytes()) if p.exists() else []
        lines = cache[raw_file]
        ingest_hash = next((r["raw_event_hash"] for r in manifests.get(raw_file, {}).get("records", [])
                            if r["line_no"] == line_no), None)
        now_hash = sha256_bytes(lines[line_no - 1]) if line_no <= len(lines) else None
        if now_hash != curated_hash:
            status[event_id] = "FAILED: raw record no longer matches the hash on this row"
        elif ingest_hash != curated_hash:
            status[event_id] = "FAILED: hash on this row differs from the one taken at ingestion"
        else:
            status[event_id] = "OK"
    return status


def full_report(con, source_dir: Path, raw: RawZone, audit_path: Path, event_ids: list[str]) -> dict:
    """Run all four custody checks and bundle the results.

    Covers source files, raw zone files, the rows in this answer, and the
    audit log chain. all_ok is True only if every check passed.
    """
    rows = check_rows(con, raw, event_ids)
    chain_ok, chain_msg = verify_chain(audit_path)
    sources = check_sources(source_dir)
    raw_files = check_raw_zone(raw)
    return {
        "source_files": sources,
        "raw_zone_files": raw_files,
        "answer_rows": {"checked": len(rows), "ok": sum(v == "OK" for v in rows.values()),
                        "failed": {k: v for k, v in rows.items() if v != "OK"}},
        "audit_log": {"status": "OK" if chain_ok else "FAILED", "detail": chain_msg},
        "all_ok": chain_ok and all(s["status"] == "OK" for s in sources + raw_files)
                  and all(v == "OK" for v in rows.values()),
        "_row_status": rows,
    }
