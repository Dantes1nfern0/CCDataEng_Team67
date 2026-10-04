#!/usr/bin/env python3
"""Broken Alibi pipeline: one command, end to end.

    python run_pipeline.py                 run ingest -> clean -> store -> serve -> verify
    python run_pipeline.py --tamper-demo   also show what happens when one raw character is changed
    python run_pipeline.py --reset         wipe output/ first (demo only; real raw storage can't be wiped)
"""
import argparse
import csv
import json
import shutil
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

from blackwood import custody, ingest, warehouse
from blackwood.audit import AuditLog
from blackwood.config import OUTPUT_DIR, SOURCE_DIR, WINDOW_END, WINDOW_START
from blackwood.rawzone import RawZone, load_checksums

SEVERITY_ORDER = ["CONTRADICTS", "NO_ALIBI", "SUSPICIOUS", "INFO"]
VERDICT = {
    "CONTRADICTS": "Contradicted by the data",
    "NO_ALIBI": "No alibi, and the activity needs explaining",
    "SUSPICIOUS": "Doesn't fully hold up",
    "INFO": "Consistent with the data",
}


def write_csv(path: Path, cols, rows):
    """Write a header row and data rows to a CSV file at `path`."""
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows([[warehouse.fmt(v) for v in r] for r in rows])


def verdicts(find_cols, findings):
    """Group findings by person and pick each person's overall verdict.

    The verdict comes from their most serious finding: CONTRADICTS beats
    NO_ALIBI, which beats SUSPICIOUS, which beats INFO.

    Returns:
        {employee_id: (verdict text, list of that person's findings)}.
    """
    by_person = defaultdict(list)
    for r in findings:
        by_person[r[find_cols.index("employee_id")]].append(dict(zip(find_cols, r)))
    out = {}
    for emp, fs in by_person.items():
        worst = min((SEVERITY_ORDER.index(f["severity"]) for f in fs), default=3)
        out[emp] = (VERDICT[SEVERITY_ORDER[worst]], fs)
    return out


def write_report(path, tl_cols, timeline, find_cols, findings, report, stats, ingest_results):
    """Write investigation_report.md: custody results, the timeline table, and each person's verdict and findings in plain English."""
    t = [dict(zip(tl_cols, r)) for r in timeline]
    lines = [
        "# Broken Alibi: investigation output",
        "",
        f"Window: {WINDOW_START[:16]} to {WINDOW_END[:16]} UTC, plus 15 minutes either side. "
        "All times below are UTC, as stamped by the source systems.",
        "",
        "## Chain of custody",
        "",
        f"- Source files vs published hashes: "
        + ", ".join(f"{s['file']} {s['status']}" for s in report["source_files"]),
        f"- Raw zone files vs hashes taken at ingestion: "
        + ", ".join(f"{Path(s['file']).name} {s['status']}" for s in report["raw_zone_files"]),
        f"- Rows in this answer re-hashed against raw: {report['answer_rows']['ok']} of "
        f"{report['answer_rows']['checked']} OK",
        f"- Audit log hash chain: {report['audit_log']['status']} ({report['audit_log']['detail']})",
        f"- **Overall: {'every check passed' if report['all_ok'] else 'A CHECK FAILED, do not rely on this output'}**",
        "",
        "## Timeline",
        "",
        "| Time (UTC) | Who | Source | What happened | Clock | Verified |",
        "|---|---|---|---|---|---|",
    ]
    for r in t:
        mark = "" if r["window_position"] == "in window" else " *(outside window)*"
        lines.append(f"| {r['time_utc'][11:]} | {r['employee_id']} | {r['event_source']} | {r['description']}{mark} "
                     f"| {r['clock_confidence']} | {report['_row_status'].get(r['event_id'], '?')} |")
    lines += ["", "Clock = low means another system recorded an event for the same person close enough "
              "that the two could really have happened in the other order.", "", "## Alibis", ""]
    for emp, (verdict, fs) in verdicts(find_cols, findings).items():
        lines += [f"### {emp}: {verdict}", "", f"> {fs[0]['alibi_text']}", ""]
        for f in sorted(fs, key=lambda f: SEVERITY_ORDER.index(f["severity"])):
            lines.append(f"- **{f['severity']}** ({f['check_name']}): {f['finding']}")
        lines.append("")
    lines += ["## Run summary", "",
              "- Ingested this run: " + ", ".join(f"{r['source']} {r['records']}" for r in ingest_results),
              f"- Curated rows: {stats['curated_rows']}, quarantined: {stats['quarantined']}, "
              f"duplicates dropped: {stats['duplicates_dropped']}", ""]
    path.write_text("\n".join(lines))


def tamper_demo(out_dir: Path, con, event_ids):
    """Copy the raw zone, change one character, and show the checks catch it.
    The real raw zone is never touched."""
    print("\n=== Tamper demo (on a throwaway copy of the raw zone) ===")
    with tempfile.TemporaryDirectory() as tmp:
        copy = Path(tmp) / "raw_zone"
        shutil.copytree(out_dir / "raw_zone", copy)
        target = next(copy.glob("badge/*/*.csv"))
        target.chmod(0o644)
        before = "BDG-0113,EMP-0031,D-SERVER,2026-08-14 23:07:44Z,OUT"
        after = "BDG-0113,EMP-0031,D-SERVER,2026-08-14 22:57:44Z,OUT"
        target.write_text(target.read_text().replace(before, after))
        print(f"Changed one line in {target.name}:\n  before: {before}\n  after:  {after}")
        print("(Someone moving EMP-0031's server room exit to before 11 PM, to match their alibi.)\n")
        throwaway_audit = AuditLog(Path(tmp) / "audit.jsonl", actor="tamper-demo")
        fake = RawZone(copy, throwaway_audit)
        for s in custody.check_raw_zone(fake):
            print(f"  raw file {s['file']}: {s['status']}" + (f" (altered lines {s['altered_lines']})" if s["altered_lines"] else ""))
        rows = custody.check_rows(con, fake, event_ids)
        for eid, st in rows.items():
            if st != "OK":
                print(f"  answer row {eid}: {st}")
        print(f"  {sum(v == 'OK' for v in rows.values())} other rows still OK")
        try:
            warehouse.index_raw_zone(warehouse.connect(Path(tmp) / "t.duckdb"), fake, throwaway_audit)
        except warehouse.RawIntegrityError as e:
            print(f"  rebuilding the timeline from this copy is refused: {e}")


def main():
    """Run the whole pipeline once: ingest, clean, store, serve, verify, then write the output files.

    Returns:
        0 if every custody check passed, 1 if any failed (used as the exit code).
    """
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--reset", action="store_true")
    ap.add_argument("--tamper-demo", action="store_true")
    ap.add_argument("--source-dir", type=Path, default=SOURCE_DIR)
    ap.add_argument("--out-dir", type=Path, default=OUTPUT_DIR)
    args = ap.parse_args()
    src, out = args.source_dir, args.out_dir

    if args.reset and out.exists():
        for p in out.rglob("*"):
            try:
                p.chmod(0o755 if p.is_dir() else 0o644)
            except OSError:
                pass
        shutil.rmtree(out)
    out.mkdir(parents=True, exist_ok=True)

    audit = AuditLog(out / "audit_log.jsonl")
    audit.append("run_started", source_dir=str(src))
    raw = RawZone(out / "raw_zone", audit)
    sums = load_checksums(src / "SHA256SUMS")
    checkpoints = ingest.Checkpoints(out / "state" / "checkpoints.json")

    print("1. Ingest (one path per source)")
    pos = ingest.open_pos_database(out / "sim_sources", src, audit, sums)
    results = [
        ingest.ingest_badge_batch(src, raw, audit, sums),
        ingest.ingest_device_stream(src, raw, audit, sums, checkpoints),
        ingest.ingest_txn_cdc(pos, raw, audit, checkpoints),
    ]
    for r in results:
        print(f"   {r['source']:<7} {r['records']} new records" + (f" ({r['note']})" if r.get("note") else ""))

    print("2. Clean and normalize, 3. Store (raw zone -> curated table)")
    con = warehouse.connect(out / "warehouse.duckdb")
    warehouse.load_reference(con)
    raw_rows = warehouse.index_raw_zone(con, raw, audit)
    stats = warehouse.build_curated(con, raw_rows, audit)
    print(f"   {stats['curated_rows']} timeline rows, {stats['quarantined']} quarantined, "
          f"{stats['duplicates_dropped']} duplicates dropped")

    print("4. Serve: timeline query and alibi checks")
    tl_cols, timeline = warehouse.serve_query(con, "01_timeline.sql", audit)
    find_cols, findings = warehouse.build_findings(con, audit)
    in_window = sum(1 for r in timeline if r[tl_cols.index("window_position")] == "in window")
    print(f"   {len(timeline)} events ({in_window} inside 22:00-00:00), {len(findings)} alibi findings")

    print("5. Verify chain of custody")
    event_ids = [r[tl_cols.index("event_id")] for r in timeline]
    report = custody.full_report(con, src, raw, out / "audit_log.jsonl", event_ids)
    audit.append("custody_verified", all_ok=report["all_ok"], rows_checked=report["answer_rows"]["checked"])
    print(f"   {'all checks passed' if report['all_ok'] else 'CHECK FAILED'}: "
          f"{report['answer_rows']['ok']}/{report['answer_rows']['checked']} rows match raw, "
          f"audit chain {report['audit_log']['status']}")

    write_csv(out / "timeline.csv", tl_cols + ["custody_check"],
              [list(r) + [report["_row_status"].get(r[tl_cols.index("event_id")])] for r in timeline])
    write_csv(out / "alibi_findings.csv", find_cols, findings)
    (out / "custody_report.json").write_text(
        json.dumps({k: v for k, v in report.items() if not k.startswith("_")}, indent=2))
    write_report(out / "investigation_report.md", tl_cols, timeline, find_cols, findings, report, stats, results)

    print("\nVerdicts:")
    for emp, (verdict, _) in verdicts(find_cols, findings).items():
        print(f"   {emp}: {verdict}")
    print(f"\nOutputs in {out}: investigation_report.md, timeline.csv, alibi_findings.csv, "
          "custody_report.json, audit_log.jsonl, warehouse.duckdb, raw_zone/")

    if args.tamper_demo:
        tamper_demo(out, con, event_ids)
    con.close()
    return 0 if report["all_ok"] else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ingest.SourceChecksumError, warehouse.RawIntegrityError) as e:
        print(f"\nSTOPPED: {e}")
        print("The evidence does not match its fingerprint, so the pipeline refuses to load it. "
              "This is logged in output/audit_log.jsonl.")
        sys.exit(2)
