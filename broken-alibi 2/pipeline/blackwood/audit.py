"""Append-only audit log. Each entry carries the hash of the entry before it,
so editing or deleting any past entry breaks the chain from that point on."""
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

GENESIS = "0" * 64


def now_utc() -> str:
    """Return the current time in UTC as an ISO 8601 string with microseconds, e.g. '2026-10-04T18:00:00.123456Z'."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _entry_hash(entry: dict) -> str:
    """Compute the SHA-256 fingerprint of one audit entry.

    The entry's own entry_hash field is left out, and keys are sorted, so the
    same entry always gives the same hash.
    """
    body = {k: v for k, v in entry.items() if k != "entry_hash"}
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


class AuditLog:
    """Append-only, hash-chained log of everything the pipeline does.

    Each call to append() writes one JSON line holding a sequence number, a
    timestamp, what happened, and the hash of the previous entry.
    """

    def __init__(self, path: Path, actor: str = "pipeline"):
        """Open the log at `path`, creating its folder if needed.

        If the log already has entries, pick up the last sequence number and hash
        so new entries continue the same chain.

        Args:
            path: Where the JSONL log file lives.
            actor: Who is writing, recorded on every entry.
        """
        self.path = path
        self.actor = actor
        path.parent.mkdir(parents=True, exist_ok=True)
        self._last_hash, self._seq = GENESIS, 0
        if path.exists():
            for line in path.read_text().splitlines():
                if line.strip():
                    e = json.loads(line)
                    self._last_hash, self._seq = e["entry_hash"], e["seq"]

    def append(self, action: str, **details) -> dict:
        """Write one new entry to the end of the log and return it.

        Args:
            action: Short name of what happened, e.g. 'raw_stored' or 'query_served'.
            **details: Any extra facts to record, such as a file name or a hash.

        Returns:
            The entry as written, including its entry_hash.
        """
        entry = {
            "seq": self._seq + 1,
            "at": now_utc(),
            "actor": self.actor,
            "action": action,
            "details": details,
            "prev_hash": self._last_hash,
        }
        entry["entry_hash"] = _entry_hash(entry)
        with open(self.path, "a") as f:
            f.write(json.dumps(entry, sort_keys=True) + "\n")
        self._last_hash, self._seq = entry["entry_hash"], entry["seq"]
        return entry


def verify_chain(path: Path) -> tuple[bool, str]:
    """Re-check every entry in the audit log.

    Confirms each entry points at the hash of the one before it, and that each
    entry still hashes to its stored entry_hash.

    Returns:
        (True, summary) if the chain is intact, or (False, which entry broke it).
    """
    if not path.exists():
        return False, "audit log missing"
    prev = GENESIS
    n = 0
    for n, line in enumerate(path.read_text().splitlines(), start=1):
        e = json.loads(line)
        if e["prev_hash"] != prev:
            return False, f"entry {n} does not point at entry {n - 1}"
        if _entry_hash(e) != e["entry_hash"]:
            return False, f"entry {n} was edited after it was written"
        prev = e["entry_hash"]
    return True, f"{n} entries, chain intact"
